import os

import json

import logging

import pathlib

from typing import Any, Dict, List

import httpx

from fastapi import FastAPI, HTTPException, Request, Query, Body

from fastapi.responses import JSONResponse

# ---------------- Logging ----------------

log = logging.getLogger("adapter")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ---------------- FastAPI ----------------

app = FastAPI(title="Ring Style Adapter (flex → JSON)", version="1.0.0")

# ---------------- Environment ----------------

BACKEND_POST_URL = os.getenv("BACKEND_POST_URL", "").strip()   # e.g. https://<api>.azurewebsites.net/createRequestJSON

API_KEY_REQUIRED = os.getenv("API_KEY_REQUIRED", "false").lower() == "true"

API_KEY          = os.getenv("API_KEY", "").strip()

def _require_backend() -> None:

    if not BACKEND_POST_URL:

        raise HTTPException(status_code=500, detail="BACKEND_POST_URL is not configured")

def _check_api_key(req: Request) -> None:

    if not API_KEY_REQUIRED:

        return

    if not API_KEY or req.headers.get("x-api-key") != API_KEY:

        raise HTTPException(status_code=401, detail="unauthorized")

# ---------------- Config load (mapping.config.json) ----------------

# This file is optional—if missing, we proceed with sensible defaults.

CFG_PATH = pathlib.Path(__file__).with_name("mapping.config.json")

CONFIG: Dict[str, Any] = {}

try:

    log.info("DEBUG - Reading config from: %s", CFG_PATH)

    with open(CFG_PATH, "r", encoding="utf-8") as f:

        head = f.read(200)

        log.info("DEBUG - First 200 chars of config.json: %s", head)

        f.seek(0)

        CONFIG = json.load(f)

except Exception as e:

    log.error("ERROR - Could not read config: %s", e)

    CONFIG = {}

# Field mapping (UI → backend)

FIELD_MAP: Dict[str, str] = { (k or "").lower(): v for k, v in CONFIG.get("field_map", {}).items() }

# Answers label → question text (for flat KV answers)

ANS_KEY_TO_Q: Dict[str, str] = CONFIG.get("answer_key_to_question", {})

# Defaults for missing fields

DEFAULTS: Dict[str, Any] = CONFIG.get("defaults", {})

# ---------------- Helpers ----------------

def _flatten(d: Any, prefix: str = "", out: Dict[str, Any] | None = None) -> Dict[str, Any]:

    """Flatten nested dict/list into dot-keys so we can scan loosely."""

    if out is None:

        out = {}

    if isinstance(d, dict):

        for k, v in d.items():

            _flatten(v, f"{prefix}.{k}" if prefix else k, out)

    elif isinstance(d, list):

        for i, v in enumerate(d):

            _flatten(v, f"{prefix}[{i}]", out)

    else:

        out[prefix] = d

    return out

def _norm_text(x: Any) -> str:

    return "" if x is None else str(x).strip()

def _normalize_identity(raw: Dict[str, Any]) -> Dict[str, Any]:

    """

    Build identity block (request_id, email, name, phoneNumber, birth_date) using FIELD_MAP + DEFAULTS.

    Accept flat or nested input keys.

    """

    flat = _flatten(raw)

    out = dict(DEFAULTS)

    # map all known fields

    for k, v in flat.items():

        last = k.split(".")[-1].lower()

        if last in FIELD_MAP:

            out[FIELD_MAP[last]] = v

    # commonly used fallbacks

    out.setdefault("request_id", raw.get("request_id") or raw.get("result_key") or raw.get("response_id") or "")

    out.setdefault("email", raw.get("email", ""))

    out.setdefault("name", raw.get("name", ""))

    out.setdefault("phoneNumber", raw.get("phone") or raw.get("phone_number") or "")

    out.setdefault("birth_date", raw.get("birth_date") or raw.get("dob") or "")

    # Purchase context (used to choose sheet on your side, but we also pass it through)

    out.setdefault("purchase_for", raw.get("purchase_for") or raw.get("who") or "")

    out["purchase_for"] = _norm_text(out["purchase_for"]).lower()  # "", "self", "others"

    # Normalize gender to one of: "", "male", "female"

    g = _norm_text(raw.get("gender") or raw.get("sex")).lower()

    if g in {"male", "m"}:

        g = "male"

    elif g in {"female", "f"}:

        g = "female"

    else:

        g = ""

    out["gender"] = g

    # Occasion & purpose are common in your flows

    out.setdefault("occasion", _norm_text(raw.get("occasion")))

    out.setdefault("purpose", _norm_text(raw.get("purpose")))

    # Final tidy to strings

    for k, v in list(out.items()):

        if isinstance(v, (dict, list)):

            continue

        out[k] = _norm_text(v)

    return out

def _normalize_answers(raw: Dict[str, Any], purchase_for: str, gender: str) -> Dict[str, Any]:

    """

    Build QnA arrays expected by backend createRequestJSON:

      - questions: ["Q1. ...", "Q2. ...", ...]

      - answers:   ["A1", "A2", ...]

    Supports:

      1) raw["answers"] as list of {question, answer} or {question, selectedOption:{value:"..."}}

      2) flat K/V (e.g., {"who":"Self","gender":"Male"}) via ANS_KEY_TO_Q mapping

    Also injects derived context (purchase_for/gender/occasion/purpose) as Q/A lines if provided.

    """

    questions: List[str] = []

    answers:   List[str] = []

    # CASE 1: structured answers[]

    if isinstance(raw.get("answers"), list) and raw["answers"]:

        for i, item in enumerate(raw["answers"], start=1):

            q = item.get("question") or item.get("q") or ""

            a = item.get("answer")

            if a is None and isinstance(item.get("selectedOption"), dict):

                a = item["selectedOption"].get("value")

            questions.append(_norm_text(q) or f"Q{i}")

            answers.append(_norm_text(a))

    else:

        # CASE 2: flat dict via mapping table

        for key, val in raw.items():

            lk = (key or "").lower()

            if lk in ANS_KEY_TO_Q:

                questions.append(ANS_KEY_TO_Q[lk])

                # if val is an object with selectedOption.value, use that

                if isinstance(val, dict) and "selectedOption" in val and isinstance(val["selectedOption"], dict):

                    answers.append(_norm_text(val["selectedOption"].get("value")))

                else:

                    answers.append(_norm_text(val))

    # Inject derived context lines so the backend can use them too

    # (Only add if they were not already present.)

    inject_pairs = [

        ("purchase_for", purchase_for),

        ("gender", gender),

        ("occasion", _norm_text(raw.get("occasion"))),

        ("purpose", _norm_text(raw.get("purpose"))),

    ]

    for key, val in inject_pairs:

        if not val:

            continue

        qtxt = ANS_KEY_TO_Q.get(key, key.replace("_", " ").title())

        # avoid duplicate question labels

        if qtxt not in questions:

            questions.append(qtxt)

            answers.append(_norm_text(val))

    return {"questions": questions, "answers": answers}

def _normalize_to_backend_json(raw: Dict[str, Any]) -> Dict[str, Any]:

    """Return the exact JSON shape your backend’s /createRequestJSON expects."""

    ident = _normalize_identity(raw)

    qna   = _normalize_answers(raw, ident.get("purchase_for", ""), ident.get("gender", ""))

    return {

        "request_id": ident.get("request_id", ""),

        "email":      ident.get("email", ""),

        "name":       ident.get("name", ""),

        "phoneNumber":ident.get("phoneNumber", ""),

        "timestamp":  "",                 # let backend fill if needed

        "birth_date": ident.get("birth_date", ""),

        "qna": {

            "questions": qna["questions"],

            "answers":   qna["answers"],

            # You can attach extra context for downstream logic:

            "context": {

                "purchase_for": ident.get("purchase_for", ""),

                "gender": ident.get("gender", ""),

                "occasion": ident.get("occasion", ""),

                "purpose": ident.get("purpose", ""),

            }

        }

    }

# ---------------- Endpoints ----------------

@app.get("/")

def root():

    return {

        "ok": True,

        "version": "1.0.0",

        "requires_api_key": API_KEY_REQUIRED,

        "has_backend_post_url": bool(BACKEND_POST_URL),

    }

@app.get("/health")

def health():

    return {"ok": True}

@app.post("/ingest")

async def ingest(

    raw: dict = Body(..., description="Flexible UI JSON"),

    preview: bool = Query(False, description="Return normalized JSON without calling backend"),

    echo: bool = Query(False, description="Return backend status + body"),

    req: Request = None

):

    """

    Accept flexible UI JSON, normalize to backend expected JSON, and:

      - if preview=true: return normalized only

      - else: POST to BACKEND_POST_URL and return status/body (if echo=true) or 200 {ok:true}

    """

    _check_api_key(req)

    log.info("DEBUG - Incoming payload: %s", raw)

    # Normalize (this never calls backend)

    try:

        normalized = _normalize_to_backend_json(raw)

    except Exception as e:

        log.exception("Normalize failed")

        raise HTTPException(status_code=422, detail=f"normalize_error: {e}")

    if preview:

        # show what we'd send

        return {"sent_json": normalized}

    # Call backend

    _require_backend()

    try:

        async with httpx.AsyncClient(timeout=30) as client:

            resp = await client.post(

                BACKEND_POST_URL,

                json=normalized,

                headers={"Content-Type": "application/json"},

            )

    except httpx.RequestError as e:

        log.error("Cannot reach backend: %s", e)

        raise HTTPException(status_code=502, detail={"adapter_error": "cannot_reach_backend", "msg": str(e)})

    # If backend failed, bubble it up with its body so you can debug there

    if not (200 <= resp.status_code < 300):

        body_text = resp.text

        log.error("Backend error %s: %s", resp.status_code, body_text)

        return JSONResponse(

            status_code=502,

            content={

                "sent_json": normalized if echo else "hidden",

                "backend_status": resp.status_code,

                "backend_headers": dict(resp.headers),

                "backend_body": body_text

            },

        )

    # Success from backend

    if echo:

        return {

            "sent_json": normalized,

            "backend_status": resp.status_code,

            "backend_headers": dict(resp.headers),

            "backend_body": resp.text,

        }

    return {"ok": True}
 
