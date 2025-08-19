import os

import json

import logging

import pathlib

from typing import Any, Dict, List, Tuple

import httpx

from fastapi import FastAPI, Request, HTTPException, Body, Query

from fastapi.responses import JSONResponse

# --------------------------------------------------------------------

# Logging

# --------------------------------------------------------------------

log = logging.getLogger("adapter")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# --------------------------------------------------------------------

# FastAPI app

# --------------------------------------------------------------------

app = FastAPI(title="Ring Style Adapter (flex → JSON)", version="1.0.0")

# --------------------------------------------------------------------

# Env vars (set these in Azure > Configuration)

# --------------------------------------------------------------------

# Example: https://<your-backend>.azurewebsites.net/createRequestJSON

BACKEND_POST_URL = os.getenv("BACKEND_POST_URL", "").strip()

# Optional GET (not used by /ingest, but kept for completeness)

BACKEND_GET_URL = os.getenv("BACKEND_GET_URL", "").strip()

# API key guard (optional)

API_KEY_REQUIRED = os.getenv("API_KEY_REQUIRED", "false").lower() == "true"

API_KEY = os.getenv("API_KEY", "")

# --------------------------------------------------------------------

# Config (field mapping). If mapping.config.json is present we use it.

# If not, we fall back to sane defaults that match your backend.

# --------------------------------------------------------------------

CFG_PATH = pathlib.Path(__file__).with_name("mapping.config.json")

DEFAULT_CONFIG = {

    "xml_root": "request",  # ignored now (JSON), kept for compatibility

    "result_key_field_candidates": ["result_key", "response_id", "responsekey", "resultKey"],

    "field_map": {

        # UI flexible keys  -> backend expected keys

        "request_id": "request_id",

        "response_id": "response_id",

        "result_key": "response_id",

        "resultkey": "response_id",

        "full_name": "full_name",

        "name": "full_name",

        "email": "email",

        "phone": "phone_number",

        "phone_number": "phone_number",

        "phonenumber": "phone_number",

        "birth_date": "birth_date",

        "date": "birth_date",

    },

    # If UI gives answers as a dict (key -> value), map keys to readable questions:

    "answer_key_to_question": {

        "who": "Q1. Who are you?",

        "gender": "Q2. Gender",

        "occasion": "Q3. Occasion",

        "purpose": "Q4. Purpose",

    },

    "defaults": {

        "request_id": "",

        "response_id": "",

        "full_name": "",

        "email": "",

        "phone_number": "",

        "birth_date": "",

    },

}

def _load_config() -> dict:

    if CFG_PATH.exists():

        try:

            with open(CFG_PATH, "r", encoding="utf-8") as f:

                head = f.read(200)  # also helps debug deployment issues

                log.info("Read mapping.config.json first 200 chars: %s", head)

                f.seek(0)

                cfg = json.load(f)

                # defensive normalization

                cfg.setdefault("field_map", {})

                cfg.setdefault("answer_key_to_question", {})

                cfg.setdefault("result_key_field_candidates", ["result_key", "response_id"])

                cfg.setdefault("defaults", {})

                return cfg

        except Exception as e:

            log.warning("Could not parse mapping.config.json: %s. Using defaults.", e)

    return DEFAULT_CONFIG

CONFIG = _load_config()

FIELD_MAP: Dict[str, str] = {k.lower(): v for k, v in CONFIG.get("field_map", {}).items()}

ANS_KEY_TO_Q: Dict[str, str] = CONFIG.get("answer_key_to_question", {})

RESULT_KEY_CANDIDATES: List[str] = [s.lower() for s in CONFIG.get("result_key_field_candidates", [])]

DEFAULTS: Dict[str, Any] = CONFIG.get("defaults", {})

# --------------------------------------------------------------------

# Small helpers

# --------------------------------------------------------------------

def _require_backend():

    if not BACKEND_POST_URL:

        raise HTTPException(status_code=500, detail="BACKEND_POST_URL is not configured")

def _check_api_key(req: Request):

    if not API_KEY_REQUIRED:

        return

    if not API_KEY or req.headers.get("x-api-key") != API_KEY:

        raise HTTPException(status_code=401, detail="unauthorized")

def _flatten(obj: Any, prefix: str = "", out: Dict[str, Any] = None) -> Dict[str, Any]:

    """Flatten nested dict/list into dotted keys for greedy lookup."""

    if out is None:

        out = {}

    if isinstance(obj, dict):

        for k, v in obj.items():

            _flatten(v, f"{prefix}.{k}" if prefix else k, out)

    elif isinstance(obj, list):

        for i, v in enumerate(obj):

            _flatten(v, f"{prefix}[{i}]", out)

    else:

        out[prefix] = obj

    return out

def _extract_result_key(d: Dict[str, Any]) -> str:

    # direct keys

    for k in list(d.keys()):

        if k.lower() in RESULT_KEY_CANDIDATES:

            return str(d[k])

    # nested

    for v in d.values():

        if isinstance(v, dict):

            rk = _extract_result_key(v)

            if rk:

                return rk

    return ""

def _map_flat_fields(raw: Dict[str, Any]) -> Dict[str, Any]:

    out = dict(DEFAULTS)

    flat = _flatten(raw)

    # take best match from any final segment key

    for k, v in flat.items():

        tail = k.split(".")[-1]

        # strip list suffix like key[0]

        tail = tail.split("[", 1)[0].lower()

        if tail in FIELD_MAP:

            out[FIELD_MAP[tail]] = v

    return out

def _coerce_str(x: Any) -> str:

    if x is None:

        return ""

    return str(x)

def _build_qna(raw: Dict[str, Any]) -> List[Dict[str, str]]:

    """

    Build qna as [{question, answer}, ...] from MANY possible shapes:

      - raw["qna"] already list of {question, answer}

      - raw has arrays: questions[], answers[]

      - raw has "answers" as a dict of simple keys -> values (use ANS_KEY_TO_Q map)

      - raw has an array of objects with "question"/"selectedOption"/"value" etc.

    """

    # 1) Already correct

    if isinstance(raw.get("qna"), list) and raw["qna"] and isinstance(raw["qna"][0], dict):

        out = []

        for item in raw["qna"]:

            q = _coerce_str(item.get("question"))

            a = _coerce_str(item.get("answer"))

            if q or a:

                out.append({"question": q, "answer": a})

        return out

    # 2) Parallel arrays: questions[], answers[]

    if isinstance(raw.get("questions"), list) and isinstance(raw.get("answers"), list):

        qs = [ _coerce_str(x) for x in raw["questions"] ]

        ans = [ _coerce_str(x) for x in raw["answers"] ]

        n = max(len(qs), len(ans))

        out = []

        for i in range(n):

            q = qs[i] if i < len(qs) else ""

            a = ans[i] if i < len(ans) else ""

            if q or a:

                out.append({"question": q, "answer": a})

        if out:

            return out

    # 3) answers is a dict {key: value} -> use ANS_KEY_TO_Q

    if isinstance(raw.get("answers"), dict):

        out = []

        for k, v in raw["answers"].items():

            q = ANS_KEY_TO_Q.get(k, k)  # fall back to key name

            out.append({"question": _coerce_str(q), "answer": _coerce_str(v)})

        if out:

            return out

    # 4) answers is a list of objects (selectedOption/value style)

    if isinstance(raw.get("answers"), list) and raw["answers"] and isinstance(raw["answers"][0], dict):

        out = []

        for idx, obj in enumerate(raw["answers"]):

            q = _coerce_str(obj.get("question") or obj.get("label") or f"Q{idx+1}")

            a = _coerce_str(

                obj.get("answer")

                or obj.get("selectedOption", {}).get("value")

                or obj.get("value")

                or obj.get("text")

                or obj.get("choice")

            )

            out.append({"question": q, "answer": a})

        if out:

            return out

    # 5) Nothing matched – empty

    return []

def _normalize_to_backend_json(raw: Dict[str, Any]) -> Dict[str, Any]:

    """

    Normalize flexible UI JSON to the backend JSON schema:

    {

      "request_id": "...",

      "response_id": "...",

      "full_name": "...",

      "email": "...",

      "phone_number": "...",

      "birth_date": "...",

      "qna": [{ "question": "...", "answer": "..." }]

    }

    """

    base = _map_flat_fields(raw)

    # If request_id empty but we can find a result_key, copy it

    if not base.get("request_id"):

        base["request_id"] = _extract_result_key(raw)

    # response_id should exist; default to request_id if missing

    rid = base.get("response_id")

    if not rid:

        base["response_id"] = base.get("request_id", "")

    # Ensure required keys exist

    for k in ["full_name", "email", "phone_number", "birth_date"]:

        base.setdefault(k, "")

    # Build qna

    qna = _build_qna(raw)

    base["qna"] = qna

    return base

# --------------------------------------------------------------------

# Routes

# --------------------------------------------------------------------

@app.get("/")

def root():

    return {

        "ok": True,

        "expects": "JSON for /createRequestJSON",

        "post_url_configured": bool(BACKEND_POST_URL),

        "requires_api_key": API_KEY_REQUIRED,

        "version": app.version,

    }

@app.get("/health")

def health():

    return {"ok": True}

@app.post("/ingest")

async def ingest(

    raw: dict = Body(..., description="Flexible UI JSON"),

    preview: bool = Query(False, description="Return normalized JSON without calling backend"),

    echo: bool = Query(False, description="Return backend status + body"),

    req: Request = None,

):

    _check_api_key(req)

    if not isinstance(raw, dict):

        raise HTTPException(status_code=400, detail="Body must be a JSON object")

    normalized = _normalize_to_backend_json(raw)

    if preview:

        return {"normalized_json": normalized}

    _require_backend()

    try:

        async with httpx.AsyncClient(timeout=45) as client:

            resp = await client.post(

                BACKEND_POST_URL,

                json=normalized,

                headers={"Content-Type": "application/json"},

            )

    except httpx.RequestError as e:

        log.exception("Adapter could not reach backend: %s", e)

        raise HTTPException(status_code=502, detail=f"cannot_reach_backend: {e!s}")

    # If caller wants to see backend status/body, include them

    if echo:

        body_text = resp.text

        return {

            "sent_json": normalized,

            "backend_status": resp.status_code,

            "backend_headers": {

                "content-type": resp.headers.get("content-type", ""),

                "date": resp.headers.get("date", ""),

                "server": resp.headers.get("server", ""),

            },

            "backend_body": body_text,

        }

    # Otherwise bubble success/errors appropriately

    if 200 <= resp.status_code < 300:

        # forward backend body as JSON if possible

        try:

            return resp.json()

        except Exception:

            return JSONResponse(content={"body": resp.text}, media_type="application/json")

    # Non-2xx -> explicit error

    raise HTTPException(

        status_code=502,

        detail={"backend_status": resp.status_code, "body": resp.text},

    )
 
