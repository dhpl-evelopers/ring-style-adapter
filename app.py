# app.py

import os, json, logging, pathlib

from typing import Any, Dict, List

import httpx

from fastapi import FastAPI, Request, HTTPException, Body, Query

from fastapi.responses import JSONResponse

log = logging.getLogger("adapter")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

app = FastAPI(title="Ring Style Adapter (flex → JSON)", version="1.0.1")

# --------- Environment ---------

BACKEND_POST_URL = os.getenv("BACKEND_POST_URL", "").strip()  # e.g. https://<api>.azurewebsites.net/createRequestJSON

API_KEY_REQUIRED = os.getenv("API_KEY_REQUIRED", "false").lower() == "true"

API_KEY = os.getenv("API_KEY", "")

# --------- Load mapping.config.json (best‑effort; keep app running even if missing) ---------

CONFIG: Dict[str, Any] = {}

try:

    CFG_PATH = pathlib.Path(__file__).with_name("mapping.config.json")

    log.info("Reading config from %s", CFG_PATH)

    with open(CFG_PATH, "r", encoding="utf-8") as f:

        CONFIG = json.load(f)

except Exception as e:

    log.warning("mapping.config.json not loaded (%s). Using empty config.", e)

    CONFIG = {}

XML_ROOT = CONFIG.get("xml_root", "request")  # not used now, kept for compatibility

FIELD_MAP: Dict[str, str] = {k.lower(): v for k, v in CONFIG.get("field_map", {}).items()}

ANSWER_KEY_TO_Q: Dict[str, str] = CONFIG.get("answer_key_to_question", {})

DEFAULTS: Dict[str, Any] = CONFIG.get("defaults", {})

# --------- Helpers ---------

def _require_backend():

    if not BACKEND_POST_URL:

        raise HTTPException(status_code=500, detail="BACKEND_POST_URL is not configured")

def _check_api_key(req: Request):

    if not API_KEY_REQUIRED:

        return

    if not API_KEY or req.headers.get("x-api-key") != API_KEY:

        raise HTTPException(status_code=401, detail="unauthorized")

def _flatten_answers(raw: Dict[str, Any]) -> List[str]:

    """

    Accepts flexible shapes and returns a plain list of answer strings,

    in an order that matches your backend's expectation.

    Allowed inputs:

      - answers: ["self","male",...]

      - answers: [{"question":"...","answer":"self"}, ...]

      - top-level fields like {"who":"self","gender":"male", ...}

    """

    # 1) Already a flat list?

    a = raw.get("answers")

    if isinstance(a, list) and all(isinstance(x, str) for x in a):

        return a

    # 2) List of objects? -> pull the "answer" value

    if isinstance(a, list) and all(isinstance(x, dict) for x in a):

        flat = []

        for item in a:

            val = item.get("answer")

            if isinstance(val, str) and val.strip():

                flat.append(val.strip())

        if flat:

            return flat

    # 3) Build from well-known top-level keys (order matters)

    keys_in_order = list(ANSWER_KEY_TO_Q.keys()) or ["who", "gender", "occasion", "purpose"]

    flat = []

    for k in keys_in_order:

        v = raw.get(k)

        if v is None:

            continue

        if isinstance(v, str):

            if v.strip():

                flat.append(v.strip())

        elif isinstance(v, (int, float)):

            flat.append(str(v))

    return flat

def _normalize_to_backend_json(raw: Dict[str, Any]) -> Dict[str, Any]:

    """

    Map flexible UI JSON to the backend's expected JSON keys.

    Uses FIELD_MAP + DEFAULTS and preserves unknown fields under 'context'.

    """

    out: Dict[str, Any] = dict(DEFAULTS)

    # apply field_map (case-insensitive on input keys)

    for k, v in raw.items():

        lk = k.lower()

        if lk in FIELD_MAP:

            out[FIELD_MAP[lk]] = v

    # always ensure a request_id (backend expects this key name)

    # Try common candidates: result_key / response_id / request_id

    for cand in ("request_id", "result_key", "response_id", "responseKey", "resultKey"):

        if cand in raw and str(raw[cand]).strip():

            out["request_id"] = str(raw[cand]).strip()

            break

    out.setdefault("request_id", "")

    # required identity-ish fields (mapped or pass-through)

    for k in ["name", "email", "phoneNumber", "birth_date"]:

        if k not in out and k in raw:

            out[k] = raw[k]

    # answers (flatten to list of strings)

    out["answers"] = _flatten_answers(raw)

    # keep the original raw for traceability (optional)

    out["context"] = {

        "purchase_for": raw.get("purchase_for") or raw.get("purchaseFor"),

        "gender": raw.get("gender"),

        "occasion": raw.get("occasion"),

        "purpose": raw.get("purpose"),

    }

    return out

# --------- Routes ---------

@app.get("/")

def root():

    return {

        "ok": True,

        "requires_api_key": API_KEY_REQUIRED,

        "has_backend_post_url": bool(BACKEND_POST_URL),

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

    # Normalize to backend JSON

    try:

        normalized = _normalize_to_backend_json(raw)

    except Exception as e:

        log.exception("Normalize failed")

        raise HTTPException(status_code=422, detail=f"Normalize error: {e}")

    if preview:

        # For quick inspection in Swagger

        return {"sent_json": normalized}

    _require_backend()

    # Send to backend

    try:

        async with httpx.AsyncClient(timeout=30) as client:

            resp = await client.post(

                BACKEND_POST_URL,

                json=normalized,

                headers={"Content-Type": "application/json"},

            )

    except httpx.RequestError as e:

        log.error("Backend not reachable: %s", e)

        raise HTTPException(

            status_code=502,

            detail={"adapter_error": "cannot_reach_backend", "msg": str(e)},

        )

    # Bubble backend result (don’t mask 4xx/5xx)

    body_text = resp.text

    if echo:

        return {

            "sent_json": normalized,

            "backend_status": resp.status_code,

            "backend_headers": {

                "content-type": resp.headers.get("content-type", ""),

            },

            "backend_body": body_text,

        }

    if 200 <= resp.status_code < 300:

        # try to parse JSON backend body; if it’s plain text, return as-is

        try:

            return JSONResponse(resp.json(), status_code=resp.status_code)

        except Exception:

            return JSONResponse({"body": body_text}, status_code=resp.status_code)

    # Non-2xx → surface as 502 so it’s obvious it failed downstream

    raise HTTPException(

        status_code=502,

        detail={"backend_status": resp.status_code, "backend_body": body_text},

    )
 
