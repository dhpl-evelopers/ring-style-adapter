# app.py

import os

import json

import logging

import pathlib

from typing import Any, Dict, List

import httpx

from fastapi import FastAPI, Request, HTTPException, Body, Query

from fastapi.responses import JSONResponse

# ------------- logging -------------

log = logging.getLogger("adapter")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ------------- FastAPI -------------

app = FastAPI(title="Ring Style Adapter (flex → JSON)", version="1.0.0")

# ------------- env -------------

BACKEND_POST_URL = os.getenv("BACKEND_POST_URL", "").strip()   # e.g. https://.../createRequestJSON

API_KEY_REQUIRED = os.getenv("API_KEY_REQUIRED", "false").lower() == "true"

API_KEY          = os.getenv("API_KEY", "")

# ------------- config (mapping.config.json) -------------

CFG_PATH = pathlib.Path(__file__).with_name("mapping.config.json")

try:

    with open(CFG_PATH, "r", encoding="utf-8") as f:

        CONFIG = json.load(f)

    log.info("Loaded mapping.config.json from %s", CFG_PATH)

except Exception as e:

    log.error("Failed to read mapping.config.json: %s", e)

    CONFIG = {}

FIELD_MAP: Dict[str, str] = { (k or "").lower(): v for k, v in CONFIG.get("field_map", {}).items() }

ANS_KEY_TO_Q: Dict[str, str] = CONFIG.get("answer_key_to_question", {})

RESULT_KEY_CANDIDATES: List[str] = [s.lower() for s in CONFIG.get(

    "result_key_field_candidates",

    ["result_key", "response_id", "responsekey", "resultKey"]

)]

DEFAULTS: Dict[str, Any] = CONFIG.get("defaults", {})


# ------------- helpers -------------

def _require_backend():

    if not BACKEND_POST_URL:

        raise HTTPException(status_code=500, detail="BACKEND_POST_URL is not configured")

def _check_api_key(req: Request):

    if not API_KEY_REQUIRED:

        return

    if not API_KEY or req.headers.get("x-api-key") != API_KEY:

        raise HTTPException(status_code=401, detail="unauthorized")

def _find_request_id(d: Dict[str, Any]) -> str:

    # try top-level direct

    for k in RESULT_KEY_CANDIDATES:

        for key in d.keys():

            if key.lower() == k:

                return str(d[key])

    # nested search

    for v in d.values():

        if isinstance(v, dict):

            rid = _find_request_id(v)

            if rid:

                return rid

    return ""

def _flatten(obj: Any) -> Dict[str, Any]:

    flat: Dict[str, Any] = {}

    def visit(prefix: str, node: Any):

        if isinstance(node, dict):

            for k, v in node.items():

                visit(f"{prefix}.{k}" if prefix else str(k), v)

        else:

            flat[prefix] = node

    if isinstance(obj, dict):

        visit("", obj)

    return flat

def _core_fields_from_any(raw: Dict[str, Any]) -> Dict[str, Any]:

    """

    Map free-form keys to backend core fields using FIELD_MAP + DEFAULTS.

    """

    out = dict(DEFAULTS)  # copy defaults

    flat = _flatten(raw)

    # try "data" container first if present

    data = raw.get("data")

    if isinstance(data, dict):

        flat.update(_flatten(data))

    for incoming_path, value in flat.items():

        last = incoming_path.split(".")[-1].lower()

        if last in FIELD_MAP:

            out[FIELD_MAP[last]] = value

        else:

            # Accept direct matches if defaults contain the canonical field name

            if last in DEFAULTS:

                out[last] = value

    return out

def _normalize_to_backend_json(raw: Dict[str, Any]) -> Dict[str, Any]:

    """

    Convert flexible UI JSON => backend expected JSON:

      {

        "request_id": "...",

        "name": "...",

        "email": "...",

        "phone_number": "...",

        "birth_date": "...",

        "questions": ["Q1 ...","Q2 ...", ...],

        "answers":   ["...","...", ...]

      }

    """

    # Core fields

    core = _core_fields_from_any(raw)

    request_id = _find_request_id(raw)

    # Build question/answer arrays

    questions: List[str] = []

    answers:   List[str] = []

    # Case A: compact dict of answers (preferred UI form)

    answers_obj = None

    if isinstance(raw.get("answers"), dict):

        answers_obj = raw["answers"]

    elif isinstance(raw.get("data"), dict) and isinstance(raw["data"].get("answers"), dict):

        answers_obj = raw["data"]["answers"]

    if isinstance(answers_obj, dict):

        for key, val in answers_obj.items():

            key_str = str(key)

            q_text = ANS_KEY_TO_Q.get(key_str, key_str)

            questions.append(q_text)

            answers.append("" if val is None else str(val))

    # Case B: explicit arrays (fallback UI form)

    if isinstance(raw.get("questions"), list) and isinstance(raw.get("answers"), list):

        q_list = raw["questions"]

        a_list = raw["answers"]

        for i, q in enumerate(q_list):

            a = a_list[i] if i < len(a_list) else ""

            questions.append(str(q))

            answers.append("" if a is None else str(a))

    # Final payload

    payload = {

        "request_id": request_id or "",

        "name":        str(core.get("name", "")),

        "email":       str(core.get("email", "")),

        "phone_number": str(core.get("phoneNumber", core.get("phone_number", ""))),

        "birth_date":   str(core.get("birth_date", "")),

        "questions":    questions,

        "answers":      answers,

    }

    return payload


# ------------- routes -------------

@app.get("/")

def root():

    return {

        "ok": True,

        "version": "1.0.0",

        "requires_api_key": API_KEY_REQUIRED,

        "has_backend": bool(BACKEND_POST_URL),

        "backend_post_url": bool(BACKEND_POST_URL),

        "config_loaded": bool(CONFIG),

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

    """

    Accept flexible UI JSON, normalize to backend JSON, optionally POST it to backend.

    """

    _check_api_key(req)

    normalized = _normalize_to_backend_json(raw)

    # Basic sanity to help troubleshooting

    missing = [k for k in ["request_id", "name", "email"] if not normalized.get(k)]

    if missing:

        log.warning("Normalized JSON missing fields: %s", missing)

    if preview:

        return {"sent_json": normalized}

    _require_backend()

    try:

        async with httpx.AsyncClient(timeout=30) as client:

            resp = await client.post(

                BACKEND_POST_URL,

                json=normalized,

                headers={"Content-Type": "application/json"},

            )

    except httpx.RequestError as e:

        return JSONResponse(

            status_code=502,

            content={"detail": {"adapter_error": "cannot_reach_backend", "msg": str(e)}},

        )

    # If the backend fails, expose its status + body so you can fix the backend separately

    body_text = resp.text

    if echo:

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

    if 200 <= resp.status_code < 300:

        # Backend OK → forward JSON if provided, else return raw text

        if "application/json" in resp.headers.get("content-type", ""):

            try:

                return resp.json()

            except Exception:

                return {"ok": True, "body": body_text}

        return {"ok": True, "body": body_text}

    # Bubble up backend error

    return JSONResponse(

        status_code=502,

        content={"detail": {"backend_status": resp.status_code, "body": body_text}},

    )
 
