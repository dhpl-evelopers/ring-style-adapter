import os

import json

import logging

import pathlib

from typing import Any, Dict, List, Tuple

import httpx

from fastapi import FastAPI, Request, HTTPException, Body, Query

from fastapi.responses import JSONResponse

# ----------------- Logging -----------------

log = logging.getLogger("adapter")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ----------------- FastAPI -----------------

app = FastAPI(title="Ring Style Adapter (flex → JSON)", version="1.0.0")

# ----------------- Env (set in Azure App Service) -----------------

# Example:

#   BACKEND_POST_URL = https://<api>.azurewebsites.net/createRequestJSON

#   BACKEND_GET_URL  = https://<api>.azurewebsites.net/fetchResponse?response_id=

BACKEND_POST_URL = os.getenv("BACKEND_POST_URL", "").strip()

BACKEND_GET_URL = os.getenv("BACKEND_GET_URL", "").strip()

API_KEY_REQUIRED = os.getenv("API_KEY_REQUIRED", "false").lower() == "true"

API_KEY = os.getenv("API_KEY", "")

# ----------------- Load mapping.config.json -----------------

CFG_PATH = pathlib.Path(__file__).with_name("mapping.config.json")

def _load_config() -> Dict[str, Any]:

    """

    Load mapping.config.json that defines:

      - field_map: incoming name -> backend field name

      - answer_key_to_question: map answer keys to canonical question text

      - result_key_field_candidates: keys to look for request/response id

      - defaults: default values for missing fields

    """

    try:

        with open(CFG_PATH, "r", encoding="utf-8") as f:

            cfg = json.load(f)

        return cfg

    except Exception as e:

        log.error("Could not read %s: %s", CFG_PATH, e)

        return {

            "xml_root": "request",

            "result_key_field_candidates": ["result_key", "response_id", "responseKey", "resultKey"],

            "field_map": {},

            "answer_key_to_question": {},

            "defaults": {},

        }

CONFIG = _load_config()

XML_ROOT = CONFIG.get("xml_root", "request")

FIELD_MAP: Dict[str, str] = {k.lower(): v for k, v in CONFIG.get("field_map", {}).items()}

ANS_KEY_TO_Q: Dict[str, str] = CONFIG.get("answer_key_to_question", {})

RESULT_KEY_CANDIDATES: List[str] = [s.lower() for s in CONFIG.get("result_key_field_candidates", ["result_key", "response_id"])]

DEFAULTS: Dict[str, Any] = CONFIG.get("defaults", {})


# ----------------- Helpers -----------------

def _require_backend():

    if not BACKEND_POST_URL:

        raise HTTPException(status_code=500, detail="BACKEND_POST_URL is not configured")

def _check_api_key(req: Request):

    if not API_KEY_REQUIRED:

        return

    if not API_KEY or req.headers.get("x-api-key") != API_KEY:

        raise HTTPException(status_code=401, detail="unauthorized")

def _find_result_key(d: Dict[str, Any]) -> str:

    """Find a request/result key anywhere in a nested object."""

    for k in RESULT_KEY_CANDIDATES:

        for in_key, in_val in d.items():

            if in_key.lower() == k:

                return str(in_val)

    for v in d.values():

        if isinstance(v, dict):

            rk = _find_result_key(v)

            if rk:

                return rk

    return ""

def _flatten(obj: Any) -> Dict[str, Any]:

    """Flatten nested dicts into { 'a.b.c': value } plus keep last-key map."""

    flat: Dict[str, Any] = {}

    def visit(prefix: str, node: Any):

        if isinstance(node, dict):

            for k, v in node.items():

                visit(f"{prefix}.{k}" if prefix else k, v)

        else:

            flat[prefix] = node

    if isinstance(obj, dict):

        visit("", obj)

    return flat

def _normalize_answers(raw: Dict[str, Any]) -> Tuple[List[str], List[str]]:

    """

    Produce two parallel arrays: questions[], answers[] to match backend /createRequestJSON.

    Accepts:

      1) raw["answers"] as a dict: {"who":"Self","gender":"Male"} -> map keys via ANS_KEY_TO_Q

      2) raw["answers"] as a list of {"question": "...", "answer": "..."} objects

      3) raw["answers"] as a list of strings (already ordered)

    """

    answers = raw.get("answers")

    # Case 2: already structured list

    if isinstance(answers, list) and answers and isinstance(answers[0], dict) and "question" in answers[0]:

        qs = [str(a.get("question", "")) for a in answers]

        as_ = [str(a.get("answer", "")) for a in answers]

        return qs, as_

    # Case 3: list of strings (assume UI already ordered them)

    if isinstance(answers, list) and (not answers or isinstance(answers[0], (str, int, float))):

        # Try to derive question text from ANS_KEY_TO_Q order if also provided as keys elsewhere

        # Otherwise return empty questions with same length

        return [], [str(x) for x in answers]

    # Case 1: dict of key->value, use map to question text

    if isinstance(answers, dict):

        qs, as_ = [], []

        for key, value in answers.items():

            q_text = ANS_KEY_TO_Q.get(key, key)  # fallback to the key itself

            qs.append(str(q_text))

            as_.append("" if value is None else str(value))

        return qs, as_

    # Nothing provided

    return [], []

def _normalize_to_backend_json(raw: Dict[str, Any]) -> Dict[str, Any]:

    """

    Convert an arbitrary incoming JSON into what the backend expects for /createRequestJSON:

      {

        "request_id": "...",

        "email": "...",

        "name": "...",

        "phoneNumber": "...",

        "birth_date": "...",

        "questions": ["Q1...", "Q2..."],

        "answers":   ["A1...", "A2..."]

      }

    """

    out: Dict[str, Any] = dict(DEFAULTS)  # start with defaults

    # map top-level and nested fields using FIELD_MAP (case-insensitive)

    flat = _flatten(raw)

    for k, v in flat.items():

        last = k.split(".")[-1].lower()

        if last in FIELD_MAP:

            out[FIELD_MAP[last]] = v

    # request_id

    req_id = raw.get("request_id") or _find_result_key(raw)

    if req_id:

        out["request_id"] = str(req_id)

    # questions & answers

    qs, as_ = _normalize_answers(raw)

    if qs:

        out["questions"] = qs

    if as_:

        out["answers"] = as_

    # ensure expected keys exist (even if empty string)

    for k in ["request_id", "email", "name", "phoneNumber", "birth_date"]:

        out.setdefault(k, "")

    out.setdefault("questions", [])

    out.setdefault("answers", [])

    return out


# ----------------- Routes -----------------

@app.get("/")

def root():

    return {

        "ok": True,

        "requires_api_key": API_KEY_REQUIRED,

        "has_backend_urls": bool(BACKEND_POST_URL),

        "version": "1.0.0",

    }

@app.get("/health")

def health():

    return {"ok": True}

@app.post("/ingest")

async def ingest(

    raw: Dict[str, Any] = Body(

        ...,

        example={

            "result_key": "abc123",

            "name": "Sakshi",

            "email": "sakshi@example.com",

            "phone": "9999999999",

            "birth_date": "2001-07-01",

            "answers": {"who": "Self", "gender": "Male"},

        },

        description="Flexible UI payload",

    ),

    preview: bool = Query(False, description="Return normalized JSON without calling backend"),

    echo: bool = Query(False, description="Return backend status + body"),

    req: Request = None,

):

    """

    Accept flexible UI JSON, normalize to backend expected JSON.

    If `preview=true`, return normalized JSON only.

    Otherwise post to BACKEND_POST_URL (/createRequestJSON) and return status.

    """

    _check_api_key(req)

    # Normalize now (this also works as schema/sanity validation)

    if not isinstance(raw, dict):

        raise HTTPException(status_code=400, detail="Body must be a JSON object")

    normalized = _normalize_to_backend_json(raw)

    if preview:

        return JSONResponse(status_code=200, content={"normalized": normalized, "note": "preview=true (no backend call)"})

    _require_backend()

    # Call backend

    try:

        async with httpx.AsyncClient(timeout=30) as client:

            resp = await client.post(BACKEND_POST_URL, json=normalized)

    except httpx.RequestError as e:

        log.error("Cannot reach backend: %s", e)

        raise HTTPException(status_code=502, detail=f"cannot_reach_backend: {e}")

    # Build response

    ct = resp.headers.get("content-type", "")

    body_text = resp.text

    body_json = None

    if "json" in ct.lower():

        try:

            body_json = resp.json()

        except Exception:

            body_json = None

    payload: Dict[str, Any] = {

        "backend_status": resp.status_code,

        "backend_body": body_json if body_json is not None else body_text,

    }

    if echo:

        payload["sent"] = normalized

    # Use 200 for success; bubble backend code otherwise (as 502)

    if 200 <= resp.status_code < 300:

        return JSONResponse(status_code=200, content=payload)

    else:

        return JSONResponse(status_code=502, content=payload)
 
