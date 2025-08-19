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

# ----------------- Environment -----------------

# e.g. https://<your-api>.azurewebsites.net/createRequestJSON

BACKEND_POST_URL = os.getenv("BACKEND_POST_URL", "").strip()

API_KEY_REQUIRED = os.getenv("API_KEY_REQUIRED", "false").lower() == "true"

API_KEY = os.getenv("API_KEY", "")

# ----------------- Config load (mapping.config.json) -----------------

CFG_PATH = pathlib.Path(__file__).with_name("mapping.config.json")

def _load_config() -> Dict[str, Any]:

    try:

        head = CFG_PATH.read_text(encoding="utf-8")[:200]

        print("DEBUG - Reading config from:", CFG_PATH)

        print("DEBUG - First 200 chars of config.json:", head)

        with CFG_PATH.open("r", encoding="utf-8") as f:

            return json.load(f)

    except Exception as e:

        print("ERROR - Could not read config:", e)

        return {}

CONFIG = _load_config()

# --------------- Helpers -----------------

def _require_backend():

    if not BACKEND_POST_URL:

        raise HTTPException(status_code=500, detail="BACKEND_POST_URL not configured")

def _check_api_key(req: Request):

    if not API_KEY_REQUIRED:

        return

    given = req.headers.get("x-api-key", "")

    if (not API_KEY) or (given != API_KEY):

        raise HTTPException(status_code=401, detail="unauthorized")

def _find_result_key(d: Dict[str, Any], candidates: List[str] = None) -> str:

    if candidates is None:

        candidates = CONFIG.get(

            "result_key_field_candidates",

            ["result_key", "response_id", "responseKey", "resultKey"]

        )

    # direct

    for k in list(d.keys()):

        for c in candidates:

            if k.lower() == c.lower():

                v = d.get(k)

                return "" if v is None else str(v)

    # nested

    for v in d.values():

        if isinstance(v, dict):

            rk = _find_result_key(v, candidates)

            if rk:

                return rk

    return ""

def _flatten(obj: Any, pref: str = "", out: Dict[str, Any] = None) -> Dict[str, Any]:

    """Flatten nested dicts to "a.b.c": value."""

    out = out or {}

    if isinstance(obj, dict):

        for k, v in obj.items():

            _flatten(v, f"{pref}.{k}" if pref else k, out)

    else:

        out[pref] = obj

    return out

def _choose_variant(cfg: dict, flat: Dict[str, Any]) -> Tuple[str, dict]:

    """

    Decide which survey variant applies based on 'match' rules in config.

    """

    surveys = cfg.get("surveys", {})

    for name, sdef in surveys.items():

        match = sdef.get("match", {})

        ok = True

        for k, vals in match.items():

            v = str(flat.get(k.lower(), "")).lower()

            if not any(v == str(x).lower() for x in vals):

                ok = False

                break

        if ok:

            return name, sdef

    # fallback: first defined survey if any

    if surveys:

        name, sdef = next(iter(surveys.items()))

        return name, sdef

    return "", {}

def _normalize_to_backend_json(raw: Dict[str, Any]) -> Dict[str, Any]:

    """

    Convert free-form UI JSON into backend's expected JSON for /createRequestJSON.

    Uses CONFIG.surveys to map and order questions/answers, padding missing values.

    """

    # Build lower-cased flat lookup for variant detection

    flat_raw = {k.lower(): (str(v).lower() if isinstance(v, str) else v)

                for k, v in _flatten(raw).items()}

    # Pick a survey variant

    variant_name, survey = _choose_variant(CONFIG, flat_raw)

    log.info("Variant selected: %s", variant_name or "<none>")

    # Identity fields (with a few common fallbacks)

    request_id = _find_result_key(raw) or raw.get("request_id", "")

    name = (raw.get("name") or raw.get("full_name") or "").strip()

    email = (raw.get("email") or "").strip()

    phone = (raw.get("phoneNumber") or raw.get("phone_number") or raw.get("phone") or "")

    birth_date = raw.get("birth_date") or raw.get("date") or ""

    # Questions mapping from variant

    questions: List[str] = list(survey.get("questions", []))

    answer_keys: Dict[str, str] = survey.get("answer_keys", {})

    pad_val = survey.get("missing_answer_value", "null")

    # Where answers might be present

    answers_dict: Dict[str, Any] = raw.get("answers", {}) if isinstance(raw.get("answers"), dict) else {}

    out_answers: List[str] = []

    for q in questions:

        # find the UI key mapped to this question

        ui_key = None

        for k, qname in answer_keys.items():

            if qname == q:

                ui_key = k

                break

        val = None

        if ui_key:

            if ui_key in answers_dict:

                val = answers_dict.get(ui_key)

            elif ui_key in raw:

                val = raw.get(ui_key)

        if val is None or val == "":

            val = pad_val

        out_answers.append(str(val))

    # Apply defaults if configured (won't overwrite already found values)

    defaults = CONFIG.get("defaults", {})

    request_id = request_id or defaults.get("request_id", "")

    name       = name       or defaults.get("name", "")

    email      = email      or defaults.get("email", "")

    phone      = phone      or defaults.get("phoneNumber", "")

    birth_date = birth_date or defaults.get("birth_date", "")

    normalized = {

        "request_id": request_id,

        "email": email,

        "name": name,

        "phoneNumber": phone,

        "birth_date": birth_date,

        "questions": questions,

        "answers": out_answers

    }

    return normalized

# ----------------- Routes -----------------

@app.get("/")

def root():

    return {

        "ok": True,

        "version": "1.0.0",

        "has_backend_post_url": bool(BACKEND_POST_URL),

        "api_key_required": API_KEY_REQUIRED

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

    Accept flexible UI JSON, normalize to backend expected JSON.

    If preview=true -> return normalized only.

    Otherwise POST to BACKEND_POST_URL (/createRequestJSON) and return backend result.

    """

    _check_api_key(req)

    try:

        if not isinstance(raw, dict):

            raise ValueError("Body must be a JSON object")

        normalized = _normalize_to_backend_json(raw)

    except Exception as e:

        log.exception("Normalization failed")

        raise HTTPException(status_code=400, detail=f"Invalid JSON: {e}")

    # Quick sanity: equal lengths for Q/A

    if len(normalized.get("questions", [])) != len(normalized.get("answers", [])):

        raise HTTPException(status_code=422, detail="questions/answers length mismatch")

    if preview:

        return {"normalized_json": normalized, "note": "preview"}

    _require_backend()

    # POST to backend /createRequestJSON

    try:

        async with httpx.AsyncClient(timeout=30.0) as client:

            resp = await client.post(

                BACKEND_POST_URL,

                json=normalized,  # Backend expects JSON (not XML)

                headers={"Content-Type": "application/json"}

            )

    except httpx.RequestError as e:

        log.error("Cannot reach backend: %s", e)

        raise HTTPException(status_code=502, detail={"adapter_error": "cannot_reach_backend", "msg": str(e)})

    # Build response

    if echo:

        return {

            "sent_json": normalized,

            "backend_status": resp.status_code,

            "backend_headers": dict(resp.headers),

            "backend_body": _safe_text(resp)

        }

    # Default: pass through backend JSON/text

    content_type = resp.headers.get("content-type", "")

    if "application/json" in content_type.lower():

        try:

            return JSONResponse(status_code=resp.status_code, content=resp.json())

        except Exception:

            # backend said json but wasn't—return text

            return JSONResponse(status_code=resp.status_code, content={"body": resp.text})

    else:

        # Non-JSON backend body

        return JSONResponse(status_code=resp.status_code, content={"body": resp.text})


def _safe_text(resp: httpx.Response) -> str:

    try:

        return resp.text

    except Exception:

        # extremely rare, but guard anyway

        return "<non-textual body>"
 
