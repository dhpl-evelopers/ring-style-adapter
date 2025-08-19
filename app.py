import os
import json
import logging
import pathlib
from typing import Any, Dict, List, Tuple

import httpx
from fastapi import FastAPI, Request, HTTPException, Body, Query
from fastapi.responses import JSONResponse

# ---------------- Logging ----------------
log = logging.getLogger("adapter")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ---------------- FastAPI ----------------
app = FastAPI(title="Ring Style Adapter (flex → JSON)", version="1.0.0")

# ---------------- Environment ----------------
# Set these in Azure App Service > Environment variables
BACKEND_POST_URL = os.getenv("BACKEND_POST_URL", "").strip()   # e.g. https://<api>.azurewebsites.net/createRequestJSON
API_KEY_REQUIRED = os.getenv("API_KEY_REQUIRED", "false").lower() == "true"
API_KEY          = os.getenv("API_KEY", "")

def _require_backend():
    if not BACKEND_POST_URL:
        raise HTTPException(status_code=500, detail="BACKEND_POST_URL is not configured")

def _check_api_key(req: Request):
    if not API_KEY_REQUIRED:
        return
    if not API_KEY or req.headers.get("x-api-key") != API_KEY:
        raise HTTPException(status_code=401, detail="unauthorized")

# ---------------- Config ----------------
# mapping.config.json sits next to app.py
CFG_PATH = pathlib.Path(__file__).with_name("mapping.config.json")

def _load_config() -> Dict[str, Any]:
    try:
        with open(CFG_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        log.error("Could not read mapping.config.json: %s", e)
        return {}

CONFIG = _load_config()

# Helpful shorthands (with fallbacks)
FIELD_MAP: Dict[str, str] = {k.lower(): v for k, v in CONFIG.get("field_map", {}).items()}
ANSWER_KEY_TO_QUESTION: Dict[str, str] = CONFIG.get("answer_key_to_question", {})
QUESTION_ORDER: Dict[str, List[str]] = CONFIG.get("question_order", {})   # optional
DEFAULTS: Dict[str, Any] = CONFIG.get("defaults", {})

# ---------------- Utility: normalize answers ----------------
def _answers_from_anything(body: Dict[str, Any]) -> List[Dict[str, str]]:
    """
    Turn whatever came from UI into the backend list:
        [{"question": "...", "answer": "..."}]
    Accepts:
      • already-correct: body["answers"] = [{"question":..., "answer":...}, ...]
      • array of objects with different keys (question / selectedOption / value)
      • a dict of simple key→value answers, which we map via ANSWER_KEY_TO_QUESTION
    """

    # Case 1: already [{question, answer}]
    if isinstance(body.get("answers"), list) and body["answers"]:
        if all(isinstance(a, dict) and "question" in a and "answer" in a for a in body["answers"]):
            return [{"question": str(a["question"]), "answer": str(a["answer"])} for a in body["answers"]]

    out: List[Dict[str, str]] = []

    # Case 2: list of items but with various shapes
    if isinstance(body.get("answers"), list):
        for item in body["answers"]:
            if not isinstance(item, dict):
                continue
            q = (item.get("question")
                 or item.get("Q")
                 or item.get("label")
                 or item.get("text"))
            a = (item.get("answer")
                 or item.get("value")
                 or (item.get("selectedOption") or {}).get("value"))
            if q is None:
                # Try infer from a known key, e.g. {"gender":"Male"}
                for k, v in item.items():
                    if isinstance(v, (str, int, float)) and k in ANSWER_KEY_TO_QUESTION:
                        q = ANSWER_KEY_TO_QUESTION[k]
                        a = str(v)
                        break
            if q is not None and a is not None:
                out.append({"question": str(q), "answer": str(a)})

    # Case 3: dict of key→value (flat)
    if not out:
        for k, v in body.items():
            if k in ("answers", "request_id", "name", "full_name", "email",
                     "phone", "phone_number", "phoneNumber", "birth_date",
                     "dob", "purchase_for", "gender"):
                continue
            if isinstance(v, (str, int, float)) and k in ANSWER_KEY_TO_QUESTION:
                out.append({"question": ANSWER_KEY_TO_QUESTION[k], "answer": str(v)})

    return out

def _pick_question_order(body: Dict[str, Any]) -> List[str]:
    """
    Optional ordering by scenario:
      • purchase_for: "self" | "others"
      • gender: "female" | "male"
    Config example:
      "question_order": {
        "self": ["Q1. Who are you?","Q2. Gender", ...],
        "others-female": [...],
        "others-male": [...]
      }
    """
    purchase_for = str(body.get("purchase_for", "")).strip().lower()
    gender = str(body.get("gender", "")).strip().lower()

    # Try most specific first
    if purchase_for == "others" and gender:
        key = f"others-{gender}"
        if key in QUESTION_ORDER:
            return QUESTION_ORDER[key]
    if purchase_for in QUESTION_ORDER:
        return QUESTION_ORDER[purchase_for]
    return QUESTION_ORDER.get("default", [])

def _order_answers_if_possible(answers: List[Dict[str, str]], desired_order: List[str]) -> List[Dict[str, str]]:
    if not desired_order:
        return answers
    order_index = {q: i for i, q in enumerate(desired_order)}
    return sorted(answers, key=lambda a: order_index.get(a.get("question", ""), 10_000))

# ---------------- Utility: normalize whole body → backend JSON ----------------
def _normalize_to_backend_json(body: Dict[str, Any]) -> Dict[str, Any]:
    """
    Expected backend JSON (as seen in your /createRequestJSON code path):
      {
        "request_id": "...",
        "email": "...",
        "name": "...",
        "phoneNumber": "...",
        "birth_date": "...",
        "qna": [
           {"question": "...", "answer": "..."},
           ...
        ]
      }
    """
    norm: Dict[str, Any] = dict(DEFAULTS)

    # request_id (many possible names)
    for candidate in ("request_id", "result_key", "response_id", "resultsKey", "resultKey"):
        if candidate in body:
            norm["request_id"] = str(body[candidate])
            break

    # simple fields (case-insensitive mapping support)
    flat_map = {
        "name": ["name", "full_name"],
        "email": ["email"],
        "phoneNumber": ["phoneNumber", "phone_number", "phone"],
        "birth_date": ["birth_date", "dob", "date"],
    }
    for target, keys in flat_map.items():
        for k in keys:
            if k in body:
                norm[target] = body[k]
                break

    # answers
    answers = _answers_from_anything(body)

    # optional ordering by scenario
    answers = _order_answers_if_possible(answers, _pick_question_order(body))

    # attach
    norm["qna"] = answers
    return norm

# ---------------- Routes ----------------
@app.get("/")
def root():
    return {
        "ok": True,
        "requires_api_key": API_KEY_REQUIRED,
        "backend_configured": bool(BACKEND_POST_URL),
        "config_loaded": bool(CONFIG),
        "sample_keys": list(ANSWER_KEY_TO_QUESTION.keys())[:6],
    }

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/ingest")
async def ingest(
    raw: dict = Body(...),  # <— makes the request body appear in Swagger
    preview: bool = Query(False, description="Return normalized JSON without calling backend"),
    echo: bool = Query(False, description="Return backend status + body"),
    req: Request = None
):
    """
    Accept flexible UI JSON, normalize to backend-expected JSON.
    If `preview=true`, return JSON only.
    Otherwise post to BACKEND_POST_URL (/createRequestJSON) and return status/body.
    """
    _check_api_key(req)

    normalized = _normalize_to_backend_json(raw)

    # quick sanity (not fatal; just logs)
    missing = [k for k in ["request_id", "name", "email"] if not normalized.get(k)]
    if missing:
        log.warning("Normalized JSON missing fields: %s", missing)

    if preview:
        return JSONResponse({"sent_json": normalized})

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

    # Return what we sent + backend response (always super helpful)
    if echo:
        ct = resp.headers.get("content-type", "")
        body_text = resp.text
        return JSONResponse({
            "sent_json": normalized,
            "backend_status": resp.status_code,
            "backend_headers": {"content-type": ct},
            "backend_body": body_text,
        }, status_code=200 if 200 <= resp.status_code < 300 else 502)

    # Non-echo: bubble backend failure as 502
    if not (200 <= resp.status_code < 300):
        return JSONResponse(
            status_code=502,
            content={"detail": {"backend_status": resp.status_code, "body": resp.text}},
        )
    # Success: pass-through backend body (usually JSON)
    try:
        return JSONResponse(resp.json())
    except Exception:
        return JSONResponse({"body": resp.text})
