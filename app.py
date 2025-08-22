# app.py
import json
import logging
import os
import re
import uuid
from typing import Any, Dict, List, Tuple, Union

import requests
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import JSONResponse
from xml.etree.ElementTree import Element, SubElement, tostring  # builtin XML

# ------------------------------------------------------------------------------
# Logging
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("adapter")

# ------------------------------------------------------------------------------
# App + CORS
# ------------------------------------------------------------------------------
app = FastAPI(title="Adapter Layer (UI ➜ XML ➜ Backend)")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # UI can be anywhere
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ------------------------------------------------------------------------------
# Configuration helpers
# ------------------------------------------------------------------------------

def _require_backend() -> str:
    url = os.getenv("BACKEND_POST_URL", "").strip()
    if not url:
        raise RuntimeError(
            "BACKEND_POST_URL is not set. Configure it in Azure App Service Settings."
        )
    return url

def _optional_response_url() -> str:
    return os.getenv("BACKEND_RESPONSE_URL", "").strip()

def _load_mapping() -> Dict[str, Any]:
    """
    Loads mapping.config.json (or the file specified by MAPPING_PATH) and
    tolerates common JSON mistakes (comments, trailing commas).
    """
    path = os.getenv("MAPPING_PATH", "mapping.config.json")

    if not os.path.exists(path):
        log.warning("Mapping file '%s' not found. Proceeding with empty mapping.", path)
        return {}

    with open(path, "r", encoding="utf-8") as f:
        raw = f.read()

    # Strip // and /* */ comments
    raw = re.sub(r"//.*?$", "", raw, flags=re.MULTILINE)
    raw = re.sub(r"/\*.*?\*/", "", raw, flags=re.DOTALL)

    # Remove trailing commas before } or ]
    raw = re.sub(r",\s*(\}|\])", r"\1", raw)

    try:
        data = json.loads(raw)
    except Exception as e:
        log.error("Failed to parse mapping file: %s", e)
        return {}

    # Normalize shape we expect:
    # {
    #   "quizId": "12952",
    #   "questions": {
    #        "Q1": { "questionId": 70955, "question": "Who are you purchasing for?" },
    #        "Q2": { "questionId": 70956, "question": "Gender" },
    #        ...
    #   }
    # }
    if not isinstance(data, dict):
        return {}
    return data


MAPPING = _load_mapping()

# ------------------------------------------------------------------------------
# Normalization (flexible UI ➜ one shape)
# ------------------------------------------------------------------------------

def _get_default_field(payload: Dict[str, Any], *keys: str, default=None):
    """Try several keys for a value (e.g., fullName vs full_name)."""
    for k in keys:
        if k in payload and payload[k] is not None:
            return payload[k]
    return default

def _listify(val) -> List[Any]:
    if val is None:
        return []
    if isinstance(val, list):
        return val
    return [val]

def _to_str_or_none(v):
    if v is None:
        return None
    return str(v)

def _build_selected_option(value: Any) -> Dict[str, Any]:
    """
    Backend likes:
      "selectedOption": { "id": 298819, "value": "Going out ...", "image": "..." }
    If we don't have an ID or image, we still send a value and a synthetic id.
    """
    if isinstance(value, dict):
        # already in shape
        so_id = value.get("id")
        if so_id is None:
            so_id = abs(hash(json.dumps(value, sort_keys=True))) % 1_000_000_000
        return {
            "id": so_id,
            "value": value.get("value"),
            "image": value.get("image"),
        }
    # plain string / number
    text = _to_str_or_none(value)
    so_id = abs(hash(text or "")) % 1_000_000_000
    return {"id": so_id, "value": text, "image": None}

def _enrich_qa_item(
    question_key: Union[str, int],
    raw_value: Any,
    mapping: Dict[str, Any],
    raw_question_text: str = None,
    raw_question_id: int = None,
) -> Dict[str, Any]:
    """
    Returns an item in the exact shape backend expects:
    {
      "question": "Q7. How do you prefer to spend your weekends?",
      "questionId": 70765,
      "selectedOption": { "id": ..., "value": "...", "image": "..." }
    }
    """
    mapped_questions = mapping.get("questions") or {}

    # Try to derive mapping entry by key (string) or numeric id.
    q_map = None
    if raw_question_id and isinstance(raw_question_id, int):
        # try find by exact id
        for _, v in mapped_questions.items():
            if isinstance(v, dict) and v.get("questionId") == raw_question_id:
                q_map = v
                break

    if not q_map:
        q_map = mapped_questions.get(str(question_key)) if isinstance(question_key, str) else None

    # Compose question text + id
    q_text = None
    q_id = None

    if q_map:
        q_text = q_map.get("question") or raw_question_text
        q_id = q_map.get("questionId") or raw_question_id
    else:
        q_text = raw_question_text
        q_id = raw_question_id

    # Fallbacks
    if q_text is None:
        q_text = _to_str_or_none(question_key)  # at least send something
    if q_id is None:
        # If still missing, synthesize a stable-ish id from the text
        q_id = abs(hash(q_text)) % 1_000_000_000

    return {
        "question": q_text,
        "questionId": q_id,
        "selectedOption": _build_selected_option(raw_value),
    }

def _normalize_question_answers(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Accept many UI shapes and normalize them into the backend's expected Q&A list.
    Supported:
      - payload["questionAnswers"] : already nearly correct
      - payload["answers"] : can be {key:value}, or list of {key,value}, or list of strings
    """
    qa: List[Dict[str, Any]] = []

    # Case 1: already "questionAnswers" array (preferred)
    if isinstance(payload.get("questionAnswers"), list):
        for item in payload["questionAnswers"]:
            if not isinstance(item, dict):
                continue
            q = item.get("question")
            qid = item.get("questionId")
            val = item.get("selectedOption") or item.get("answer") or item.get("value")
            qa.append(_enrich_qa_item(q, val, MAPPING, raw_question_text=q, raw_question_id=qid))
        if qa:
            return qa

    # Case 2: "answers" can be dict, list of dicts, list of strings
    answers = payload.get("answers")
    if isinstance(answers, dict):
        for k, v in answers.items():
            qa.append(_enrich_qa_item(k, v, MAPPING))
    elif isinstance(answers, list):
        for i, v in enumerate(answers, start=1):
            if isinstance(v, dict):
                # { "key": "...", "value": "..." } or already { "question": "...", "selectedOption": {...} }
                key = v.get("key") or v.get("question") or f"Q{i}"
                value = v.get("value") or v.get("selectedOption") or v.get("answer")
                qid = v.get("questionId")
                qa.append(_enrich_qa_item(key, value, MAPPING, raw_question_text=v.get("question"), raw_question_id=qid))
            else:
                # simple list of strings
                qa.append(_enrich_qa_item(f"Q{i}", v, MAPPING))
    return qa

def _normalize_payload(payload: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """
    Returns (header_fields, questionAnswers_list)
    header_fields contains the camelCase keys your backend wants.
    """
    # Accept snake_case or camelCase from UI; *emit camelCase* for backend
    full_name = _get_default_field(payload, "fullName", "full_name", "name", default=None)
    email = _get_default_field(payload, "email", "email_id", default=None)
    phone = _get_default_field(payload, "phoneNumber", "phone_number", "mobile", "mobileNumber", default=None)
    dob = _get_default_field(payload, "dateOfBirth", "date_of_birth", "birthdate", "dob", default=None)
    result_key = _get_default_field(payload, "resultKey", "userId", "user_id", "result_key", default=None)
    quiz_id = _get_default_field(payload, "quizId", "quiz_id", default="")

    # If result_key missing, generate one (backend maps to requestId)
    if not result_key:
        result_key = f"{uuid.uuid4()}"

    qa_list = _normalize_question_answers(payload)

    header = {
        "id": result_key,           # backend maps this as requestId/responseId
        "quizId": quiz_id or "",
        "fullName": _to_str_or_none(full_name),
        "email": _to_str_or_none(email),
        "phoneNumber": _to_str_or_none(phone),
        "dateOfBirth": _to_str_or_none(dob),
        # optional fields that backend in examples tolerated:
        "termsConditions": _get_default_field(payload, "termsConditions", "terms", default=0),
        "website": _get_default_field(payload, "website", default=None),
        "organization": _get_default_field(payload, "organization", "org", default=None),
        "address1": _get_default_field(payload, "address1", "address_1", default=None),
        "address2": _get_default_field(payload, "address2", "address_2", default=None),
        "city": _get_default_field(payload, "city", default=None),
        "country": _get_default_field(payload, "country", default=None),
        "state": _get_default_field(payload, "state", default=None),
        "zipCode": _get_default_field(payload, "zipCode", "zipcode", "pin", default=None),
        "customInputs": payload.get("customInputs", None),
        "products": payload.get("products", None),
    }
    return header, qa_list

# ------------------------------------------------------------------------------
# XML construction (back-end expects XML; questionAnswers value is JSON string)
# ------------------------------------------------------------------------------

def _build_xml(header: Dict[str, Any], qa_list: List[Dict[str, Any]]) -> str:
    """
    Compose XML that mirrors your backend's example.
    Root element: <data> (matches the JSON 'data' object in the example).
    <questionAnswers> contains a JSON string (the backend converts XML ➜ JSON internally).
    """
    # Prepare the QA JSON string
    qa_json_str = json.dumps(qa_list, ensure_ascii=False)

    root = Element("data")

    # Emit header fields in EXACT camelCase the backend expects
    def emit(name: str, value: Any):
        el = SubElement(root, name)
        if value is None:
            # send empty node
            el.text = ""
        else:
            el.text = str(value)

    emit("id", header.get("id"))
    emit("quizId", header.get("quizId"))
    emit("fullName", header.get("fullName"))
    emit("email", header.get("email"))
    emit("phoneNumber", header.get("phoneNumber"))
    emit("dateOfBirth", header.get("dateOfBirth"))
    emit("termsConditions", header.get("termsConditions"))
    emit("website", header.get("website"))
    emit("organization", header.get("organization"))
    emit("address1", header.get("address1"))
    emit("address2", header.get("address2"))
    emit("city", header.get("city"))
    emit("country", header.get("country"))
    emit("state", header.get("state"))
    emit("zipCode", header.get("zipCode"))
    emit("customInputs", json.dumps(header.get("customInputs")) if header.get("customInputs") is not None else "")
    emit("products", json.dumps(header.get("products")) if header.get("products") is not None else "")

    # IMPORTANT: backend expects questionAnswers as JSON string inside XML
    qa_el = SubElement(root, "questionAnswers")
    qa_el.text = qa_json_str

    xml_bytes = tostring(root, encoding="utf-8", method="xml")
    return xml_bytes.decode("utf-8")

# ------------------------------------------------------------------------------
# Routes
# ------------------------------------------------------------------------------

@app.get("/health")
def health():
    return {"status": "ok", "mappingLoaded": bool(MAPPING)}

@app.post("/createRequest")
async def create_request(request: Request):
    """
    Adapter entrypoint. Accept flexible JSON from UI, map & normalize, build XML,
    and forward to your backend's /createRequest (always called).
    """
    try:
        payload = await request.json()
    except Exception:
        # If UI posts raw text, try to parse; else return 400
        raw = await request.body()
        raise HTTPException(status_code=400, detail=f"Invalid JSON body. Raw: {raw[:200]!r}")

    # Support wrapper {"data": {...}} like your example
    if isinstance(payload, dict) and "data" in payload and isinstance(payload["data"], dict):
        body = payload["data"]
    else:
        body = payload if isinstance(payload, dict) else {}

    # Normalize ➜ XML
    header, qa = _normalize_payload(body)
    xml_payload = _build_xml(header, qa)

    # POST to backend
    backend_url = _require_backend()
    try:
        resp = requests.post(
            backend_url,
            data=xml_payload.encode("utf-8"),
            headers={"Content-Type": "application/xml"},
            timeout=int(os.getenv("BACKEND_TIMEOUT_SEC", "30")),
        )
    except requests.RequestException as e:
        log.error("Error calling backend: %s", e)
        raise HTTPException(status_code=502, detail=f"Backend unreachable: {e}")

    # Best-effort parse backend response
    backend_text = resp.text
    try:
        backend_json = resp.json()
    except Exception:
        backend_json = None

    return JSONResponse(
        status_code=200,
        content={
            "sent_xml": xml_payload,            # so you can verify exact payload
            "backend_status": resp.status_code,
            "backend_headers": dict(resp.headers),
            "backend_body_json": backend_json,  # if backend returned JSON
            "backend_body_text": backend_text,  # otherwise raw text (XML/HTML/etc.)
        },
    )

@app.get("/response")
def get_response(requestId: str = None, resultKey: str = None):
    """
    (Optional) Convenience passthrough to your backend's response endpoint.
    You can call:  GET /response?requestId=<id>
                   GET /response?resultKey=<key>   (maps to requestId)
    """
    base = _optional_response_url()
    if not base:
        raise HTTPException(status_code=501, detail="BACKEND_RESPONSE_URL not configured.")
    rid = requestId or resultKey
    if not rid:
        raise HTTPException(status_code=400, detail="Provide ?requestId=... or ?resultKey=...")

    try:
        r = requests.get(base, params={"requestId": rid}, timeout=int(os.getenv("BACKEND_TIMEOUT_SEC", "30")))
        body_text = r.text
        try:
            body_json = r.json()
        except Exception:
            body_json = None
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"Backend response endpoint unreachable: {e}")

    return {
        "requestId": rid,
        "backend_status": r.status_code,
        "backend_headers": dict(r.headers),
        "backend_body_json": body_json,
        "backend_body_text": body_text,
    }

# ------------------------------------------------------------------------------
# Root (optional)
# ------------------------------------------------------------------------------
@app.get("/")
def root():
    return {
        "message": "Adapter is running. POST your flexible UI JSON to /createRequest.",
        "health": "/health",
        "docs": "/docs",
    }
