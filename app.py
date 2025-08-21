import os
import json
import logging
import pathlib
import httpx
from typing import Any, Dict, List, Optional, Tuple
from fastapi import FastAPI, Request, HTTPException, Body, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from dicttoxml import dicttoxml

# FastAPI app setup
app = FastAPI(title="Adapter (Flexible JSON → XML)")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Load config from mapping.config.json (must exist next to this script)
CFG_PATH = pathlib.Path(__file__).with_name("mapping.config.json")
if not CFG_PATH.exists():
    raise RuntimeError(f"Config file not found at {CFG_PATH}")
with CFG_PATH.open("r", encoding="utf-8") as f:
    CONFIG = json.load(f)

# Backend endpoint from environment
BACKEND_POST_URL = os.getenv("BACKEND_POST_URL", "").strip()
API_KEY = os.getenv("API_KEY", "").strip()
log = logging.getLogger("uvicorn.error")

def _require_backend():
    if not BACKEND_POST_URL:
        raise HTTPException(status_code=500, detail="BACKEND_POST_URL not configured")

def _check_api_key(req: Request):
    """If API_KEY is set, require header X-API-Key."""
    if not API_KEY:
        return
    got = req.headers.get("x-api-key") or req.headers.get("X-Api-Key")
    if got != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")

def _coalesce(*values: Any) -> Optional[Any]:
    """Return the first value that is non-null/non-empty."""
    for v in values:
        if v not in (None, ""):
            return v
    return None

def _get_payload(body: Dict[str, Any]) -> Dict[str, Any]:
    """Unwrap { data: {...} } if present."""
    return body.get("data", body)

def _pick_user_fields(p: Dict[str, Any]) -> Tuple[str,str,str,str,str]:
    """
    Extract resultKey, fullName, email, phoneNumber, dateOfBirth (as date) from payload.
    Accepts multiple aliases and uses defaults from CONFIG if missing.
    """
    result_key = _coalesce(p.get("result_key"), p.get("resultKey"), p.get("id"), CONFIG.get("defaults", {}).get("result_key"))
    if not result_key:
        raise HTTPException(status_code=400, detail="Missing mandatory field: resultKey/id")
    full_name = _coalesce(p.get("full_name"), p.get("fullName"), CONFIG.get("defaults", {}).get("full_name", ""))
    email = p.get("email", "") or CONFIG.get("defaults", {}).get("email", "")
    phone = _coalesce(p.get("phone_number"), p.get("phoneNumber"), CONFIG.get("defaults", {}).get("phone_number", ""))
    date = _coalesce(p.get("date"), p.get("dateOfBirth"), CONFIG.get("defaults", {}).get("date", ""))
    return result_key, full_name, email, phone, date

def _detect_mode(p: Dict[str, Any]) -> str:
    """
    Determine input format mode: 'structured', 'keyed', or 'answers_only'.
    """
    if isinstance(p.get("questionAnswers"), list) and p["questionAnswers"]:
        return "structured"
    ans = p.get("answers")
    if isinstance(ans, list) and ans:
        if isinstance(ans[0], dict) and "key" in ans[0] and "value" in ans[0]:
            return "keyed"
        if isinstance(ans[0], str):
            return "answers_only"
    return "none"

def _from_structured(qas: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Convert structured input (questionId & selectedOption) to uniform list."""
    out = []
    for item in qas:
        qid = item.get("questionId")
        sel = item.get("selectedOption") or {}
        out.append({
            "questionId": qid,
            "optionId": sel.get("id"),
            "value": sel.get("value"),
        })
    return out

def _from_keyed(kv_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Convert keyed answers (list of {key, value}) using config mappings.
    keyToQuestionId maps 'who'->70955, etc; options map each questionId to possible answers.
    """
    key_to_qid = CONFIG.get("keyToQuestionId", {})
    options = CONFIG.get("options", {})
    out = []
    for kv in kv_list:
        key = kv.get("key")
        val = kv.get("value")
        qid = key_to_qid.get(str(key))
        if not qid:
            log.warning(f"Unknown key in config: {key!r}")
            continue
        opt_map = options.get(str(qid), {})
        opt_id = opt_map.get(str(val), opt_map.get(val))
        out.append({"questionId": qid, "optionId": opt_id, "value": val})
    return out

def _from_answers_only(answers: List[str]) -> List[Dict[str, Any]]:
    """
    Convert a list of string answers by position. Use CONFIG['questionOrder'] to map index->questionId.
    """
    order = CONFIG.get("questionOrder", [])
    out = []
    for idx, val in enumerate(answers):
        if idx >= len(order):
            log.warning(f"Answer index {idx} has no questionOrder mapping")
            break
        qid = order[idx]
        opt_map = CONFIG.get("options", {}).get(str(qid), {})
        opt_id = opt_map.get(str(val), opt_map.get(val))
        out.append({"questionId": qid, "optionId": opt_id, "value": val})
    return out

def _build_qna(p: Dict[str, Any]) -> List[Dict[str, Any]]:
    mode = _detect_mode(p)
    if mode == "structured":
        return _from_structured(p["questionAnswers"])
    if mode == "keyed":
        return _from_keyed(p["answers"])
    if mode == "answers_only":
        return _from_answers_only(p["answers"])
    raise HTTPException(
        status_code=400,
        detail="No answers provided. Expect questionAnswers[], or answers[] (keyed), or answers[] (list of strings)."
    )

def _build_xml(result_key: str, full_name: str, email: str, phone: str, date: str, qna: List[Dict[str, Any]]) -> str:
    """
    Build the XML payload as a string. We put the Q&A list as a JSON string inside <questionAnswers>.
    """
    # Optional: enrich with question text if config provides it
    qid_to_text = CONFIG.get("qidToText", {})
    enriched = []
    for it in qna:
        qid = it.get("questionId")
        enriched.append({
            "question": qid_to_text.get(str(qid)),
            "questionId": qid,
            "isMasterQuestion": False,
            "questionType": "singleAnswer",
            "selectedOption": {
                "id": it.get("optionId"),
                "value": it.get("value"),
                "image": None,
                "score_value": 0
            },
        })
    # Serialize enriched Q&A list as JSON (embedded inside XML)
    qa_json = json.dumps(enriched, ensure_ascii=False)
    # Build dict for XML
    data = {
        "id": result_key,
        "quiz_id": "",
        "full_name": full_name,
        "email": email,
        "phone_number": phone,
        "date": date,
        "custom_inputs": [],   # empty lists become empty tags
        "products": [],
        "questionAnswers": qa_json  # JSON string as element text
    }
    # Convert to XML bytes, then decode to string
    xml_bytes = dicttoxml(data, custom_root="data", attr_type=False)
    return xml_bytes.decode("utf-8")

@app.get("/")
def root():
    return {"ok": True, "service": "flexible-adapter", "version": "1.0.0"}

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/ingest")
async def ingest(
    req: Request,
    payload: Dict[str, Any] = Body(..., description="Flexible UI JSON payload"),
    preview: bool = Query(False, description="If true, do not call backend; just show XML"),
    echo: bool = Query(True, description="If true, include backend status/body in response"),
):
    """
    Accept flexible UI JSON, normalize fields, build XML, and POST to backend /createRequest.
    Returns the XML sent and (optionally) the backend response.
    """
    _check_api_key(req)
    _require_backend()

    # 1. Normalize payload
    body = _get_payload(payload)

    # 2. Extract user fields (resultKey, full_name, email, phone, date)
    result_key, full_name, email, phone, date = _pick_user_fields(body)

    # 3. Build questionAnswers list
    qna_list = _build_qna(body)
    if not qna_list:
        raise HTTPException(status_code=400, detail="No valid answers supplied")

    # 4. Build XML payload
    xml_body = _build_xml(result_key, full_name, email, phone, date, qna_list)

    backend_status = None
    backend_headers = None
    backend_body = None

    # 5. Optionally call backend
    if not preview:
        headers = {"Content-Type": "application/xml"}
        if API_KEY:
            headers["X-API-Key"] = API_KEY
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.post(BACKEND_POST_URL, content=xml_body.encode("utf-8"), headers=headers)
            backend_status = resp.status_code
            backend_body = resp.text
            backend_headers = {"content-type": resp.headers.get("content-type")}
        except httpx.RequestError as e:
            raise HTTPException(status_code=502, detail=f"Backend request failed: {e}")

    # 6. Construct response
    result = {
        "sent_xml": xml_body,
        "context": {
            "result_key": result_key,
            "full_name": full_name,
            "email": email,
            "phone_number": phone,
            "date": date,
        }
    }
    if echo and backend_status is not None:
        result.update({
            "backend_status": backend_status,
            "backend_headers": backend_headers,
            "backend_body": backend_body,
        })
    return JSONResponse(result)
