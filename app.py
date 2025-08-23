import os, json, time
from typing import Dict, Any, List
from flask import Flask, request, jsonify
import requests
from xml.sax.saxutils import escape
app = Flask(__name__)
# --- config from env ---
BACKEND_BASE_URL = os.environ.get("BACKEND_BASE_URL", "").rstrip("/")
CREATE_PATH = os.environ.get("BACKEND_CREATE_PATH", "/CreateRequest")
FETCH_PATH  = os.environ.get("BACKEND_FETCH_PATH",  "/fetchResponse")
TIMEOUT_SEC = int(os.environ.get("BACKEND_TIMEOUT_SEC", "25"))
MAPPING_PATH = os.environ.get("MAPPING_PATH", "/home/site/wwwroot/mapping.config.json")
# --- load mapping on startup ---
def load_mapping() -> Dict[str, Any]:
   with open(MAPPING_PATH, "r", encoding="utf-8") as f:
       return json.load(f)
_MAPPING = load_mapping()
REQUIRED_USER_FIELDS = ["full_name", "email", "phone_number", "birth_date", "request_id", "result_key"]

def _find_question_key_by_label(label: str) -> str | None:
   """Resolve incoming question text to mapping key (q1_xxx)."""
   if not label:
       return None
   label_norm = label.strip().lower()
   for qkey, qdef in _mapping_questions().items():
       labels = [qdef.get("canonical_label", "")] + qdef.get("labels", [])
       for l in labels:
           if l and l.strip().lower() == label_norm:
               return qkey
   return None

def _mapping_questions() -> Dict[str, Any]:
   return _MAPPING.get("questions", {})

def _normalize_answer(qkey: str, incoming_text: str) -> Dict[str, str]:
   """
   Given a mapping question key and the user's UI answer text,
   return a dict with 'answer_text' (UI) and 'backend_value' (canonical for backend).
   Free-text questions just echo the text for backend_value.
   """
   qdef = _mapping_questions().get(qkey, {})
   opts: List[Dict[str, str]] = qdef.get("options", [])
   # if options exist, try to map to backend_key
   if opts:
       tnorm = (incoming_text or "").strip().lower()
       for opt in opts:
           if (opt.get("text","").strip().lower() == tnorm) or (opt.get("backend_key","").strip().lower() == tnorm):
               return {"answer_text": opt.get("text",""), "backend_value": opt.get("backend_key", opt.get("text",""))}
       # not found -> keep text, backend will likely reject; better to pass the UI text
       return {"answer_text": incoming_text, "backend_value": incoming_text}
   else:
       # free text
       return {"answer_text": incoming_text, "backend_value": incoming_text}

def _build_backend_xml(payload: Dict[str, Any], normalized: List[Dict[str, Any]]) -> str:
   """
   Make the XML the backend expects, like the one in your working logs.
   """
   # Required header fields
   full_name = escape(payload["full_name"])
   email     = escape(payload["email"])
   phone     = escape(payload["phone_number"])
   dob       = escape(payload["birth_date"])
   req_id    = escape(payload["request_id"])
   res_key   = escape(payload["result_key"])
   # Questions & Answers
   qa_xml_parts = []
   for item in normalized:
       qtext = escape(item["question"])
       aval  = escape(item["backend_value"])
       qa_xml_parts.append(f"<QA><Question>{qtext}</Question><Answer>{aval}</Answer></QA>")
   qa_xml = "".join(qa_xml_parts)
   # The backend response you showed uses this shape:
   return (
       f"<Request>"
       f"<RequestID>{req_id}</RequestID>"
       f"<ResultKey>{res_key}</ResultKey>"
       f"<FullName>{full_name}</FullName>"
       f"<Email>{email}</Email>"
       f"<PhoneNumber>{phone}</PhoneNumber>"
       f"<DateOfBirth>{dob}</DateOfBirth>"
       f"<QuestionAnswers>{qa_xml}</QuestionAnswers>"
       f"</Request>"
   )

def _post_create_request(xml_body: str) -> str:
   if not BACKEND_BASE_URL:
       raise RuntimeError("BACKEND_URL is not configured")
   url = f"{BACKEND_BASE_URL}{CREATE_PATH}"
   r = requests.post(url, data=xml_body.encode("utf-8"), headers={"Content-Type": "application/xml"}, timeout=TIMEOUT_SEC)
   r.raise_for_status()
   # assume backend returns a plain id or a small JSON/XML; try both
   try:
       j = r.json()
       return j.get("response_id") or j.get("id") or j.get("ResponseId") or ""
   except Exception:
       # fallback: text
       return r.text.strip()

def _get_fetch_response(response_id: str) -> str:
   url = f"{BACKEND_BASE_URL}{FETCH_PATH}"
   r = requests.get(url, params={"response_id": response_id}, timeout=TIMEOUT_SEC)
   r.raise_for_status()
   return r.text

@app.post("/adapter")
def adapter():
   try:
       payload = request.get_json(force=True)
   except Exception:
       return jsonify({"error": "Invalid JSON"}), 400
   # Validate user fields
   missing = [k for k in REQUIRED_USER_FIELDS if not str(payload.get(k, "")).strip()]
   if missing:
       return jsonify({"error": "Missing required fields", "missing": missing}), 400
   # Normalize Q&A
   incoming_qas: List[Dict[str, str]] = payload.get("questionAnswers", [])
   if not isinstance(incoming_qas, list):
       return jsonify({"error": "questionAnswers must be a list"}), 400
   normalized: List[Dict[str, Any]] = []
   missing_keys: List[str] = []
   must_have = set(_MAPPING.get("must_have_questions_keys", []))
   for qa in incoming_qas:
       question_text = (qa.get("question") or qa.get("id") or "").strip()
       answer_text   = (qa.get("answer") or "").strip()
       if not question_text:
           # skip blanks
           continue
       qkey = _find_question_key_by_label(question_text)
       if not qkey:
           if not _MAPPING.get("allow_unknown_questions", True):
               continue
           # pass through unknown questions as free text
           backend_value = answer_text
           normalized.append({
               "qkey": "unknown",
               "question": question_text,
               "answer_text": answer_text,
               "backend_value": backend_value
           })
           continue
       # map answer
       ans = _normalize_answer(qkey, answer_text)
       normalized.append({
           "qkey": qkey,
           "question": question_text,
           "answer_text": ans["answer_text"],
           "backend_value": ans["backend_value"]
       })
       if qkey in must_have:
           must_have.remove(qkey)
   if must_have:
       # tell the UI exactly which logical keys are missing
       return jsonify({"error": "Mandatory questions missing", "missing_keys": sorted(must_have)}), 400
   # Build XML and call backend
   xml_body = _build_backend_xml(payload, normalized)
   response_id = _post_create_request(xml_body)
   # short poll (single fetch; your backend looked synchronous)
   backend_response = _get_fetch_response(response_id) if response_id else ""
   return jsonify({
       "status": "ok",
       "request_id": payload["request_id"],
       "result_key": payload["result_key"],
       "normalized": normalized,
       "response_id": response_id,
       "backend_response": backend_response
   })
