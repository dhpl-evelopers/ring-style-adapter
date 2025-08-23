import os
import json
import time
import logging
from typing import Dict, Any, List, Tuple, Optional
from flask import Flask, request, jsonify
import requests
from xml.etree.ElementTree import Element, SubElement, tostring

# --------------------------------------------------------------------------------------
# Flask
# --------------------------------------------------------------------------------------
app = Flask(_name_)
app.config["JSON_SORT_KEYS"] = False

# --------------------------------------------------------------------------------------
# Env
# --------------------------------------------------------------------------------------
BACKEND_BASE_URL   = os.getenv("BACKEND_BASE_URL", "").rstrip("/")
CREATE_PATH        = os.getenv("BACKEND_CREATE_PATH", "/createRequest")
FETCH_PATH         = os.getenv("BACKEND_FETCH_PATH", "/fetchResponse")
BACKEND_TIMEOUT_S  = int(os.getenv("BACKEND_TIMEOUT_SEC", "25"))
MAPPING_PATH       = os.getenv("MAPPING_PATH", "/home/site/wwwroot/mapping.json")
API_KEY_REQUIRED   = os.getenv("API_KEY_REQUIRED", "false").lower() == "true"

if not BACKEND_BASE_URL:
    app.logger.warning("BACKEND_BASE_URL not configured. End‑to‑end calls will fail.")

# --------------------------------------------------------------------------------------
# Mapping loader
# --------------------------------------------------------------------------------------
class Mapping:
    def _init_(self, raw: Dict[str, Any]) -> None:
        self.allow_unknown = bool(raw.get("allow_unknown_questions", False))
        self.must_have_keys: List[str] = list(raw.get("must_have_questions_keys", []))
        self.questions: Dict[str, Dict[str, Any]] = dict(raw.get("questions", {}))
        # Build quick reverse index of label -> key and option text normalization
        self.label_to_key: Dict[str, str] = {}
        self.normalized_options: Dict[str, Dict[str, str]] = {}

        for q_key, meta in self.questions.items():
            labels = [meta.get("canonical_label", "")] + meta.get("labels", [])
            for lbl in labels:
                if lbl:
                    self.label_to_key[lbl.strip().lower()] = q_key

            # options set (for coded questions)
            opts = meta.get("options")
            if isinstance(opts, list):
                norm = {}
                for o in opts:
                    if isinstance(o, str):
                        norm[o.strip().lower()] = o  # lowercase -> canonical option
                self.normalized_options[q_key] = norm

    def resolve_q_key(self, incoming_label_or_key: str) -> Optional[str]:
        """Accept either a mapping key (e.g., q4_occasion) or a UI label string."""
        if not incoming_label_or_key:
            return None
        k = incoming_label_or_key.strip()
        if k in self.questions:
            return k
        return self.label_to_key.get(k.lower())

    def normalize_answer(self, q_key: str, answer: str) -> str:
        """If a question has coded options, normalize to a canonical option text."""
        answer = (answer or "").strip()
        if not answer:
            return answer
        norm_table = self.normalized_options.get(q_key)
        if not norm_table:
            return answer  # free text question
        # try exact/lower match
        return norm_table.get(answer.lower(), answer)

def load_mapping(path: str) -> Mapping:
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    return Mapping(raw)

# Load once at startup
try:
    MAPPING = load_mapping(MAPPING_PATH)
    app.logger.info(f"Loaded mapping from {MAPPING_PATH}")
except Exception as e:
    app.logger.exception(f"Failed to load mapping: {e}")
    # still allow /health to work; /adapter will 500 until mapping is fixed
    MAPPING = None  # type: ignore

# --------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------
def require_api_key(headers: Dict[str, str]) -> Optional[str]:
    """Return error message if API key missing when required; else None."""
    if not API_KEY_REQUIRED:
        return None
    key = headers.get("x-api-key") or headers.get("X-API-Key")
    if not key:
        return "Missing API key header 'x-api-key'."
    # If you need to validate value(s), add here.
    return None

def validate_and_normalize(payload: Dict[str, Any], mapping: Mapping) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """
    Returns (user_meta, normalized_qas).
    - user_meta carries full_name/email/phone_number/birth_date/request_id/result_key
    - normalized_qas is a list of {key, question_text, answer_text}
    Raises ValueError with an error JSON to return if invalid.
    """
    # Basic user fields (empty strings are fine — backend expects tags)
    user_meta = {
        "full_name":   payload.get("full_name") or payload.get("name") or "",
        "email":       payload.get("email") or "",
        "phone_number":payload.get("phone_number") or "",
        "birth_date":  payload.get("birth_date") or "",
        "request_id":  payload.get("request_id") or "",
        "result_key":  payload.get("result_key") or "",
        "test_mode":   payload.get("test_mode") or "live"
    }

    raw_qas: List[Dict[str, Any]] = payload.get("questionAnswers") or payload.get("question_answers") or []
    if not isinstance(raw_qas, list):
        raise ValueError(json.dumps({"error": "questionAnswers must be a list"}))

    normalized: List[Dict[str, Any]] = []
    seen_keys = set()

    for item in raw_qas:
        if not isinstance(item, dict):
            continue

        # Accept either {id: "...", answer: "..."} OR {question:"...", answer:"..."}
        q_label_or_key = item.get("question") or item.get("id") or item.get("key") or ""
        answer_text = item.get("answer") or ""

        q_key = mapping.resolve_q_key(str(q_label_or_key))
        if not q_key:
            if mapping.allow_unknown:
                continue  # drop unknown questions silently
            else:
                raise ValueError(json.dumps({
                    "error": "Unknown question",
                    "question_received": q_label_or_key
                }))

        # Normalize option text (for coded questions)
        answer_norm = mapping.normalize_answer(q_key, str(answer_text))

        meta = mapping.questions[q_key]
        normalized.append({
            "key": q_key,
            "question_text": meta.get("canonical_label") or q_key,
            "answer_text": answer_norm
        })
        seen_keys.add(q_key)

    # Conditional requirement: relation when Others
    purchasing = next((x for x in normalized if x["key"] == "q1_purchasing_for"), None)
    if purchasing and purchasing["answer_text"].strip().lower() == "others":
        if "q1b_relation" not in seen_keys:
            raise ValueError(json.dumps({
                "error": "Mandatory question missing",
                "missing_keys": ["q1b_relation"]
            }))

    # Enforce global must-have keys
    missing = [k for k in mapping.must_have_keys if k not in seen_keys]
    if missing:
        raise ValueError(json.dumps({
            "error": "Mandatory questions missing",
            "missing_keys": missing
        }))

    # Sort QAs in a sensible order (by must-have list first, then others)
    order = {k: i for i, k in enumerate(mapping.must_have_keys)}
    normalized.sort(key=lambda x: order.get(x["key"], 9999))

    return user_meta, normalized

def to_backend_xml(user_meta: Dict[str, Any], qas: List[Dict[str, Any]]) -> str:
    """
    Build the XML the backend expects, using canonical question text + final answer text.
    """
    req = Element("Request")

    SubElement(req, "RequestId").text  = user_meta.get("request_id", "")
    SubElement(req, "ResultKey").text  = user_meta.get("result_key", "")
    SubElement(req, "FullName").text   = user_meta.get("full_name", "")
    SubElement(req, "Email").text      = user_meta.get("email", "")
    SubElement(req, "PhoneNumber").text= user_meta.get("phone_number", "")
    SubElement(req, "DateOfBirth").text= user_meta.get("birth_date", "")

    qas_root = SubElement(req, "QuestionAnswers")
    for qa in qas:
        qa_el = SubElement(qas_root, "QA")
        SubElement(qa_el, "Question").text = qa["question_text"]
        SubElement(qa_el, "Answer").text   = qa["answer_text"]

    # tostring returns bytes; backend is fine with no XML declaration
    return tostring(req, encoding="unicode")

def call_backend(xml_body: str, test_mode: str) -> Dict[str, Any]:
    """
    POST /createRequest with XML; then poll GET /fetchResponse?responseId=...
    until we get a final payload or timeout.
    """
    if not BACKEND_BASE_URL:
        raise RuntimeError("BACKEND_URL is not configured")

    # Create
    create_url = f"{BACKEND_BASE_URL}{CREATE_PATH}"
    headers = {"Content-Type": "application/xml"}
    resp = requests.post(create_url, data=xml_body.encode("utf-8"), headers=headers, timeout=BACKEND_TIMEOUT_S)

    if resp.status_code >= 400:
        raise RuntimeError(f"Backend createRequest failed: {resp.status_code} {resp.text}")

    create_json = {}
    try:
        create_json = resp.json()
    except Exception:
        # Some backends return XML; try to pass through
        return {"backend_raw": resp.text}

    response_id = create_json.get("response_id") or create_json.get("ResponseId") or create_json.get("id")
    if not response_id:
        # If backend already returns a final recommendation, pass it back.
        return {"backend_create": create_json}

    # Poll fetchResponse
    fetch_url = f"{BACKEND_BASE_URL}{FETCH_PATH}"
    deadline = time.time() + BACKEND_TIMEOUT_S

    while time.time() < deadline:
        try:
            r = requests.get(fetch_url, params={"responseId": response_id}, timeout=BACKEND_TIMEOUT_S)
            if r.status_code >= 400:
                raise RuntimeError(f"Backend fetchResponse failed: {r.status_code} {r.text}")
            data = r.json()
        except Exception as e:
            raise RuntimeError(f"Error parsing fetchResponse: {e}")

        status = str(data.get("status") or data.get("Status") or "").lower()
        if status in ("done", "completed", "ok", "success"):
            return {"backend_final": data}

        # not done yet – short sleep, then continue
        time.sleep(0.7)

    # timed out – return last known data if any
    return {"backend_fetch_timeout": True, "response_id": response_id}

# --------------------------------------------------------------------------------------
# Routes
# --------------------------------------------------------------------------------------
@app.get("/")
def health():
    return jsonify({"status": "ok", "mapping_loaded": MAPPING is not None})

@app.post("/adapter")
def adapter():
    # API key gate (optional)
    err = require_api_key(request.headers)
    if err:
        return jsonify({"error": err}), 401

    if MAPPING is None:
        return jsonify({"error": "Mapping not loaded on server"}), 500

    # parse JSON
    try:
        payload = request.get_json(force=True, silent=False)
        if not isinstance(payload, dict):
            raise ValueError("Body must be JSON object")
    except Exception as e:
        return jsonify({"error": f"Invalid JSON body: {e}"}), 400

    # normalize
    try:
        user_meta, qas = validate_and_normalize(payload, MAPPING)
    except ValueError as ve:
        # message is a JSON string
        try:
            return jsonify(json.loads(str(ve))), 400
        except Exception:
            return jsonify({"error": str(ve)}), 400

    # If caller only wants normalization (useful for testing)
    if str(payload.get("normalize_only", "")).lower() in ("1", "true", "yes"):
        return jsonify({
            "status": "ok",
            "request_id": user_meta["request_id"],
            "result_key": user_meta["result_key"],
            "normalized": [
                {"key": qa["key"], "question": qa["question_text"], "answer": qa["answer_text"]}
                for qa in qas
            ]
        })

    # Build XML for backend
    xml_body = to_backend_xml(user_meta, qas)

    # Call backend end‑to‑end
    try:
        backend_result = call_backend(xml_body, user_meta["test_mode"])
    except Exception as e:
        app.logger.exception("Backend call failed")
        return jsonify({"details": str(e), "xml": xml_body}), 500

    return jsonify({
        "status": "ok",
        "request_id": user_meta["request_id"],
        "result_key": user_meta["result_key"],
        "normalized": [
            {"key": qa["key"], "question": qa["question_text"], "answer": qa["answer_text"]}
            for qa in qas
        ],
        "backend": backend_result
    })

# For local dev (NOT used by gunicorn in Azure)
if _name_ == "_main_":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
