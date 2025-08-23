# adapter_app.py
import os
import json
import time
from typing import Dict, Any, List, Tuple, Optional
from flask import Flask, request, jsonify
import requests
from xml.etree.ElementTree import Element, SubElement, tostring

app = Flask(__name__)
app.config["JSON_SORT_KEYS"] = False

# ------------------ ENV ------------------
BACKEND_BASE_URL   = os.getenv("BACKEND_BASE_URL", "").rstrip("/")
CREATE_PATH        = os.getenv("BACKEND_CREATE_PATH", "/createRequest")
FETCH_PATH         = os.getenv("BACKEND_FETCH_PATH", "/fetchResponse")
BACKEND_TIMEOUT_S  = int(os.getenv("BACKEND_TIMEOUT_SEC", "25"))
MAPPING_PATH       = os.getenv("MAPPING_PATH", "/home/site/wwwroot/mapping.json")
API_KEY_REQUIRED   = os.getenv("API_KEY_REQUIRED", "false").lower() == "true"

if not BACKEND_BASE_URL:
    app.logger.warning("BACKEND_BASE_URL not configured. End‑to‑end calls will fail.")

# ------------------ Mapping ------------------
class Mapping:
    def __init__(self, raw: Dict[str, Any]) -> None:
        self.allow_unknown = bool(raw.get("allow_unknown_questions", False))
        self.must_have_keys: List[str] = list(raw.get("must_have_questions_keys", []))
        self.questions: Dict[str, Dict[str, Any]] = dict(raw.get("questions", {}))
        self.label_to_key: Dict[str, str] = {}
        self.normalized_options: Dict[str, Dict[str, str]] = {}

        for q_key, meta in self.questions.items():
            labels = [meta.get("canonical_label", "")] + meta.get("labels", [])
            for lbl in labels:
                if lbl:
                    self.label_to_key[lbl.strip().lower()] = q_key
            opts = meta.get("options")
            if isinstance(opts, list):
                norm = {}
                for o in opts:
                    if isinstance(o, str):
                        norm[o.strip().lower()] = o
                self.normalized_options[q_key] = norm

    def resolve_q_key(self, incoming_label_or_key: str) -> Optional[str]:
        if not incoming_label_or_key:
            return None
        k = incoming_label_or_key.strip()
        if k in self.questions:
            return k
        return self.label_to_key.get(k.lower())

    def normalize_answer(self, q_key: str, answer: str) -> str:
        answer = (answer or "").strip()
        if not answer:
            return answer
        norm_table = self.normalized_options.get(q_key)
        if not norm_table:
            return answer
        return norm_table.get(answer.lower(), answer)

def load_mapping(path: str) -> Mapping:
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    return Mapping(raw)

try:
    MAPPING = load_mapping(MAPPING_PATH)
    app.logger.info(f"Loaded mapping from {MAPPING_PATH}")
except Exception as e:
    app.logger.exception(f"Failed to load mapping: {e}")
    MAPPING = None

# ------------------ Helpers ------------------
def require_api_key(headers: Dict[str, str]) -> Optional[str]:
    if not API_KEY_REQUIRED:
        return None
    key = headers.get("x-api-key") or headers.get("X-API-Key")
    if not key:
        return "Missing API key header 'x-api-key'."
    return None

def validate_and_normalize(payload: Dict[str, Any], mapping: Mapping) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    user_meta = {
        "full_name":    payload.get("full_name") or payload.get("name") or "",
        "email":        payload.get("email") or "",
        "phone_number": payload.get("phone_number") or "",
        "birth_date":   payload.get("birth_date") or "",
        "request_id":   payload.get("request_id") or "",
        "result_key":   payload.get("result_key") or "",
        "test_mode":    payload.get("test_mode") or "live",
    }

    raw_qas: List[Dict[str, Any]] = payload.get("questionAnswers") or payload.get("question_answers") or []
    if not isinstance(raw_qas, list):
        raise ValueError(json.dumps({"error": "questionAnswers must be a list"}))

    normalized: List[Dict[str, Any]] = []
    seen_keys = set()

    for item in raw_qas:
        if not isinstance(item, dict):
            continue
        q_label_or_key = item.get("question") or item.get("id") or item.get("key") or ""
        answer_text    = item.get("answer") or ""

        q_key = mapping.resolve_q_key(str(q_label_or_key))
        if not q_key:
            if mapping.allow_unknown:
                continue
            raise ValueError(json.dumps({"error": "Unknown question", "question_received": q_label_or_key}))

        answer_norm = mapping.normalize_answer(q_key, str(answer_text))
        meta = mapping.questions[q_key]
        normalized.append({
            "key": q_key,
            "question_text": meta.get("canonical_label") or q_key,
            "answer_text": answer_norm,
        })
        seen_keys.add(q_key)

    purchasing = next((x for x in normalized if x["key"] == "q1_purchasing_for"), None)
    if purchasing and purchasing["answer_text"].strip().lower() == "others":
        if "q1b_relation" not in seen_keys:
            raise ValueError(json.dumps({"error": "Mandatory question missing", "missing_keys": ["q1b_relation"]}))

    missing = [k for k in mapping.must_have_keys if k not in seen_keys]
    if missing:
        raise ValueError(json.dumps({"error": "Mandatory questions missing", "missing_keys": missing}))

    order = {k: i for i, k in enumerate(mapping.must_have_keys)}
    normalized.sort(key=lambda x: order.get(x["key"], 9999))
    return user_meta, normalized

def to_backend_xml(user_meta: Dict[str, Any], qas: List[Dict[str, Any]]) -> str:
    # Emit snake_case everywhere to match backend that does root.find('.//full_name')
    req = Element("request")
    SubElement(req, "request_id").text    = user_meta.get("request_id", "")
    SubElement(req, "result_key").text    = user_meta.get("result_key", "")
    SubElement(req, "full_name").text     = user_meta.get("full_name", "")
    SubElement(req, "email").text         = user_meta.get("email", "")
    SubElement(req, "phone_number").text  = user_meta.get("phone_number", "")
    SubElement(req, "date_of_birth").text = user_meta.get("birth_date", "")

    qas_root = SubElement(req, "question_answers")
    for qa in qas:
        qa_el = SubElement(qas_root, "qa")
        SubElement(qa_el, "question").text = qa["question_text"]
        SubElement(qa_el, "answer").text   = qa["answer_text"]

    return tostring(req, encoding="unicode")

def call_backend(xml_body: str, test_mode: str) -> Dict[str, Any]:
    if not BACKEND_BASE_URL:
        raise RuntimeError("BACKEND_BASE_URL is not configured")
    create_url = f"{BACKEND_BASE_URL}{CREATE_PATH}"
    headers = {"Content-Type": "application/xml", "Accept": "application/json"}
    resp = requests.post(create_url, data=xml_body.encode("utf-8"), headers=headers, timeout=BACKEND_TIMEOUT_S)
    if resp.status_code >= 400:
        raise RuntimeError(f"Backend createRequest failed: {resp.status_code} {resp.text}")

    try:
        create_json = resp.json()
    except Exception:
        return {"backend_raw": resp.text}

    response_id = create_json.get("response_id") or create_json.get("ResponseId") or create_json.get("id")
    if not response_id:
        return {"backend_create": create_json}

    fetch_url = f"{BACKEND_BASE_URL}{FETCH_PATH}"
    deadline = time.time() + BACKEND_TIMEOUT_S
    last_data = None

    while time.time() < deadline:
        r = requests.get(fetch_url, params={"response_id": response_id}, headers={"Accept": "application/json"}, timeout=BACKEND_TIMEOUT_S)
        if r.status_code >= 400:
            raise RuntimeError(f"Backend fetchResponse failed: {r.status_code} {r.text}")
        try:
            data = r.json()
        except Exception as e:
            raise RuntimeError(f"Error parsing fetchResponse: {e}")

        last_data = data
        status = str(data.get("status") or data.get("Status") or "").lower()
        if status in ("done", "completed", "ok", "success"):
            return {"backend_final": data}
        time.sleep(0.7)

    return {"backend_fetch_timeout": True, "response_id": response_id, "last": last_data}

# ------------------ Routes ------------------
@app.get("/")
def health():
    return jsonify({"status": "ok", "mapping_loaded": MAPPING is not None})

@app.post("/adapter")
def adapter():
    err = require_api_key(request.headers)
    if err:
        return jsonify({"error": err}), 401

    if MAPPING is None:
        return jsonify({"error": "Mapping not loaded on server"}), 500

    try:
        payload = request.get_json(force=True, silent=False)
        if not isinstance(payload, dict):
            raise ValueError("Body must be JSON object")
    except Exception as e:
        return jsonify({"error": f"Invalid JSON body: {e}"}), 400

    try:
        user_meta, qas = validate_and_normalize(payload, MAPPING)
    except ValueError as ve:
        try:
            return jsonify(json.loads(str(ve))), 400
        except Exception:
            return jsonify({"error": str(ve)}), 400

    if str(payload.get("normalize_only", "")).lower() in ("1", "true", "yes"):
        return jsonify({
            "status": "ok",
            "request_id": user_meta["request_id"],
            "result_key": user_meta["result_key"],
            "normalized": [
                {"key": qa["key"], "question": qa["question_text"], "answer": qa["answer_text"]} for qa in qas
            ]
        })

    xml_body = to_backend_xml(user_meta, qas)

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
            {"key": qa["key"], "question": qa["question_text"], "answer": qa["answer_text"]} for qa in qas
        ],
        "backend": backend_result
    })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
