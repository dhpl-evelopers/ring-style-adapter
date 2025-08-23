import os
import json
import time
import uuid
import logging
from typing import Dict, Any, List, Tuple, Optional

from flask import Flask, request, jsonify, g
from werkzeug.middleware.proxy_fix import ProxyFix
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from xml.etree.ElementTree import Element, SubElement, tostring

# ==================== Config ====================
def _getenv(name: str, default: Optional[str] = None, required: bool = False) -> str:
    v = os.getenv(name, default)
    if required and not (v and str(v).strip()):
        raise RuntimeError(f"Missing required env var: {name}")
    return (v or "").strip()

BACKEND_BASE_URL   = _getenv("BACKEND_BASE_URL", required=True).rstrip("/")
CREATE_PATH        = _getenv("BACKEND_CREATE_PATH", "/createRequest")
FETCH_PATH         = _getenv("BACKEND_FETCH_PATH", "/fetchResponse")
BACKEND_TIMEOUT_S  = int(_getenv("BACKEND_TIMEOUT_SEC", "25"))
MAPPING_PATH       = _getenv("MAPPING_PATH", "/home/site/wwwroot/mapping.config.json")
API_KEY_REQUIRED   = _getenv("API_KEY_REQUIRED", "false").lower() == "true"
MAX_CONTENT_LENGTH = int(_getenv("MAX_CONTENT_LENGTH", str(512 * 1024)))
LOG_LEVEL          = _getenv("LOG_LEVEL", "INFO").upper()

# ==================== App / Logging ====================
app = Flask(__name__)
app.config["JSON_SORT_KEYS"] = False
app.config["MAX_CONTENT_LENGTH"] = MAX_CONTENT_LENGTH
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_port=1, x_prefix=1)  # type: ignore

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("adapter")

# ==================== HTTP Session (retries) ====================
def _session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=0.4,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "POST"]),
        raise_on_status=False,
    )
    ad = HTTPAdapter(max_retries=retries, pool_connections=20, pool_maxsize=50)
    s.mount("http://", ad)
    s.mount("https://", ad)
    return s

HTTP = _session()

# ==================== Mapping ====================
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

    def resolve_q_key(self, incoming: str) -> Optional[str]:
        if not incoming: return None
        k = incoming.strip()
        if k in self.questions: return k
        return self.label_to_key.get(k.lower())

    def normalize_answer(self, q_key: str, answer: str) -> str:
        answer = (answer or "").strip()
        if not answer: return answer
        norm = self.normalized_options.get(q_key)
        if not norm: return answer
        return norm.get(answer.lower(), answer)

def _load_mapping(path: str) -> Optional[Mapping]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        if not isinstance(raw, dict) or "questions" not in raw:
            raise ValueError("mapping must include 'questions'")
        return Mapping(raw)
    except Exception:
        logger.exception("Failed to load mapping from %s", path)
        return None

MAPPING = _load_mapping(MAPPING_PATH)

# ==================== Helpers ====================
def _require_api_key(headers: Dict[str, str]) -> Optional[str]:
    if not API_KEY_REQUIRED: return None
    key = headers.get("x-api-key") or headers.get("X-API-Key")
    if not key: return "Missing API key header 'x-api-key'."
    return None

def _validate(payload: Dict[str, Any], mapping: Mapping) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    user = {
        "full_name":    payload.get("full_name") or payload.get("name") or "",
        "email":        payload.get("email") or "",
        "phone_number": payload.get("phone_number") or "",
        "birth_date":   payload.get("birth_date") or payload.get("dob") or payload.get("date") or "",
        "request_id":   payload.get("request_id") or payload.get("id") or "",
        "result_key":   payload.get("result_key") or "",
        "test_mode":    payload.get("test_mode") or "live",
    }

    raw_qas = payload.get("questionAnswers") or payload.get("question_answers") or []
    if not isinstance(raw_qas, list):
        raise ValueError(json.dumps({"error": "questionAnswers must be a list"}))

    normalized: List[Dict[str, Any]] = []
    seen = set()
    for item in raw_qas:
        if not isinstance(item, dict): continue
        label = item.get("question") or item.get("id") or item.get("key") or ""
        ans   = item.get("answer") or ""
        q_key = mapping.resolve_q_key(str(label))
        if not q_key:
            if mapping.allow_unknown: continue
            raise ValueError(json.dumps({"error": "Unknown question", "question_received": label}))
        meta = mapping.questions[q_key]
        normalized.append({
            "key": q_key,
            "question_text": meta.get("canonical_label") or q_key,
            "answer_text": mapping.normalize_answer(q_key, str(ans))
        })
        seen.add(q_key)

    # example conditional rule
    purchasing = next((x for x in normalized if x["key"] == "q1_purchasing_for"), None)
    if purchasing and purchasing["answer_text"].strip().lower() == "others" and "q1b_relation" not in seen:
        raise ValueError(json.dumps({"error": "Mandatory question missing", "missing_keys": ["q1b_relation"]}))

    missing = [k for k in mapping.must_have_keys if k not in seen]
    if missing:
        raise ValueError(json.dumps({"error": "Mandatory questions missing", "missing_keys": missing}))

    order = {k: i for i, k in enumerate(mapping.must_have_keys)}
    normalized.sort(key=lambda x: order.get(x["key"], 9999))
    return user, normalized

def _xml_superset(user: Dict[str, Any], qas: List[Dict[str, Any]]) -> str:
    """
    Build a superset XML that satisfies multiple backend parsers:
    - both snake_case and TitleCase for fields
    - all common date tag aliases
    - multiple QA container spellings
    """
    req = Element("Request")

    # IDs / person meta — duplicate in multiple casings/aliases
    for tag in ("request_id", "RequestId", "RequestID"):
        SubElement(req, tag).text = user.get("request_id", "")
    for tag in ("result_key", "ResultKey"):
        SubElement(req, tag).text = user.get("result_key", "")
    for tag in ("full_name", "FullName"):
        SubElement(req, tag).text = user.get("full_name", "")
    for tag in ("email", "Email"):
        SubElement(req, tag).text = user.get("email", "")
    for tag in ("phone_number", "PhoneNumber", "contact", "Contact"):
        SubElement(req, tag).text = user.get("phone_number", "")

    # dates: include all variants some backends look for
    date_val = user.get("birth_date", "")
    for tag in ("date_of_birth", "DateOfBirth", "dob", "DOB", "date", "Date"):
        SubElement(req, tag).text = date_val

    # QA containers: snake_case, TitleCase, and camelCase
    containers = [
        ("question_answers", "qa", "question", "answer"),
        ("QuestionAnswers", "QA", "Question", "Answer"),
        ("questionAnswers", "qa", "question", "answer"),
    ]
    for cont_name, qa_tag, q_tag, a_tag in containers:
        cont = SubElement(req, cont_name)
        for qa in qas:
            qa_el = SubElement(cont, qa_tag)
            SubElement(qa_el, q_tag).text = qa["question_text"]
            SubElement(qa_el, a_tag).text = qa["answer_text"]

    return tostring(req, encoding="unicode")

def _call_backend(xml_body: str, cid: str) -> Dict[str, Any]:
    create_url = f"{BACKEND_BASE_URL}{CREATE_PATH}"
    headers = {"Content-Type": "application/xml", "Accept": "application/json"}

    logger.info("POST %s cid=%s", create_url, cid)
    logger.debug("XML cid=%s payload=%s", cid, xml_body)

    resp = HTTP.post(create_url, data=xml_body.encode("utf-8"), headers=headers, timeout=BACKEND_TIMEOUT_S)
    if resp.status_code >= 400:
        raise RuntimeError(f"Backend createRequest failed: {resp.status_code} {resp.text}")

    try:
        create_json = resp.json()
    except Exception:
        # if backend returns XML or text, pass it back
        return {"backend_raw": resp.text}

    # common id keys
    response_id = (create_json.get("response_id") or
                   create_json.get("ResponseId") or
                   create_json.get("id") or
                   create_json.get("Id"))

    if not response_id:
        # some backends return final payload immediately
        return {"backend_create": create_json}

    # Poll fetch endpoint: try multiple param names
    fetch_url = f"{BACKEND_BASE_URL}{FETCH_PATH}"
    deadline = time.time() + BACKEND_TIMEOUT_S
    last = None

    param_variants = [
        {"response_id": response_id},
        {"responseId": response_id},
        {"id": response_id},
    ]

    while time.time() < deadline:
        for params in param_variants:
            r = HTTP.get(fetch_url, params=params, headers={"Accept": "application/json"}, timeout=BACKEND_TIMEOUT_S)
            if r.status_code >= 400:
                last = {"status_code": r.status_code, "body": r.text}
                continue
            try:
                data = r.json()
            except Exception as e:
                raise RuntimeError(f"Error parsing fetchResponse: {e}")

            last = data
            status = str(data.get("status") or data.get("Status") or "").lower()
            if status in ("done", "completed", "ok", "success"):
                return {"backend_final": data}
        time.sleep(0.7)

    return {"backend_fetch_timeout": True, "response_id": response_id, "last": last}

def _json_error(status: int, code: str, message: str, details: Optional[Dict[str, Any]] = None):
    payload = {"status": "error", "error": {"code": code, "message": message}, "correlation_id": g.get("cid")}
    if details: payload["details"] = details
    return jsonify(payload), status

# ==================== Hooks ====================
@app.before_request
def _before():
    g.cid = request.headers.get("x-request-id") or str(uuid.uuid4())
    if request.endpoint == "adapter":
        if not request.content_type or "application/json" not in request.content_type.lower():
            return _json_error(415, "unsupported_media_type", "Content-Type must be application/json")

@app.after_request
def _after(resp):
    resp.headers["X-Request-Id"] = g.get("cid", "")
    resp.headers["X-Content-Type-Options"] = "nosniff"
    resp.headers["X-Frame-Options"] = "DENY"
    return resp

# ==================== Routes ====================
@app.get("/health")
def health():
    return jsonify({"status": "ok", "service": "adapter"})

@app.get("/ready")
def ready():
    if MAPPING is None:
        return _json_error(503, "not_ready", "Mapping not loaded")
    return jsonify({"status": "ok"})

@app.post("/adapter")
def adapter():
    # gate
    if API_KEY_REQUIRED:
        err = _require_api_key(request.headers)
        if err: return _json_error(401, "unauthorized", err)
    if MAPPING is None:
        return _json_error(503, "not_ready", "Mapping not loaded")

    # parse JSON
    try:
        payload = request.get_json(force=True, silent=False)
        if not isinstance(payload, dict):
            return _json_error(400, "bad_request", "Body must be a JSON object")
    except Exception as e:
        return _json_error(400, "bad_json", f"Invalid JSON: {e}")

    # normalize UI -> canonical Q/As
    try:
        user, qas = _validate(payload, MAPPING)
    except ValueError as ve:
        try:
            return jsonify({"status": "error", "error": json.loads(str(ve)), "correlation_id": g.cid}), 400
        except Exception:
            return _json_error(400, "validation_error", str(ve))

    # optional normalize-only path (no backend call)
    if str(payload.get("normalize_only", "")).lower() in ("1", "true", "yes"):
        return jsonify({
            "status": "ok",
            "request_id": user["request_id"],
            "result_key": user["result_key"],
            "normalized": [{"key": qa["key"], "question": qa["question_text"], "answer": qa["answer_text"]} for qa in qas],
            "correlation_id": g.cid,
        })

    # build superset XML and call backend
    xml_body = _xml_superset(user, qas)
    try:
        backend_result = _call_backend(xml_body, g.cid)
    except Exception as e:
        logger.exception("Backend call failed cid=%s", g.cid)
        return _json_error(502, "backend_error", "Upstream backend call failed",
                           {"details": str(e), "xml": xml_body})

    return jsonify({
        "status": "ok",
        "request_id": user["request_id"],
        "result_key": user["result_key"],
        "normalized": [{"key": qa["key"], "question": qa["question_text"], "answer": qa["answer_text"]} for qa in qas],
        "backend": backend_result,
        "correlation_id": g.cid,
    })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
