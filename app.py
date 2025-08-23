# app.py
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

# ======================================================================================
# Configuration
# ======================================================================================

def _getenv(name: str, default: Optional[str] = None, required: bool = False) -> str:
    val = os.getenv(name, default)
    if required and not (val and str(val).strip()):
        raise RuntimeError(f"Missing required environment variable: {name}")
    return (val or "").strip()

BACKEND_BASE_URL   = _getenv("BACKEND_BASE_URL", required=True).rstrip("/")
CREATE_PATH        = _getenv("BACKEND_CREATE_PATH", "/createRequest")
FETCH_PATH         = _getenv("BACKEND_FETCH_PATH", "/fetchResponse")
BACKEND_TIMEOUT_S  = int(_getenv("BACKEND_TIMEOUT_SEC", "25"))
MAPPING_PATH       = _getenv("MAPPING_PATH", "/home/site/wwwroot/mapping.json")
API_KEY_REQUIRED   = _getenv("API_KEY_REQUIRED", "false").lower() == "true"
MAX_CONTENT_LENGTH = int(_getenv("MAX_CONTENT_LENGTH", str(512 * 1024)))  # 512 KB cap

# ======================================================================================
# App & logging
# ======================================================================================

app = Flask(__name__)
app.config["JSON_SORT_KEYS"] = False
app.config["MAX_CONTENT_LENGTH"] = MAX_CONTENT_LENGTH

# Honor X-Forwarded-* (Azure/App Service)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_port=1, x_prefix=1)  # type: ignore

# Structured logging (human & machine friendly)
LOG_LEVEL = _getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

logger = logging.getLogger("ring-style-adapter")

# ======================================================================================
# HTTP session with retries
# ======================================================================================

def _make_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=0.4,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "POST"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=20, pool_maxsize=50)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s

HTTP = _make_session()

# ======================================================================================
# Mapping loader
# ======================================================================================

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
                        norm[o.strip().lower()] = o  # lowercase -> canonical option
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
    # Basic sanity checks
    if not isinstance(raw, dict) or "questions" not in raw:
        raise ValueError("mapping.json must contain a 'questions' object")
    return Mapping(raw)

# Load mapping at startup
try:
    MAPPING = load_mapping(MAPPING_PATH)
    logger.info("Loaded mapping from %s", MAPPING_PATH)
except Exception as e:
    logger.exception("Failed to load mapping")
    MAPPING = None  # liveness still OK, readiness will fail

# ======================================================================================
# Utilities
# ======================================================================================

def _require_api_key(headers: Dict[str, str]) -> Optional[str]:
    if not API_KEY_REQUIRED:
        return None
    key = headers.get("x-api-key") or headers.get("X-API-Key")
    if not key:
        return "Missing API key header 'x-api-key'."
    return None

def _validate_payload(payload: Dict[str, Any], mapping: Mapping) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
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
            "answer_text": answer_norm
        })
        seen_keys.add(q_key)

    # Conditional requirement: relation when Others
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

def _xml_for_backend(user_meta: Dict[str, Any], qas: List[Dict[str, Any]]) -> str:
    """Build snake_case XML that the backend expects."""
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

def _backend_call(xml_body: str, req_id: str) -> Dict[str, Any]:
    """Create then poll the backend with retries and timeouts."""
    create_url = f"{BACKEND_BASE_URL}{CREATE_PATH}"
    headers = {"Content-Type": "application/xml", "Accept": "application/json"}

    logger.info("createRequest POST url=%s req_id=%s", create_url, req_id)
    logger.debug("XML payload req_id=%s xml=%s", req_id, xml_body)

    resp = HTTP.post(create_url, data=xml_body.encode("utf-8"), headers=headers, timeout=BACKEND_TIMEOUT_S)
    if resp.status_code >= 400:
        # Surface body to caller for faster debugging
        raise RuntimeError(f"Backend createRequest failed: {resp.status_code} {resp.text}")

    try:
        create_json = resp.json()
    except Exception:
        # Backend might return XML—pass raw
        return {"backend_raw": resp.text}

    response_id = create_json.get("response_id") or create_json.get("ResponseId") or create_json.get("id")
    if not response_id:
        # Some backends return the final payload right away
        return {"backend_create": create_json}

    fetch_url = f"{BACKEND_BASE_URL}{FETCH_PATH}"
    deadline = time.time() + BACKEND_TIMEOUT_S
    last_data: Optional[Dict[str, Any]] = None

    while time.time() < deadline:
        r = HTTP.get(fetch_url, params={"response_id": response_id}, headers={"Accept": "application/json"}, timeout=BACKEND_TIMEOUT_S)
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

def _json_error(status: int, code: str, message: str, extras: Optional[Dict[str, Any]] = None):
    payload = {
        "status": "error",
        "error": {"code": code, "message": message},
        "request_id": getattr(g, "request_id", None),
    }
    if extras:
        payload["details"] = extras
    return jsonify(payload), status

# ======================================================================================
# Request lifecycle
# ======================================================================================

@app.before_request
def _before():
    g.request_id = request.headers.get("x-request-id") or str(uuid.uuid4())
    # Basic content-type guard for JSON endpoints
    if request.endpoint in {"adapter"}:
        if not request.content_type or "application/json" not in request.content_type.lower():
            return _json_error(415, "unsupported_media_type", "Content-Type must be application/json")

@app.after_request
def _after(resp):
    # Security headers (safe defaults)
    resp.headers["X-Content-Type-Options"] = "nosniff"
    resp.headers["X-Frame-Options"] = "DENY"
    resp.headers["Referrer-Policy"] = "no-referrer"
    resp.headers["X-Request-Id"] = getattr(g, "request_id", "")
    return resp

# ======================================================================================
# Routes
# ======================================================================================

@app.get("/health")
def health():
    return jsonify({"status": "ok", "service": "ring-style-adapter"})

@app.get("/")
def root():
    # Lightweight liveness info
    return jsonify({"status": "ok", "mapping_loaded": MAPPING is not None})

@app.get("/ready")
def ready():
    # Readiness: mapping must be loaded
    if MAPPING is None:
        return _json_error(503, "not_ready", "Mapping not loaded")
    return jsonify({"status": "ok"})

@app.post("/adapter")
def adapter():
    # API key (optional)
    err = _require_api_key(request.headers)
    if err:
        return _json_error(401, "unauthorized", err)

    if MAPPING is None:
        return _json_error(503, "not_ready", "Mapping not loaded")

    # Parse JSON
    try:
        payload = request.get_json(force=True, silent=False)
        if not isinstance(payload, dict):
            return _json_error(400, "bad_request", "Body must be a JSON object")
    except Exception as e:
        return _json_error(400, "bad_json", f"Invalid JSON: {e}")

    # Validate & normalize
    try:
        user_meta, qas = _validate_payload(payload, MAPPING)
    except ValueError as ve:
        try:
            # Raise structured errors when provided
            return jsonify({"status": "error", "error": json.loads(str(ve)), "request_id": g.request_id}), 400
        except Exception:
            return _json_error(400, "validation_error", str(ve))

    # Fast path for normalization-only callers
    if str(payload.get("normalize_only", "")).lower() in ("1", "true", "yes"):
        return jsonify({
            "status": "ok",
            "request_id": user_meta["request_id"],
            "result_key": user_meta["result_key"],
            "normalized": [
                {"key": qa["key"], "question": qa["question_text"], "answer": qa["answer_text"]} for qa in qas
            ],
            "correlation_id": g.request_id,
        })

    # Build XML and call backend
    xml_body = _xml_for_backend(user_meta, qas)
    try:
        backend_result = _backend_call(xml_body, req_id=g.request_id)
    except Exception as e:
        logger.exception("Backend call failed req_id=%s", g.request_id)
        # Return the XML to help ops debug (it contains no secrets beyond user-provided fields)
        return _json_error(502, "backend_error", "Upstream backend call failed", {"details": str(e), "xml": xml_body})

    return jsonify({
        "status": "ok",
        "request_id": user_meta["request_id"],
        "result_key": user_meta["result_key"],
        "normalized": [
            {"key": qa["key"], "question": qa["question_text"], "answer": qa["answer_text"]} for qa in qas
        ],
        "backend": backend_result,
        "correlation_id": g.request_id,
    })

# ======================================================================================
# Entrypoint for local dev
# ======================================================================================

if __name__ == "__main__":
    # Local dev server (gunicorn is used in production)
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
