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
from urllib.parse import urljoin
from xml.etree.ElementTree import Element, SubElement, tostring

# ==================== Config ====================

def _getenv(name: str, default: Optional[str] = None, required: bool = False) -> str:
    v = os.getenv(name, default)
    if required and not (v and str(v).strip()):
        raise RuntimeError(f"Missing required env var: {name}")
    return (v or "").strip()

BACKEND_BASE_URL    = _getenv("BACKEND_BASE_URL", required=True).rstrip("/")
CREATE_PATH         = _getenv("BACKEND_CREATE_PATH", "/createRequest")
FETCH_PATH          = _getenv("BACKEND_FETCH_PATH", "/fetchResponse")
BACKEND_TIMEOUT_S   = int(_getenv("BACKEND_TIMEOUT_SEC", "25"))
MAPPING_PATH        = _getenv("MAPPING_PATH", "/home/site/wwwroot/mapping.config.json")
API_KEY_REQUIRED    = _getenv("API_KEY_REQUIRED", "false").lower() == "true"
MAX_CONTENT_LENGTH  = int(_getenv("MAX_CONTENT_LENGTH", str(512 * 1024)))
LOG_LEVEL           = _getenv("LOG_LEVEL", "INFO").upper()
LOG_XML_ALWAYS      = _getenv("LOG_XML_ALWAYS", "false").lower() == "true"

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
        if not incoming:
            return None
        k = incoming.strip()
        if k in self.questions:
            return k
        return self.label_to_key.get(k.lower())

    def normalize_answer(self, q_key: str, answer: str) -> str:
        answer = (answer or "").strip()
        if not answer:
            return answer
        norm = self.normalized_options.get(q_key)
        if not norm:
            return answer
        return norm.get(answer.lower(), answer)

def _load_mapping(path: str) -> Optional[Mapping]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        if not isinstance(raw, dict) or "questions" not in raw:
            raise ValueError("mapping must include 'questions'")
        logger.info("Loaded mapping from %s", path)
        return Mapping(raw)
    except Exception:
        logger.exception("Failed to load mapping from %s", path)
        return None

MAPPING = _load_mapping(MAPPING_PATH)

# ==================== Helpers ====================

def _require_api_key(headers: Dict[str, str]) -> Optional[str]:
    if not API_KEY_REQUIRED:
        return None
    key = headers.get("x-api-key") or headers.get("X-API-Key")
    if not key:
        return "Missing API key header 'x-api-key'."
    return None

def _validate(payload: Dict[str, Any], mapping: Mapping) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    user = {
        "full_name":    payload.get("full_name") or payload.get("name") or "",
        "email":        payload.get("email") or "",
        "phone_number": payload.get("phone_number") or payload.get("contact") or "",
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
        if not isinstance(item, dict):
            continue
        label = item.get("question") or item.get("id") or item.get("key") or ""
        ans   = item.get("answer") or ""
        q_key = mapping.resolve_q_key(str(label))
        if not q_key:
            if mapping.allow_unknown:
                continue
            raise ValueError(json.dumps({"error": "Unknown question", "question_received": label}))
        meta = mapping.questions[q_key]
        normalized.append({
            "key": q_key,
            "question_text": meta.get("canonical_label") or q_key,
            "answer_text": mapping.normalize_answer(q_key, str(ans))
        })
        seen.add(q_key)

    # Conditional example: require relation when purchasing for Others
    purchasing = next((x for x in normalized if x["key"] == "q1_purchasing_for"), None)
    if purchasing and purchasing["answer_text"].strip().lower() == "others" and "q1b_relation" not in seen:
        raise ValueError(json.dumps({"error": "Mandatory question missing", "missing_keys": ["q1b_relation"]}))

    missing = [k for k in mapping.must_have_keys if k not in seen]
    if missing:
        raise ValueError(json.dumps({"error": "Mandatory questions missing", "missing_keys": missing}))

    order = {k: i for i, k in enumerate(mapping.must_have_keys)}
    normalized.sort(key=lambda x: order.get(x["key"], 9999))
    return user, normalized

def _flatten_qas_to_text(qas: List[Dict[str, Any]]) -> str:
    return "\n".join(f"{qa['question_text']} :: {qa['answer_text']}" for qa in qas)

def _xml_superset(user: Dict[str, Any], qas: List[Dict[str, Any]]) -> str:
    """
    Build XML that is compatible with various backends:
    - Provide multiple casings/aliases for person fields
    - Include QA as containers, plus flat text mirrors and JSON mirrors
    - Also set container .text to the flat string so parsers that expect text under <questionAnswers> work
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

    # Flat text & JSON mirrors
    flat = _flatten_qas_to_text(qas)
    for tag in ("qna_text", "QNA", "qna", "Qna",
                "question_answers_text", "QuestionAnswersText", "questionAnswersText"):
        SubElement(req, tag).text = flat

    qa_json = json.dumps(
        [{"question": qa["question_text"], "answer": qa["answer_text"]} for qa in qas],
        ensure_ascii=False
    )
    for tag in ("question_answers_json", "QuestionAnswersJson"):
        SubElement(req, tag).text = qa_json

    # QA containers: snake_case, TitleCase, camelCase.
    # Also set the container .text to the flat string for parsers doing .find('.//questionAnswers').text
    containers = [
        ("question_answers", "qa", "question", "answer"),
        ("QuestionAnswers", "QA", "Question", "Answer"),
        ("questionAnswers", "qa", "question", "answer"),
    ]
    for cont_name, qa_tag, q_tag, a_tag in containers:
        cont = SubElement(req, cont_name)
        cont.text = flat  # important: satisfy backends reading .text from the container
        for qa in qas:
            qa_el = SubElement(cont, qa_tag)
            SubElement(qa_el, q_tag).text = qa["question_text"]
            SubElement(qa_el, a_tag).text = qa["answer_text"]

    return tostring(req, encoding="unicode")

def _get_retry_after(resp: requests.Response, body_json: Optional[Dict[str, Any]]) -> float:
    """Figure out how long to wait before the next poll."""
    ra = resp.headers.get("Retry-After")
    if ra:
        try:
            sec = int(ra.strip())
            return max(0.2, min(sec, 5.0))
        except Exception:
            pass
    if body_json:
        for key in ("retryAfter", "retry_after", "pollIntervalSec", "poll_interval_sec"):
            if key in body_json:
                try:
                    return max(0.2, min(float(body_json[key]), 5.0))
                except Exception:
                    pass
        for key in ("pollInMs", "poll_in_ms"):
            if key in body_json:
                try:
                    ms = float(body_json[key])
                    return max(0.2, min(ms / 1000.0, 5.0))
                except Exception:
                    pass
    return 0.7

def _build_fetch_target(create_json: Dict[str, Any], headers: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Understand whatever the backend returned:
    - An indicator code (ID),
    - A direct polling URL (in JSON or Location header), or
    - HAL/links-ish shapes.
    """
    # common id/code keys
    id_keys = ["response_id", "ResponseId", "responseId", "id", "Id", "code", "Code", "requestCode", "RequestCode", "requestId", "request_id"]
    for k in id_keys:
        if k in create_json and str(create_json[k]).strip():
            return {"mode": "id", "id": str(create_json[k]).strip()}

    # direct link keys
    link_keys = ["fetch_url", "fetchUrl", "url", "URL", "link", "Link", "result_url", "resultUrl"]
    for k in link_keys:
        if k in create_json and str(create_json[k]).strip():
            url = str(create_json[k]).strip()
            if url.startswith("/"):
                url = urljoin(BACKEND_BASE_URL + "/", url.lstrip("/"))
            return {"mode": "link", "url": url}

    # HAL-style
    links = create_json.get("_links") or create_json.get("links")
    if isinstance(links, dict):
        for cand in ("result", "self", "poll", "status"):
            node = links.get(cand)
            if isinstance(node, dict) and node.get("href"):
                url = str(node["href"]).strip()
                if url.startswith("/"):
                    url = urljoin(BACKEND_BASE_URL + "/", url.lstrip("/"))
                return {"mode": "link", "url": url}

    # Location header from 202/201
    loc = headers.get("Location") or headers.get("location")
    if loc and str(loc).strip():
        url = str(loc).strip()
        if url.startswith("/"):
            url = urljoin(BACKEND_BASE_URL + "/", url.lstrip("/"))
        return {"mode": "link", "url": url}

    return None

def _call_backend(xml_body: str, cid: str) -> Dict[str, Any]:
    create_url = f"{BACKEND_BASE_URL}{CREATE_PATH}"
    headers = {"Content-Type": "application/xml", "Accept": "application/json, */*"}

    logger.info("POST %s cid=%s", create_url, cid)
    logger.info("xml_string :  %s", xml_body)  # always log for visibility (matches previous logs)
    logger.debug("XML cid=%s payload=%s", cid, xml_body)

    # 1) CREATE
    resp = HTTP.post(create_url, data=xml_body.encode("utf-8"), headers=headers, timeout=BACKEND_TIMEOUT_S)

    if resp.status_code >= 400:
        raise RuntimeError(f"Backend createRequest failed: {resp.status_code} {resp.text}")

    # Try JSON; if not JSON, just pass raw back
    create_json: Optional[Dict[str, Any]] = None
    try:
        create_json = resp.json()
    except Exception:
        # If create already returns a final, non-JSON payload, just hand it back.
        return {"backend_raw": resp.text}

    # If the create response itself looks final, short-circuit
    def _is_final(d: Dict[str, Any]) -> bool:
        status = str(d.get("status") or d.get("Status") or "").lower()
        if status in ("done", "completed", "ok", "success", "ready", "finished"):
            return True
        if d.get("done") is True or d.get("ready") is True or d.get("is_ready") is True:
            return True
        if "result" in d or "data" in d:
            return True
        return False

    if _is_final(create_json):
        return {"backend_final": create_json}

    # 2) Figure out how to poll: by ID or direct link/Location
    fetch_target = _build_fetch_target(create_json, resp.headers)
    if not fetch_target:
        # No ID or link => just return what we got
        return {"backend_create": create_json}

    deadline = time.time() + BACKEND_TIMEOUT_S
    last = None

    while time.time() < deadline:
        try:
            if fetch_target["mode"] == "id":
                fetch_url = f"{BACKEND_BASE_URL}{FETCH_PATH}"
                rid = fetch_target["id"]
                # Try many likely param names
                param_variants = [
                    {"response_id": rid},
                    {"responseId": rid},
                    {"id": rid},
                    {"code": rid},
                    {"requestId": rid},
                    {"request_id": rid},
                ]
                for params in param_variants:
                    r = HTTP.get(fetch_url, params=params, headers={"Accept": "application/json, */*"}, timeout=BACKEND_TIMEOUT_S)
                    if r.status_code == 202:
                        time.sleep(_get_retry_after(r, None))
                        continue
                    if r.status_code >= 400:
                        last = {"status_code": r.status_code, "body": r.text}
                        continue
                    try:
                        data = r.json()
                    except Exception:
                        # Non-JSON final
                        return {"backend_raw": r.text}
                    last = data
                    if _is_final(data):
                        return {"backend_final": data}
                # none worked this loop
                time.sleep(0.7)
            else:
                # Direct polling link
                link = fetch_target["url"]
                r = HTTP.get(link, headers={"Accept": "application/json, */*"}, timeout=BACKEND_TIMEOUT_S)
                if r.status_code == 202:
                    time.sleep(_get_retry_after(r, None))
                    continue
                if r.status_code >= 400:
                    last = {"status_code": r.status_code, "body": r.text}
                    time.sleep(0.7)
                    continue
                try:
                    data = r.json()
                except Exception:
                    return {"backend_raw": r.text}
                last = data
                status = str(data.get("status") or data.get("Status") or "").lower()
                if status in ("done", "completed", "ok", "success", "ready", "finished") or data.get("done") is True:
                    return {"backend_final": data}
                # Not final yet — body-provided retry hints
                time.sleep(_get_retry_after(r, last))
        except Exception as e:
            last = {"exception": str(e)}
            time.sleep(0.7)

    return {"backend_fetch_timeout": True, "last": last, "fetch_target": fetch_target}

def _json_error(status: int, code: str, message: str, details: Optional[Dict[str, Any]] = None):
    payload = {"status": "error", "error": {"code": code, "message": message}, "correlation_id": g.get("cid")}
    if details:
        payload["details"] = details
    return jsonify(payload), status

# ==================== Hooks ====================

@app.before_request
def _before():
    g.cid = request.headers.get("x-request-id") or str(uuid.uuid4())
    if request.endpoint == "adapter":
        ctype = (request.content_type or "").lower()
        if "application/json" not in ctype:
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
    # API key gate (optional)
    if API_KEY_REQUIRED:
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

    # Normalize UI -> canonical Q/As
    try:
        user, qas = _validate(payload, MAPPING)
    except ValueError as ve:
        try:
            return jsonify({"status": "error", "error": json.loads(str(ve)), "correlation_id": g.cid}), 400
        except Exception:
            return _json_error(400, "validation_error", str(ve))

    # Optional normalize-only path (no backend call)
    if str(payload.get("normalize_only", "")).lower() in ("1", "true", "yes"):
        return jsonify({
            "status": "ok",
            "request_id": user["request_id"],
            "result_key": user["result_key"],
            "normalized": [{"key": qa["key"], "question": qa["question_text"], "answer": qa["answer_text"]} for qa in qas],
            "correlation_id": g.cid,
        })

    # Build XML and call backend
    xml_body = _xml_superset(user, qas)

    # If the caller wants to see the XML for debugging, include it in the response (header X-Debug-XML: 1)
    want_xml_echo = (request.headers.get("X-Debug-XML", "0").lower() in ("1", "true", "yes"))

    try:
        backend_result = _call_backend(xml_body, g.cid)
    except Exception as e:
        logger.exception("Backend call failed cid=%s", g.cid)
        body = {"details": str(e), "xml": xml_body} if want_xml_echo or LOG_XML_ALWAYS else {"details": str(e)}
        return _json_error(502, "backend_error", "Upstream backend call failed", body)

    result_payload = {
        "status": "ok",
        "request_id": user["request_id"],
        "result_key": user["result_key"],
        "normalized": [{"key": qa["key"], "question": qa["question_text"], "answer": qa["answer_text"]} for qa in qas],
        "backend": backend_result,
        "correlation_id": g.cid,
    }
    if want_xml_echo or LOG_XML_ALWAYS:
        result_payload["xml"] = xml_body

    return jsonify(result_payload)

# ==================== Main ====================

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
