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

BACKEND_BASE_URL    = _getenv("BACKEND_BASE_URL", required=True).rstrip("/")
CREATE_PATH         = _getenv("BACKEND_CREATE_PATH", "/createRequest")
FETCH_PATH          = _getenv("BACKEND_FETCH_PATH", "/fetchResponse")
BACKEND_TIMEOUT_S   = int(_getenv("BACKEND_TIMEOUT_SEC", "25"))
MAPPING_PATH        = _getenv("MAPPING_PATH", "/home/site/wwwroot/mapping.config.json")
API_KEY_REQUIRED    = _getenv("API_KEY_REQUIRED", "false").lower() == "true"
MAX_CONTENT_LENGTH  = int(_getenv("MAX_CONTENT_LENGTH", str(512 * 1024)))
LOG_LEVEL           = _getenv("LOG_LEVEL", "INFO").upper()
LOG_XML_ALWAYS      = _getenv("LOG_XML_ALWAYS", "false").lower() == "true"

# —— Option-1: Only poll with response_id (never result_key/request_id)
PREFER_RESULT_KEY_WHEN_NO_ID = False

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
        with open(path, "r", encoding="utf-8-sig") as f:
            raw = json.load(f)
        if not isinstance(raw, dict) or "questions" not in raw:
            raise ValueError("mapping must include 'questions'")
        logger.info("Loaded mapping from %s", path)
        return Mapping(raw)
    except Exception:
        logger.exception("Failed to load mapping from %s", path)
        return None

MAPPING = _load_mapping(MAPPING_PATH)

# ==================== Flex Q&A helpers ====================

def _flex_str(x: Any) -> str:
    if x is None:
        return ""
    if isinstance(x, bool):
        return "1" if x else "0"
    return str(x)

def _pick_first_truthy(*vals):
    for v in vals:
        if v is None:
            continue
        if isinstance(v, str) and v.strip() == "":
            continue
        return v
    return None

def _extract_question_and_answer(item: Dict[str, Any]) -> Tuple[str, str]:
    """
    Accept multiple UI shapes and return (question_text, answer_text).
    """
    if not isinstance(item, dict):
        return "", ""

    # QUESTION CANDIDATES
    q = _pick_first_truthy(
        item.get("question"),
        item.get("q"),
        item.get("label"),
        item.get("title"),
        item.get("text"),
        item.get("id"),
        item.get("key"),
    )

    # ANSWER CANDIDATES (flat)
    a = _pick_first_truthy(
        item.get("answer"),
        item.get("value"),
        item.get("option"),
        item.get("selected"),
        item.get("choice"),
        item.get("val"),
    )

    # NESTED: selectedOption.value / selected_option.value / option.value
    if a is None:
        sel = _pick_first_truthy(
            (item.get("selectedOption") or {}).get("value") if isinstance(item.get("selectedOption"), dict) else None,
            (item.get("selected_option") or {}).get("value") if isinstance(item.get("selected_option"), dict) else None,
            (item.get("option") or {}).get("value") if isinstance(item.get("option"), dict) else None,
        )
        if sel is not None:
            a = sel

    # LISTS: answers[], options[], choices[]
    if a is None:
        for key in ("answers", "options", "choices"):
            arr = item.get(key)
            if isinstance(arr, list) and arr:
                picked: List[str] = []
                for el in arr:
                    if isinstance(el, dict):
                        if el.get("selected") is True:
                            picked.append(_flex_str(_pick_first_truthy(el.get("value"), el.get("label"), el.get("text"))))
                    else:
                        picked.append(_flex_str(el))
                if not picked:
                    for el in arr:
                        if isinstance(el, dict):
                            cand = _pick_first_truthy(el.get("value"), el.get("label"), el.get("text"))
                            if cand:
                                picked.append(_flex_str(cand))
                                break
                        else:
                            if el:
                                picked.append(_flex_str(el))
                                break
                if picked:
                    a = ", ".join([p for p in picked if p != ""])
                    break

    q_text = _flex_str(q)
    a_text = _flex_str(a)
    return q_text, a_text

# ==================== Helpers ====================

def _require_api_key(headers: Dict[str, str]) -> Optional[str]:
    if not API_KEY_REQUIRED:
        return None
    key = headers.get("x-api-key") or headers.get("X-API-Key")
    if not key:
        return "Missing API key header 'x-api-key'."
    return None

def _ensure_list_qas(raw_qas: Any) -> List[Any]:
    """
    Accept list or stringified JSON or dict with 'qas'/'items' etc.
    """
    if isinstance(raw_qas, list):
        return raw_qas
    if isinstance(raw_qas, dict):
        for k in ("qas", "items", "data"):
            v = raw_qas.get(k)
            if isinstance(v, list):
                return v
    if isinstance(raw_qas, str):
        s = raw_qas.strip()
        if s:
            try:
                val = json.loads(s)
                if isinstance(val, list):
                    return val
            except Exception:
                pass
    return []

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

    raw_qas_input = payload.get("questionAnswers") or payload.get("question_answers") or []
    raw_qas = _ensure_list_qas(raw_qas_input)

    if not isinstance(raw_qas, list):
        raise ValueError(json.dumps({"error": "questionAnswers must be a list or JSON string"}))

    normalized: List[Dict[str, Any]] = []
    seen = set()

    for item in raw_qas:
        # Flexible extraction
        q_text_in, a_text_in = _extract_question_and_answer(item)
        if not q_text_in:
            # fallback to legacy fields if extractor couldn't find a label
            q_text_in = _flex_str((isinstance(item, dict) and (item.get("question") or item.get("id") or item.get("key"))) or "")

        q_key = mapping.resolve_q_key(q_text_in)
        if not q_key:
            if mapping.allow_unknown:
                continue
            raise ValueError(json.dumps({"error": "Unknown question", "question_received": q_text_in}))

        meta = mapping.questions[q_key]
        normalized.append({
            "key": q_key,
            "question_text": meta.get("canonical_label") or q_key,
            "answer_text": mapping.normalize_answer(q_key, a_text_in),
        })
        seen.add(q_key)

    # Conditional required question example
    purchasing = next((x for x in normalized if x["key"] == "q1_purchasing_for"), None)
    if purchasing and purchasing["answer_text"].strip().lower() == "others" and "q1b_relation" not in seen:
        raise ValueError(json.dumps({"error": "Mandatory question missing", "missing_keys": ["q1b_relation"]}))

    missing = [k for k in mapping.must_have_keys if k not in seen]
    if missing:
        raise ValueError(json.dumps({"error": "Mandatory questions missing", "missing_keys": missing}))

    order = {k: i for i, k in enumerate(mapping.must_have_keys)}
    normalized.sort(key=lambda x: order.get(x["key"], 9999))
    return user, normalized

# ---------- XML builder (matches backend expectations) ----------
def _xml_superset(user: Dict[str, Any], qas: List[Dict[str, Any]]) -> str:
    """
    Backend expects:
      <full_name>, <email>, <phone_number>, <result_key>, <date>,
      <questionAnswers>  // stringified JSON: [{question, selectedOption:{value}}]
    """
    result_key = (user.get("result_key") or user.get("request_id") or str(uuid.uuid4())).strip()

    qa_payload = [
        {
            "question": qa.get("question_text", "") or "",
            "selectedOption": {"value": qa.get("answer_text", "") or ""}
        }
        for qa in qas
    ]
    qa_json_str = json.dumps(qa_payload, ensure_ascii=False)

    req = Element("Request")
    SubElement(req, "full_name").text    = user.get("full_name", "") or user.get("name", "") or ""
    SubElement(req, "email").text        = user.get("email", "") or ""
    SubElement(req, "phone_number").text = user.get("phone_number", "") or user.get("contact", "") or ""
    SubElement(req, "result_key").text   = result_key
    SubElement(req, "date").text         = user.get("birth_date", "") or user.get("dob", "") or user.get("date", "") or ""
    SubElement(req, "questionAnswers").text = qa_json_str

    return tostring(req, encoding="unicode")

def _get_retry_after(resp: requests.Response, body_json: Optional[Dict[str, Any]]) -> float:
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

def _extract_response_id(create_json: Optional[Dict[str, Any]], headers: Dict[str, Any]) -> Optional[str]:
    if create_json:
        id_keys = [
            "response_id", "ResponseId", "responseId",
            "id", "Id",
            "code", "Code",
            "requestId", "request_id",
        ]
        for k in id_keys:
            val = create_json.get(k)
            if val and str(val).strip():
                return str(val).strip()
    loc = headers.get("Location") or headers.get("location")
    if loc and str(loc).strip():
        return None
    return None

def _call_backend(xml_body: str, cid: str, user: Dict[str, Any]) -> Dict[str, Any]:
    create_url = f"{BACKEND_BASE_URL}{CREATE_PATH}"
    headers = {"Content-Type": "application/xml", "Accept": "application/json, */*"}

    logger.info("POST %s cid=%s", create_url, cid)
    if LOG_XML_ALWAYS:
        logger.info("XML cid=%s payload=%s", cid, xml_body)

    # CREATE
    resp = HTTP.post(create_url, data=xml_body.encode("utf-8"), headers=headers, timeout=BACKEND_TIMEOUT_S)
    if resp.status_code >= 400:
        raise RuntimeError(f"Backend createRequest failed: {resp.status_code} {resp.text}")

    create_json: Optional[Dict[str, Any]] = None
    try:
        create_json = resp.json()
    except Exception:
        return {"backend_raw": resp.text, "polling_with": None}

    def _is_final(d: Dict[str, Any]) -> bool:
        if "data" in d or "result" in d:
            return True
        status = str(d.get("status") or d.get("Status") or "").lower()
        if status in ("done", "completed", "ok", "success", "ready", "finished"):
            return True
        if d.get("done") is True or d.get("ready") is True or d.get("is_ready") is True:
            return True
        return False

    if _is_final(create_json):
        return {"backend_final": create_json, "polling_with": None}

    # Poll only if a response_id is explicitly returned
    response_id = _extract_response_id(create_json, resp.headers)
    if not response_id:
        return {"backend_create": create_json, "polling_with": None}

    fetch_url = f"{BACKEND_BASE_URL}{FETCH_PATH}"
    deadline = time.time() + BACKEND_TIMEOUT_S
    last: Any = None

    while time.time() < deadline:
        try:
            r = HTTP.get(
                fetch_url,
                params={"response_id": response_id},
                headers={"Accept": "application/json, */*"},
                timeout=BACKEND_TIMEOUT_S
            )

            if r.status_code == 202:
                time.sleep(_get_retry_after(r, None))
                continue

            ctype = (r.headers.get("Content-Type") or "").lower()
            if r.status_code == 200 and "application/json" not in ctype:
                body_text = r.text.strip() if isinstance(r.text, str) else ""
                if not body_text or "list index out of range" in body_text.lower():
                    time.sleep(0.7)
                    continue
                return {
                    "backend_raw": body_text,
                    "polling_with": "response_id",
                    "fetch_target": {"mode": "id", "id": response_id, "id_hint": "response_id"}
                }

            if r.status_code >= 400:
                last = {"status_code": r.status_code, "body": r.text, "params_tried": {"response_id": response_id}}
                time.sleep(0.7)
                continue

            try:
                data = r.json()
            except Exception:
                return {
                    "backend_raw": r.text,
                    "polling_with": "response_id",
                    "fetch_target": {"mode": "id", "id": response_id, "id_hint": "response_id"}
                }

            last = data
            status = str(data.get("status") or data.get("Status") or "").lower()
            if status in ("done", "completed", "ok", "success", "ready", "finished") or data.get("done") is True:
                return {
                    "backend_final": data,
                    "polling_with": "response_id",
                    "fetch_target": {"mode": "id", "id": response_id, "id_hint": "response_id"}
                }

            time.sleep(_get_retry_after(r, data))

        except Exception as e:
            last = {"exception": str(e)}
            time.sleep(0.7)

    return {
        "backend_fetch_timeout": True,
        "last": last,
        "fetch_target": {"mode": "id", "id": response_id, "id_hint": "response_id"},
        "polling_with": "response_id"
    }

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
    return jsonify({"status": "ok", "service": "adapter", "backend": BACKEND_BASE_URL})

@app.get("/ready")
def ready():
    if MAPPING is None:
        return _json_error(503, "not_ready", "Mapping not loaded")
    return jsonify({"status": "ok"})

@app.post("/adapter")
def adapter():
    if API_KEY_REQUIRED:
        err = _require_api_key(request.headers)
        if err:
            return _json_error(401, "unauthorized", err)
    if MAPPING is None:
        return _json_error(503, "not_ready", "Mapping not loaded")

    try:
        payload = request.get_json(force=True, silent=False)
        if not isinstance(payload, dict):
            return _json_error(400, "bad_request", "Body must be a JSON object")
    except Exception as e:
        return _json_error(400, "bad_json", f"Invalid JSON: {e}")

    try:
        user, qas = _validate(payload, MAPPING)
    except ValueError as ve:
        try:
            return jsonify({"status": "error", "error": json.loads(str(ve)), "correlation_id": g.cid}), 400
        except Exception:
            return _json_error(400, "validation_error", str(ve))

    # Debug-only: return normalized without calling backend
    if str(payload.get("normalize_only", "")).lower() in ("1", "true", "yes"):
        return jsonify({
            "status": "ok",
            "request_id": user["request_id"],
            "result_key": user["result_key"],
            "normalized": [{"key": qa["key"], "question": qa["question_text"], "answer": qa["answer_text"]} for qa in qas],
            "correlation_id": g.cid,
        })

    xml_body = _xml_superset(user, qas)
    if LOG_XML_ALWAYS:
        logger.info("Built XML cid=%s: %s", g.cid, xml_body)

    try:
        backend_result = _call_backend(xml_body, g.cid, user)
    except Exception as e:
        logger.exception("Backend call failed cid=%s", g.cid)
        body = {"details": str(e), "xml": xml_body} if LOG_XML_ALWAYS else {"details": str(e)}
        return _json_error(502, "backend_error", "Upstream backend call failed", body)

    result_payload = {
        "status": "ok",
        "request_id": user["request_id"],
        "result_key": user["result_key"],  # echo only
        "normalized": [{"key": qa["key"], "question": qa["question_text"], "answer": qa["answer_text"]} for qa in qas],
        "backend": backend_result,
        "correlation_id": g.cid,
    }

    if isinstance(result_payload.get("backend"), dict):
        bt = result_payload["backend"]
        if isinstance(bt.get("fetch_target"), dict):
            bt["fetch_target"]["id_hint"] = "response_id"
        if "polling_with" in bt:
            bt["polling_with"] = "response_id" if bt.get("polling_with") else None

    return jsonify(result_payload)

# ==================== Main ====================

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
