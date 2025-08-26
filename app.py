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
CREATE_JSON_PATH    = _getenv("BACKEND_CREATE_JSON_PATH", "/createRequestJSON")
FETCH_PATH          = _getenv("BACKEND_FETCH_PATH", "/fetchResponse")
BACKEND_TIMEOUT_S   = int(_getenv("BACKEND_TIMEOUT_SEC", "25"))
MAPPING_PATH        = _getenv("MAPPING_PATH", "/home/site/wwwroot/mapping.config.json")
API_KEY_REQUIRED    = _getenv("API_KEY_REQUIRED", "false").lower() == "true"
MAX_CONTENT_LENGTH  = int(_getenv("MAX_CONTENT_LENGTH", str(512 * 1024)))
LOG_LEVEL           = _getenv("LOG_LEVEL", "INFO").upper()
LOG_XML_ALWAYS      = _getenv("LOG_XML_ALWAYS", "false").lower() == "true"
ACCEPT_PARTIAL      = _getenv("ACCEPT_PARTIAL", "true").lower() == "true"
ALLOW_UNKNOWN       = _getenv("ALLOW_UNKNOWN", "true").lower() == "true"
MOCK_BACKEND        = _getenv("MOCK_BACKEND", "false").lower() == "true"

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
        self.allow_unknown = bool(raw.get("allow_unknown_questions", ALLOW_UNKNOWN))
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
                norm: Dict[str, str] = {}
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

    def canonical_label(self, q_key: str) -> str:
        meta = self.questions.get(q_key, {})
        return meta.get("canonical_label") or q_key

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

# ==================== Flex helpers ====================

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

def _ensure_list_qas(raw_qas: Any) -> List[Any]:
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

def _extract_question_and_answer(item: Dict[str, Any]) -> Tuple[str, str]:
    if not isinstance(item, dict):
        return "", ""
    q = _pick_first_truthy(
        item.get("question"), item.get("q"), item.get("label"), item.get("title"),
        item.get("text"), item.get("id"), item.get("key")
    )
    a = _pick_first_truthy(
        item.get("answer"), item.get("value"), item.get("option"),
        item.get("selected"), item.get("choice"), item.get("val")
    )
    if a is None:
        sel = _pick_first_truthy(
            (item.get("selectedOption") or {}).get("value") if isinstance(item.get("selectedOption"), dict) else None,
            (item.get("selected_option") or {}).get("value") if isinstance(item.get("selected_option"), dict) else None,
            (item.get("option") or {}).get("value") if isinstance(item.get("option"), dict) else None,
        )
        if sel is not None:
            a = sel
    if a is None:
        for key in ("answers", "options", "choices"):
            arr = item.get(key)
            if isinstance(arr, list) and arr:
                picked: List[str] = []
                for el in arr:
                    if isinstance(el, dict) and el.get("selected") is True:
                        picked.append(_flex_str(_pick_first_truthy(el.get("value"), el.get("label"), el.get("text"))))
                    elif not isinstance(el, dict):
                        picked.append(_flex_str(el))
                if not picked:
                    for el in arr:
                        if isinstance(el, dict):
                            cand = _pick_first_truthy(el.get("value"), el.get("label"), el.get("text"))
                            if cand:
                                picked.append(_flex_str(cand)); break
                        elif el:
                            picked.append(_flex_str(el)); break
                if picked:
                    a = ", ".join([p for p in picked if p != ""])
                    break
    return _flex_str(q), _flex_str(a)

# ==================== Free-form normalization (lightweight) ====================

def _normalize_freeform(q_key: str, text: str) -> str:
    if not text:
        return text
    s = text.strip().lower()
    def has(*words): return any(w in s for w in words)

    if q_key == "q1_purchasing_for":
        if has("myself", "for me", "self", "me only"): return "Self"
        if has("wife", "husband", "mother", "father", "friend", "gf", "bf", "partner", "fiancé", "fiance"): return "Others"

    if q_key == "q2_gender":
        if has("female", "woman", "girl", "she", "her"): return "Female"
        if has("male", "man", "boy", "he", "him"): return "Male"

    if q_key == "q4_occasion":
        if has("wedding", "marriage", "bride", "groom", "shaadi"): return "Wedding"
        if has("engagement", "ring ceremony", "roka", "sagai"):   return "Engagement"
        if has("daily", "office", "work", "casual"):               return "Daily wear"
        if has("party", "birthday", "festival", "anniversary"):    return "Party"

    if q_key == "q5_purpose":
        if has("daily", "routine", "office", "work"):              return "Daily wear"
        if has("gift"):                                            return "Gifting"
        if has("wedding"):                                         return "Wedding"
        if has("engagement"):                                      return "Engagement"
        if has("party", "occasion", "event"):                      return "Occasional wear"

    if q_key == "q6_day":
        if has("busy", "hectic", "packed", "lot of activities"):  return "Busy With a lot of Activities"
        if has("relaxed", "easy", "laid back"):                    return "Relaxed & Easy-going"

    if q_key == "q7_weekend":
        if has("social", "party", "go out", "friends", "outing"): return "Going out & socialising"
        if has("home", "stay in", "me-time", "read", "netflix"):  return "Staying in & relaxing"

    if q_key == "q8_work_dress":
        if has("designer", "chic", "stylish", "fashion"):          return "Stylish & Chic: Designer Clothing"
        if has("formal", "business", "smart"):                     return "Classic Formal"
        if has("casual", "comfortable", "comfy"):                  return "Smart Casuals"

    if q_key == "q9_line":
        if has("alone", "by myself", "independent"):               return "You prefer to be on your own"
        if has("chat", "talk", "friends", "social"):               return "You like to chat with people around"

    if q_key == "q10_painting":
        if has("meaning", "story", "symbol", "origin", "heritage"):return "You value the meaning of the art & origin"
        if has("color", "colour", "vibrant", "aesthetic", "look"): return "You value the colors & aesthetics"

    if q_key == "q11_emergency":
        if has("arrange care", "care for my mother", "both"):      return "You will arrange care for your mother & attend the meeting"
        if has("stay", "skip", "miss") and has("mother", "mom"):   return "You will stay with your mother"
        if has("meeting") and has("attend"):                       return "You will attend the meeting"

    # pass-through for free text questions
    return text

# ==================== Validation / Normalization pipeline ====================

def _gen_result_key(seed: Optional[str] = None) -> str:
    base = (seed or time.strftime("%Y%m%d-%H%M%S")).strip()
    return f"rk-{base}-{uuid.uuid4().hex[:6]}"

def _ensure_result_key(user: Dict[str, Any]) -> str:
    incoming = (user.get("result_key") or user.get("request_id") or "").strip()
    return incoming or _gen_result_key()

def _validate_and_normalize(payload: Dict[str, Any], mapping: Mapping) -> Tuple[Dict[str, Any], List[Dict[str, Any]], Dict[str, Any]]:
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

    for idx, item in enumerate(raw_qas):
        q_text_in, a_text_in = _extract_question_and_answer(item)
        q_key = mapping.resolve_q_key(q_text_in) if q_text_in else None
        if not q_key and idx < len(mapping.must_have_keys):
            q_key = mapping.must_have_keys[idx]  # positional fallback

        if not q_key:
            if mapping.allow_unknown or ACCEPT_PARTIAL:
                continue
            raise ValueError(json.dumps({"error": "Unknown question", "question_received": q_text_in or f'index_{idx}'}))

        meta_label = mapping.canonical_label(q_key)
        freeform = _normalize_freeform(q_key, a_text_in)
        normalized.append({
            "key": q_key,
            "question_text": meta_label,
            "answer_text": mapping.normalize_answer(q_key, freeform),
        })
        seen.add(q_key)

    missing = [k for k in mapping.must_have_keys if k not in seen]
    validation = {"missing_keys": missing}
    if missing and not ACCEPT_PARTIAL:
        raise ValueError(json.dumps({"error": "Mandatory questions missing", "missing_keys": missing}))

    order = {k: i for i, k in enumerate(mapping.must_have_keys)}
    normalized.sort(key=lambda x: order.get(x["key"], 9999))
    return user, normalized, validation

# ==================== Backend formats (XML / JSON) ====================

def _to_backend_json(result_key: str, user: Dict[str, Any], qas: List[Dict[str, Any]]) -> Dict[str, Any]:
    questions = [qa.get("question_text", "") or "" for qa in qas]
    answers   = [qa.get("answer_text", "")   or "" for qa in qas]
    return {
        "request_id":   result_key,
        "email":        (user.get("email") or "unknown@example.invalid"),
        "name":         (user.get("full_name") or user.get("name") or "Unknown User"),
        "phone_number": (user.get("phone_number") or user.get("contact") or "N/A"),
        "birth_date":   (user.get("birth_date") or user.get("dob") or user.get("date") or "1970-01-01"),
        "questions":    questions,
        "answers":      answers,
    }

def _to_backend_xml(result_key: str, user: Dict[str, Any], qas: List[Dict[str, Any]]) -> str:
    full_name    = (user.get("full_name") or user.get("name") or "Unknown User").strip()
    email        = (user.get("email") or "unknown@example.invalid").strip()
    phone_number = (user.get("phone_number") or user.get("contact") or "N/A").strip()
    birth_date   = (user.get("birth_date") or user.get("dob") or user.get("date") or "1970-01-01").strip()

    qa_payload = [
        {"question": qa.get("question_text", "") or "",
         "selectedOption": {"value": qa.get("answer_text", "") or ""}}
        for qa in qas
    ]
    qa_json_str = json.dumps(qa_payload, ensure_ascii=False)

    req = Element("Request")
    SubElement(req, "full_name").text    = full_name
    SubElement(req, "email").text        = email
    SubElement(req, "phone_number").text = phone_number
    SubElement(req, "result_key").text   = result_key
    SubElement(req, "date").text         = birth_date
    SubElement(req, "questionAnswers").text = qa_json_str
    return tostring(req, encoding="unicode")

# ==================== Backend calls (XML → JSON fallback) ====================

def _get_retry_after(resp: requests.Response, body_json: Optional[Dict[str, Any]]) -> float:
    ra = resp.headers.get("Retry-After")
    if ra:
        try:
            sec = int(ra.strip()); return max(0.2, min(sec, 5.0))
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
                    ms = float(body_json[key]); return max(0.2, min(ms/1000.0, 5.0))
                except Exception:
                    pass
    return 0.7

def _extract_response_id(create_json: Optional[Dict[str, Any]]) -> Optional[str]:
    if create_json:
        for k in ("response_id","ResponseId","responseId","id","Id","code","Code","requestId","request_id"):
            val = create_json.get(k)
            if val and str(val).strip():
                return str(val).strip()
    return None

def _post_xml_then_json_fallback(backend_xml: str, backend_json: Dict[str, Any]) -> Dict[str, Any]:
    # XML first
    try:
        r1 = HTTP.post(f"{BACKEND_BASE_URL}{CREATE_PATH}", data=backend_xml.encode("utf-8"),
                       headers={"Content-Type": "application/xml", "Accept": "application/json, */*"},
                       timeout=BACKEND_TIMEOUT_S)
    except Exception as e:
        xml_attempt = {"transport_error": str(e)}
        r1 = None
    else:
        xml_attempt = {"status_code": r1.status_code, "detail": r1.text[:500]}
        if r1.status_code < 400:
            try:
                body = r1.json()
            except Exception:
                return {"backend_raw": r1.text, "path": "xml_first_try"}
            if ("data" in body) or (str(body.get("status", "")).lower() in ("ok","success","done","completed","ready")):
                return {"backend_final": body, "path": "xml_first_try"}
            rid = _extract_response_id(body)
            if not rid:
                return {"backend_create": body, "path": "xml_first_try"}
            deadline = time.time() + BACKEND_TIMEOUT_S
            while time.time() < deadline:
                rp = HTTP.get(f"{BACKEND_BASE_URL}{FETCH_PATH}", params={"response_id": rid},
                              headers={"Accept": "application/json, */*"}, timeout=BACKEND_TIMEOUT_S)
                if rp.status_code == 202:
                    time.sleep(_get_retry_after(rp, None)); continue
                if rp.status_code >= 400:
                    time.sleep(0.7); continue
                try:
                    pdata = rp.json()
                except Exception:
                    return {"backend_raw": rp.text, "path": "xml_first_try"}
                st = str(pdata.get("status") or "").lower()
                if ("data" in pdata) or (st in ("ok","success","done","completed","ready")):
                    return {"backend_final": pdata, "path": "xml_first_try"}
                time.sleep(_get_retry_after(rp, pdata))

    # JSON fallback
    try:
        r2 = HTTP.post(f"{BACKEND_BASE_URL}{CREATE_JSON_PATH}", json=backend_json,
                       headers={"Content-Type": "application/json", "Accept": "application/json, */*"},
                       timeout=BACKEND_TIMEOUT_S)
    except Exception as e:
        return {"xml_first_try": xml_attempt, "json_fallback": {"transport_error": str(e), "body_sent": backend_json}, "path": "json_fallback"}

    if r2.status_code >= 400:
        return {"xml_first_try": xml_attempt, "json_fallback": {"status_code": r2.status_code, "detail": r2.text, "body_sent": backend_json}, "path": "json_fallback"}

    try:
        body2 = r2.json()
    except Exception:
        return {"backend_raw": r2.text, "path": "json_fallback"}

    if ("data" in body2) or (str(body2.get("status","")).lower() in ("ok","success","done","completed","ready")):
        return {"backend_final": body2, "path": "json_fallback"}

    rid2 = _extract_response_id(body2)
    if not rid2:
        return {"backend_create": body2, "path": "json_fallback"}

    deadline = time.time() + BACKEND_TIMEOUT_S
    while time.time() < deadline:
        rp2 = HTTP.get(f"{BACKEND_BASE_URL}{FETCH_PATH}", params={"response_id": rid2},
                       headers={"Accept": "application/json, */*"}, timeout=BACKEND_TIMEOUT_S)
        if rp2.status_code == 202:
            time.sleep(_get_retry_after(rp2, None)); continue
        if rp2.status_code >= 400:
            time.sleep(0.7); continue
        try:
            pdata2 = rp2.json()
        except Exception:
            return {"backend_raw": rp2.text, "path": "json_fallback"}
        st2 = str(pdata2.get("status") or "").lower()
        if ("data" in pdata2) or (st2 in ("ok","success","done","completed","ready")):
            return {"backend_final": pdata2, "path": "json_fallback"}
        time.sleep(_get_retry_after(rp2, pdata2))

    return {"backend_fetch_timeout": True, "path": "json_fallback"}

# ==================== Misc helpers ====================

def _require_api_key(headers: Dict[str, str]) -> Optional[str]:
    if not API_KEY_REQUIRED:
        return None
    key = headers.get("x-api-key") or headers.get("X-API-Key")
    if not key:
        return "Missing API key header 'x-api-key'."
    return None

def _json_error(status: int, code: str, message: str, details: Optional[Dict[str, Any]] = None):
    payload = {"status": "error", "error": {"code": code, "message": message}, "correlation_id": g.get("cid")}
    if details:
        payload["details"] = details
    return jsonify(payload), status

# ==================== Hooks ====================

@app.before_request
def _before():
    g.cid = request.headers.get("x-request-id") or str(uuid.uuid4())
    if request.endpoint in ("adapter", "transform"):
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
    return jsonify({"status": "ok", "service": "adapter", "backend": BACKEND_BASE_URL, "mock_backend": MOCK_BACKEND})

@app.get("/ready")
def ready():
    if MAPPING is None:
        return _json_error(503, "not_ready", "Mapping not loaded")
    return jsonify({"status": "ok"})

# ---------- Transform-only endpoint ----------
@app.post("/adapter/transform")
def transform():
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
        user, qas, validation = _validate_and_normalize(payload, MAPPING)
    except ValueError as ve:
        try:
            return jsonify({"status": "error", "error": json.loads(str(ve)), "correlation_id": g.cid}), 400
        except Exception:
            return _json_error(400, "validation_error", str(ve))

    result_key = _ensure_result_key(user)
    backend_json = _to_backend_json(result_key, user, qas)
    backend_xml  = _to_backend_xml(result_key, user, qas)

    return jsonify({
        "status": "ok",
        "result_key": result_key,
        "normalized": [{"key": qa["key"], "question": qa["question_text"], "answer": qa["answer_text"]} for qa in qas],
        "validation": validation,
        "backend_json": backend_json,
        "backend_xml": backend_xml,
        "correlation_id": g.cid,
    })

# ---------- Main adapter (backend call, mock toggle, transform_only flag) ----------
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

    # Normalize-only exit
    if str(payload.get("normalize_only", "")).lower() in ("1", "true", "yes"):
        try:
            user, qas, validation = _validate_and_normalize(payload, MAPPING)
        except ValueError as ve:
            try:
                return jsonify({"status": "error", "error": json.loads(str(ve)), "correlation_id": g.cid}), 400
            except Exception:
                return _json_error(400, "validation_error", str(ve))
        rk = _ensure_result_key(user)
        return jsonify({
            "status": "ok",
            "request_id": user.get("request_id", ""),
            "result_key": rk,
            "normalized": [{"key": qa["key"], "question": qa["question_text"], "answer": qa["answer_text"]} for qa in qas],
            "validation": {"missing_keys": validation["missing_keys"]},
            "correlation_id": g.cid,
        })

    # Full flow
    try:
        user, qas, validation = _validate_and_normalize(payload, MAPPING)
    except ValueError as ve:
        try:
            return jsonify({"status": "error", "error": json.loads(str(ve)), "correlation_id": g.cid}), 400
        except Exception:
            return _json_error(400, "validation_error", str(ve))

    result_key = _ensure_result_key(user)
    backend_json = _to_backend_json(result_key, user, qas)
    backend_xml  = _to_backend_xml(result_key, user, qas)

    # Only transform
    if str(request.args.get("transform_only", "")).lower() in ("1", "true", "yes"):
        return jsonify({
            "status": "ok",
            "result_key": result_key,
            "normalized": [{"key": qa["key"], "question": qa["question_text"], "answer": qa["answer_text"]} for qa in qas],
            "validation": validation,
            "backend_json": backend_json,
            "backend_xml": backend_xml,
            "correlation_id": g.cid,
        })

    # Mock backend path (bypass real backend/DB)
    if MOCK_BACKEND:
        return jsonify({
            "status": "ok",
            "request_id": user.get("request_id", ""),
            "result_key": result_key,
            "normalized": [{"key": qa["key"], "question": qa["question_text"], "answer": qa["answer_text"]} for qa in qas],
            "validation": validation,
            "backend": {
                "backend_final": {
                    "status": 200,
                    "data": {
                        "mock": True,
                        "message": "MOCK_BACKEND enabled; DB bypassed",
                        "received_formats": ["xml", "json"],
                    }
                },
                "path": "mock"
            },
            "correlation_id": g.cid,
        })

    # Real backend call (XML first, JSON fallback)
    try:
        backend_result = _post_xml_then_json_fallback(backend_xml, backend_json)
    except Exception as e:
        logger.exception("Backend call failed cid=%s", g.cid)
        body = {"details": str(e)}
        if LOG_XML_ALWAYS:
            body["xml"]  = backend_xml
            body["json"] = backend_json
        return _json_error(502, "backend_error", "Upstream backend call failed", body)

    return jsonify({
        "status": "ok",
        "request_id": user.get("request_id", ""),
        "result_key": result_key,
        "normalized": [{"key": qa["key"], "question": qa["question_text"], "answer": qa["answer_text"]} for qa in qas],
        "validation": validation,
        "backend": backend_result,
        "correlation_id": g.cid,
    })

# ==================== Main ====================

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8000")))

