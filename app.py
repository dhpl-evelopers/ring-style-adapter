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
CREATE_JSON_PATH    = _getenv("BACKEND_CREATE_JSON_PATH", "/createRequestJSON")  # fallback path
FETCH_PATH          = _getenv("BACKEND_FETCH_PATH", "/fetchResponse")
BACKEND_TIMEOUT_S   = int(_getenv("BACKEND_TIMEOUT_SEC", "25"))
MAPPING_PATH        = _getenv("MAPPING_PATH", "/home/site/wwwroot/mapping.config.json")
API_KEY_REQUIRED    = _getenv("API_KEY_REQUIRED", "false").lower() == "true"
MAX_CONTENT_LENGTH  = int(_getenv("MAX_CONTENT_LENGTH", str(512 * 1024)))
LOG_LEVEL           = _getenv("LOG_LEVEL", "INFO").upper()
LOG_XML_ALWAYS      = _getenv("LOG_XML_ALWAYS", "false").lower() == "true"
ACCEPT_PARTIAL      = _getenv("ACCEPT_PARTIAL", "true").lower() == "true"  # don't 400 on missing keys

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
    """Accept many UI shapes and return (question_text, answer_text)."""
    if not isinstance(item, dict):
        return "", ""

    q = _pick_first_truthy(
        item.get("question"), item.get("q"), item.get("label"), item.get("title"),
        item.get("text"), item.get("id"), item.get("key"),
    )

    a = _pick_first_truthy(
        item.get("answer"), item.get("value"), item.get("option"),
        item.get("selected"), item.get("choice"), item.get("val"),
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

    return _flex_str(q), _flex_str(a)

# ==================== Free-form normalization ====================

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

    if q_key == "q3_profession": return text

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

    if q_key == "q12_drive": return text

    if q_key == "q13_last_minute_plans":
        if has("dislike", "hate", "prefer planning", "plan ahead"):return "You dislike last-minute plans & prefer planning ahead"
        if has("spontaneous", "go with the flow", "last minute"):  return "You enjoy spontaneous last-minute plans"

    return text

# ==================== Validation (flex / partial) ====================

def _validate(payload: Dict[str, Any], mapping: Mapping) -> Tuple[Dict[str, Any], List[Dict[str, Any]], Dict[str, Any]]:
    user = {
        "full_name":    payload.get("full_name") or payload.get("name") or "",
        "email":        payload.get("email") or "",
        "phone_number": payload.get("phone_number") or payload.get("contact") or "",
        "birth_date":   payload.get("birth_date") or payload.get("dob") or payload.get("date") or "",
        "request_id":   payload.get("request_id") or payload.get("id") or "",
        "result_key":   payload.get("result_key") or "",  # may be auto generated later
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
            q_key = mapping.must_have_keys[idx]

        if not q_key:
            if mapping.allow_unknown or ACCEPT_PARTIAL:
                continue
            raise ValueError(json.dumps({"error": "Unknown question", "question_received": q_text_in or f'index_{idx}'}))

        meta = mapping.questions.get(q_key, {"canonical_label": q_key})
        freeform = _normalize_freeform(q_key, a_text_in)
        normalized.append({
            "key": q_key,
            "question_text": meta.get("canonical_label") or q_key,
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

# ==================== XML builder (unique key + placeholders) ====================

def _gen_result_key(seed: Optional[str] = None) -> str:
    base = (seed or time.strftime("%Y%m%d-%H%M%S")).strip()
    return f"rk-{base}-{uuid.uuid4().hex[:6]}"

def _nonempty(val: Optional[str], fallback: str) -> str:
    v = (val or "").strip()
    return v if v else fallback

def _xml_superset(user: Dict[str, Any], qas: List[Dict[str, Any]]) -> Tuple[str, str]:
    """
    Build XML expected by backend AND return the (unique) result_key we used.
    Backend reads:
      <full_name>, <email>, <phone_number>, <result_key>, <date>, <questionAnswers>
    """
    incoming = (user.get("result_key") or user.get("request_id") or "").strip()
    result_key = _gen_result_key(incoming)  # ALWAYS unique to avoid DB duplicates

    full_name    = _nonempty(user.get("full_name") or user.get("name"), "Unknown User")
    email        = _nonempty(user.get("email"), "unknown@example.invalid")
    phone_number = _nonempty(user.get("phone_number") or user.get("contact"), "N/A")
    birth_date   = _nonempty(user.get("birth_date") or user.get("dob") or user.get("date"), "1970-01-01")

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

    return tostring(req, encoding="unicode"), result_key

# ==================== Backend calling (XML → JSON fallback) ====================

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
        for k in ("response_id","ResponseId","responseId","id","Id","code","Code","requestId","request_id"):
            val = create_json.get(k)
            if val and str(val).strip():
                return str(val).strip()
    return None

def _as_json_backend_payload(effective_result_key: str, user: Dict[str, Any], qas: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Shape body for /createRequestJSON per your backend."""
    questions = [qa.get("question_text", "") or "" for qa in qas]
    answers   = [qa.get("answer_text", "")   or "" for qa in qas]
    return {
        "request_id":   effective_result_key,
        "email":        (user.get("email") or "unknown@example.invalid"),
        "name":         (user.get("full_name") or user.get("name") or "Unknown User"),
        "phone_number": (user.get("phone_number") or user.get("contact") or "N/A"),
        "birth_date":   (user.get("birth_date") or user.get("dob") or user.get("date") or "1970-01-01"),
        "questions":    questions,
        "answers":      answers,
    }

def _call_backend(xml_body: str, cid: str, *, effective_result_key: str, user: Dict[str, Any], qas: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    1) Try XML /createRequest (current path)
    2) On 4xx/5xx -> Try JSON /createRequestJSON with body your backend expects
    """
    create_xml_url  = f"{BACKEND_BASE_URL}{CREATE_PATH}"
    create_json_url = f"{BACKEND_BASE_URL}{CREATE_JSON_PATH}"
    headers_xml  = {"Content-Type": "application/xml", "Accept": "application/json, */*"}
    headers_json = {"Content-Type": "application/json", "Accept": "application/json, */*"}

    # -------- 1) XML first --------
    try:
        resp = HTTP.post(create_xml_url, data=xml_body.encode("utf-8"), headers=headers_xml, timeout=BACKEND_TIMEOUT_S)
    except Exception as e:
        xml_first_try = {"transport_error": str(e)}
        resp = None
    else:
        xml_first_try = {"status_code": resp.status_code}
        if resp.status_code < 400:
            try:
                create_json = resp.json()
            except Exception:
                return {"backend_raw": resp.text, "polling_with": None, "path": "xml_first_try"}

            def _is_final(d: Dict[str, Any]) -> bool:
                if "data" in d or "result" in d:
                    return True
                status = str(d.get("status") or d.get("Status") or "").lower()
                if status in ("done","completed","ok","success","ready","finished"):
                    return True
                if d.get("done") is True or d.get("ready") is True or d.get("is_ready") is True:
                    return True
                return False

            if _is_final(create_json):
                return {"backend_final": create_json, "polling_with": None, "path": "xml_first_try"}

            rid = _extract_response_id(create_json, resp.headers)
            if not rid:
                return {"backend_create": create_json, "polling_with": None, "path": "xml_first_try"}

            # Poll by response_id
            fetch_url = f"{BACKEND_BASE_URL}{FETCH_PATH}"
            deadline = time.time() + BACKEND_TIMEOUT_S
            last: Any = None
            while time.time() < deadline:
                r = HTTP.get(fetch_url, params={"response_id": rid}, headers={"Accept": "application/json, */*"}, timeout=BACKEND_TIMEOUT_S)
                if r.status_code == 202:
                    time.sleep(_get_retry_after(r, None)); continue
                if r.status_code >= 400:
                    last = {"status_code": r.status_code, "body": r.text}; time.sleep(0.7); continue
                try:
                    data = r.json()
                except Exception:
                    return {"backend_raw": r.text, "polling_with": "response_id", "fetch_target": {"mode": "id", "id": rid, "id_hint": "response_id"}, "path": "xml_first_try"}
                status = str(data.get("status") or data.get("Status") or "").lower()
                if "data" in data or "result" in data or status in ("done","completed","ok","success","ready","finished") or data.get("done") is True:
                    return {"backend_final": data, "polling_with": "response_id", "fetch_target": {"mode": "id", "id": rid, "id_hint": "response_id"}, "path": "xml_first_try"}
                time.sleep(_get_retry_after(r, data))
            return {"backend_fetch_timeout": True, "last": last, "fetch_target": {"mode": "id", "id": rid, "id_hint": "response_id"}, "polling_with": "response_id", "path": "xml_first_try"}
        else:
            xml_first_try["detail"] = resp.text  # surface backend error

    # -------- 2) JSON fallback --------
    json_body = _as_json_backend_payload(effective_result_key, user, qas)
    try:
        rj = HTTP.post(create_json_url, json=json_body, headers=headers_json, timeout=BACKEND_TIMEOUT_S)
    except Exception as e:
        return {
            "xml_first_try": xml_first_try,
            "json_fallback": {"transport_error": str(e), "body_sent": json_body},
            "polling_with": None,
            "path": "json_fallback"
        }

    if rj.status_code >= 400:
        return {
            "xml_first_try": xml_first_try,
            "json_fallback": {"status_code": rj.status_code, "detail": rj.text, "body_sent": json_body},
            "polling_with": None,
            "path": "json_fallback"
        }

    try:
        cj = rj.json()
    except Exception:
        return {"backend_raw": rj.text, "polling_with": None, "path": "json_fallback"}

    def _is_final2(d: Dict[str, Any]) -> bool:
        if "data" in d or "result" in d:
            return True
        status = str(d.get("status") or d.get("Status") or "").lower()
        if status in ("done","completed","ok","success","ready","finished"):
            return True
        if d.get("done") is True or d.get("ready") is True or d.get("is_ready") is True:
            return True
        return False

    if _is_final2(cj):
        return {"backend_final": cj, "polling_with": None, "path": "json_fallback"}

    rid = _extract_response_id(cj, rj.headers)
    if not rid:
        return {"backend_create": cj, "polling_with": None, "path": "json_fallback"}

    # Poll by response_id (fallback path)
    fetch_url = f"{BACKEND_BASE_URL}{FETCH_PATH}"
    deadline = time.time() + BACKEND_TIMEOUT_S
    last: Any = None
    while time.time() < deadline:
        rr = HTTP.get(fetch_url, params={"response_id": rid}, headers={"Accept": "application/json, */*"}, timeout=BACKEND_TIMEOUT_S)
        if rr.status_code == 202:
            time.sleep(_get_retry_after(rr, None)); continue
        if rr.status_code >= 400:
            last = {"status_code": rr.status_code, "body": rr.text}; time.sleep(0.7); continue
        try:
            data = rr.json()
        except Exception:
            return {"backend_raw": rr.text, "polling_with": "response_id", "fetch_target": {"mode": "id", "id": rid, "id_hint": "response_id"}, "path": "json_fallback"}
        st = str(data.get("status") or data.get("Status") or "").lower()
        if "data" in data or "result" in data or st in ("done","completed","ok","success","ready","finished") or data.get("done") is True:
            return {"backend_final": data, "polling_with": "response_id", "fetch_target": {"mode": "id", "id": rid, "id_hint": "response_id"}, "path": "json_fallback"}
        time.sleep(_get_retry_after(rr, data))

    return {"backend_fetch_timeout": True, "last": last, "fetch_target": {"mode": "id", "id": rid, "id_hint": "response_id"}, "polling_with": "response_id", "path": "json_fallback"}

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
        user, qas, validation = _validate(payload, MAPPING)
    except ValueError as ve:
        try:
            return jsonify({"status": "error", "error": json.loads(str(ve)), "correlation_id": g.cid}), 400
        except Exception:
            return _json_error(400, "validation_error", str(ve))

    # Build XML (and ensure a result_key exists & is unique)
    xml_body, effective_result_key = _xml_superset(user, qas)

    # Debug-only: return normalized without calling backend
    if str(payload.get("normalize_only", "")).lower() in ("1", "true", "yes"):
        out = {
            "status": "ok",
            "request_id": user.get("request_id", ""),
            "result_key": effective_result_key,
            "normalized": [{"key": qa["key"], "question": qa["question_text"], "answer": qa["answer_text"]} for qa in qas],
            "validation": validation,
            "correlation_id": g.cid,
        }
        if LOG_XML_ALWAYS:
            out["xml"] = xml_body
        return jsonify(out)

    try:
        backend_result = _call_backend(xml_body, g.cid, effective_result_key=effective_result_key, user=user, qas=qas)
    except Exception as e:
        logger.exception("Backend call failed cid=%s", g.cid)
        body = {"details": str(e), "xml": xml_body} if LOG_XML_ALWAYS else {"details": str(e)}
        return _json_error(502, "backend_error", "Upstream backend call failed", body)

    result_payload = {
        "status": "ok",
        "request_id": user.get("request_id", ""),
        "result_key": effective_result_key,   # always present now
        "normalized": [{"key": qa["key"], "question": qa["question_text"], "answer": qa["answer_text"]} for qa in qas],
        "validation": validation,
        "backend": backend_result,
        "correlation_id": g.cid,
    }

    if isinstance(result_payload.get("backend"), dict):
        bt = result_payload["backend"]
        if isinstance(bt.get("fetch_target"), dict):
            bt["fetch_target"]["id_hint"] = "response_id"
        if "polling_with" in bt and bt["polling_with"]:
            bt["polling_with"] = "response_id"

    return jsonify(result_payload)

# ==================== Main ====================

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
