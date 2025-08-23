import os, json, re

from typing import Dict, Any, Tuple

from flask import Flask, request, jsonify

import requests

# ---------------- ENV ----------------

BASE_URL   = os.getenv("BACKEND_BASE_URL", "").rstrip("/")

CREATE_P   = os.getenv("BACKEND_CREATE_PATH", "/createRequest")

FETCH_P    = os.getenv("BACKEND_FETCH_PATH", "/fetchResponse")

TIMEOUT_S  = int(os.getenv("BACKEND_TIMEOUT_SEC", "30"))

MAP_PATH   = os.getenv("MAPPING_PATH") or os.path.join(os.path.dirname(__file__), "mapping.config.json")

app = Flask(__name__)

# ---------------- mapping ----------------

with open(MAP_PATH, "r", encoding="utf-8") as f:

    MAPPING = json.load(f)

def _norm(s: str) -> str:

    return re.sub(r"\s+", " ", (s or "").strip().lower())

# id/label -> canonical key

_ID2KEY = {

    "q1":"q1_purchasing_for","q1b":"q1b_relation","q2":"q2_gender","q3":"q3_profession",

    "q4":"q4_occasion","q5":"q5_purpose","q6":"q6_day","q7":"q7_weekend","q8":"q8_work_dress",

    "q9":"q9_social_dress","q10":"q10_waiting_line","q11":"q11_artwork",

    "q12":"q12_meeting_mother_unwell","q13":"q13_last_minute_plans"

}

def find_question_key(qid_or_text: str) -> str:

    s = (qid_or_text or "").strip()

    s_norm = _norm(s)

    if s_norm in _ID2KEY:

        return _ID2KEY[s_norm]

    for key, meta in MAPPING["questions"].items():

        labels = [meta.get("canonical_label","")] + meta.get("labels", [])

        if any(_norm(lbl) == s_norm for lbl in labels):

            return key

    return ""

def map_answer_to_backend(key: str, answer_text: str) -> Tuple[str, Any]:

    meta = MAPPING["questions"].get(key, {})

    backend_ids = meta.get("backend_ids", {})

    for opt, opt_id in backend_ids.items():

        if _norm(opt) == _norm(answer_text):

            return ("id", opt_id)

    return ("text", answer_text)

def require_keys(payload: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:

    missing = []

    must = list(MAPPING.get("must_have_questions_keys", []))

    # conditional relation if purchasing for Others

    if payload.get("q1_purchasing_for", {}).get("answer_text", "").lower() == "others":

        if "q1b_relation" not in payload:

            missing.append("q1b_relation")

    for k in must:

        if k not in payload:

            missing.append(k)

    return (not missing, {"missing_keys": missing})

def xml_escape(s: str) -> str:

    return (s or "").replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

def build_backend_xml(meta: Dict[str, Any], normalized: Dict[str, Any]) -> str:

    parts = []

    parts.append("<Request>")

    parts.append(f"<RequestId>{xml_escape(meta.get('request_id',''))}</RequestId>")

    parts.append(f"<ResultKey>{xml_escape(meta.get('result_key',''))}</ResultKey>")

    parts.append(f"<FullName>{xml_escape(meta.get('full_name',''))}</FullName>")

    parts.append(f"<Email>{xml_escape(meta.get('email',''))}</Email>")

    parts.append(f"<PhoneNumber>{xml_escape(meta.get('phone_number',''))}</PhoneNumber>")

    parts.append(f"<DateOfBirth>{xml_escape(meta.get('birth_date',''))}</DateOfBirth>")

    parts.append("<QuestionAnswers>")

    order = [

      "q1_purchasing_for","q1b_relation","q2_gender","q3_profession","q4_occasion","q5_purpose",

      "q6_day","q7_weekend","q8_work_dress","q9_social_dress",

      "q10_waiting_line","q11_artwork","q12_meeting_mother_unwell","q13_last_minute_plans"

    ]

    for key in order:

        if key not in normalized: 

            continue

        q = MAPPING["questions"][key]["canonical_label"]

        a = normalized[key]

        parts.append("<QA>")

        parts.append(f"<Question>{xml_escape(q)}</Question>")

        if a["kind"] == "id":

            parts.append(f"<SelectedOptionId>{a['answer_id']}</SelectedOptionId>")

        parts.append(f"<Answer>{xml_escape(a.get('answer_text',''))}</Answer>")

        parts.append("</QA>")

    parts.append("</QuestionAnswers>")

    parts.append("</Request>")

    return "".join(parts)

@app.post("/adapter")

def adapter():

    body = request.get_json(force=True, silent=True) or {}

    meta = {

        "full_name":   body.get("full_name") or body.get("name") or "",

        "email":       body.get("email",""),

        "phone_number":body.get("phone_number",""),

        "birth_date":  body.get("birth_date",""),

        "request_id":  body.get("request_id",""),

        "result_key":  body.get("result_key",""),

        "test_mode":   body.get("test_mode","live")

    }

    incoming = body.get("questionAnswers") or body.get("question_answers") or []

    normalized = {}

    for item in incoming:

        qkey = ""

        if "id" in item:

            qkey = find_question_key(str(item["id"]))

        elif "question" in item:

            qkey = find_question_key(str(item["question"]))

        if not qkey:

            if not MAPPING.get("allow_unknown_questions", True):

                continue

            else:

                # unknown questions are ignored (or you can pass-through in XML if needed)

                continue

        answer_text = str(item.get("answer","")).strip()

        kind, mapped = map_answer_to_backend(qkey, answer_text)

        if kind == "id":

            normalized[qkey] = {"kind": "id", "answer_id": mapped, "answer_text": answer_text}

        else:

            normalized[qkey] = {"kind": "text", "answer_text": mapped}

    ok, info = require_keys(normalized)

    if not ok:

        return jsonify({"error":"Mandatory questions missing", **info}), 400

    xml = build_backend_xml(meta, normalized)

    if not BASE_URL:

        return jsonify({"details":"BACKEND_BASE_URL is not configured", "preview_xml": xml}), 500

    create_url = f"{BASE_URL}{CREATE_P}"

    fetch_url  = f"{BASE_URL}{FETCH_P}"

    try:

        # ---------- CREATE ----------

        c_headers = {"Content-Type": "application/xml"}

        c_resp = requests.post(create_url, data=xml.encode("utf-8"), headers=c_headers, timeout=TIMEOUT_S)

        response_id = None

        try:

            cj = c_resp.json()

            # try several common keys; fallback to None

            response_id = cj.get("response_id") or cj.get("id") or cj.get("responseId")

        except Exception:

            pass

        # ---------- FETCH ----------

        # Prefer GET with either response_id or request_id/result_key

        params = {}

        if response_id:

            params["response_id"] = response_id

        else:

            # best-effort generic params

            if meta["request_id"]: params["request_id"] = meta["request_id"]

            if meta["result_key"]: params["result_key"] = meta["result_key"]

        f_status = None

        f_text   = None

        # Try GET first

        try:

            f_resp = requests.get(fetch_url, params=params, timeout=TIMEOUT_S)

            f_status, f_text = f_resp.status_code, f_resp.text

            # If backend doesn’t support GET or returns non-OK, try POST as fallback

            if f_status >= 400:

                raise RuntimeError(f"GET fetch returned {f_status}")

        except Exception:

            try:

                f_resp = requests.post(fetch_url, json=params, timeout=TIMEOUT_S)

                f_status, f_text = f_resp.status_code, f_resp.text

            except Exception as e:

                f_status, f_text = None, f"fetch failed: {e}"

        return jsonify({

            "status": "ok",

            "request_id": meta["request_id"],

            "result_key": meta["result_key"],

            "normalized": normalized,

            "preview_xml": xml,

            "backend": {

                "create_url": create_url,

                "create_status": c_resp.status_code,

                "create_text": c_resp.text[:4000],

                "fetch_url": fetch_url,

                "fetch_params": params,

                "fetch_status": f_status,

                "fetch_text": (f_text or "")[:4000]

            }

        }), 200

    except Exception as e:

        return jsonify({"error":"Backend call failed", "message": str(e), "preview_xml": xml}), 502

@app.get("/healthz")

def health():

    return {"ok": True}
 
