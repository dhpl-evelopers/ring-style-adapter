import os
import json
import traceback
from datetime import datetime
from flask import Flask, request, jsonify
import requests
import xml.etree.ElementTree as ET
app = Flask(__name__)
# ---------- ENV ----------
MAPPING_PATH = os.getenv("MAPPING_PATH", "/home/site/wwwroot/mapping.config.json")
BACKEND_URL  = os.getenv("BACKEND_URL", "").rstrip("/")
BACKEND_PATH = os.getenv("BACKEND_PATH", "/createRequest")
BACKEND_API_KEY = os.getenv("BACKEND_API_KEY", "")  # optional
# ---------- UTIL ----------
def load_mapping():
   with open(MAPPING_PATH, "r", encoding="utf-8") as f:
       return json.load(f)
def as_text(x):
   return (x or "").strip()
def norm_key(s):
   return as_text(s).lower()
def soft_equal(a, b):
   return norm_key(a) == norm_key(b)
def find_question_key(mapping, q_text):
   """
   Given a user question text, find which mapping question key (q1..q13 etc.)
   it belongs to by matching any of the 'labels' entries (case/space-insensitive).
   """
   qn = norm_key(q_text)
   for qkey, qdef in mapping["questions"].items():
       for lab in qdef.get("labels", []):
           if norm_key(lab) == qn:
               return qkey
   return None
def map_answer_text(qdef, ans_text):
   """
   For this use case the backend wants TEXT, not ids.
   We still allow a whitelist of acceptable option labels.
   If an answer doesn't match any listed option, we still pass the text through.
   """
   a = as_text(ans_text)
   options = qdef.get("options", [])
   if not options:
       return a
   # try exact-insensitive match against options
   for opt in options:
       if soft_equal(opt, a):
           return opt
   # if no clean match, pass-through (backend expects text)
   return a
def extract_ui_answers(payload):
   """
   Accepts both:
     1) {"questionAnswers":[{"question":"...","answer":"..."}]}
     2) {"answers":{"Gender":"Male", "Occasion":"Anniversary", ...}}
     3) a flat dict inside payload["answers"] where keys are labels
   Returns list of (question_text, answer_text)
   """
   out = []
   if isinstance(payload.get("questionAnswers"), list):
       for qa in payload["questionAnswers"]:
           q = qa.get("question")
           a = qa.get("answer")
           if q is not None and a is not None:
               out.append((str(q), str(a)))
   elif isinstance(payload.get("answers"), dict):
       for k, v in payload["answers"].items():
           out.append((str(k), str(v)))
   return out
def require_fields(p, fields):
   missing = [f for f in fields if not as_text(p.get(f))]
   return missing
def build_xml(mapping, user_fields, normalized_qas):
   """
   Build XML in the form your backend log shows (Request + User + QuestionAnswers).
   Structure:
<Request>
<RequestID>...</RequestID>
<ResultKey>...</ResultKey>
<FullName>...</FullName>
<Email>...</Email>
<PhoneNumber>...</PhoneNumber>
<DateOfBirth>YYYY-MM-DD</DateOfBirth>
<QuestionAnswers>
<QA>
<Question>text</Question>
<Answer>text</Answer>
</QA>
         ...
</QuestionAnswers>
</Request>
   """
   root = ET.Element("Request")
   def add(tag, val):
       el = ET.SubElement(root, tag)
       el.text = as_text(val)
   add("RequestID", user_fields["request_id"])
   add("ResultKey", user_fields["result_key"])
   add("FullName", user_fields["full_name"])
   add("Email", user_fields["email"])
   add("PhoneNumber", user_fields["phone_number"])
   # normalize DoB to YYYY-MM-DD if possible
   dob = as_text(user_fields["birth_date"])
   dob_out = dob
   if dob:
       for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%m/%d/%Y", "%Y/%m/%d"):
           try:
               dob_out = datetime.strptime(dob, fmt).strftime("%Y-%m-%d")
               break
           except Exception:
               pass
   add("DateOfBirth", dob_out)
   qa_parent = ET.SubElement(root, "QuestionAnswers")
   for item in normalized_qas:
       qa = ET.SubElement(qa_parent, "QA")
       q_el = ET.SubElement(qa, "Question")
       q_el.text = item["question"]
       a_el = ET.SubElement(qa, "Answer")
       a_el.text = item["answer"]
   return ET.tostring(root, encoding="utf-8", method="xml")
def call_backend(xml_bytes):
   if not BACKEND_URL:
       raise RuntimeError("BACKEND_URL is not configured")
   url = f"{BACKEND_URL}{BACKEND_PATH}"
   headers = {
       "Content-Type": "application/xml"
   }
   if BACKEND_API_KEY:
       headers["x-api-key"] = BACKEND_API_KEY
   resp = requests.post(url, data=xml_bytes, headers=headers, timeout=60)
   return {
       "status_code": resp.status_code,
       "headers": dict(resp.headers),
       "body": resp.text
   }
# ---------- ROUTES ----------
@app.get("/healthz")
def health():
   return jsonify({"status": "ok"})
@app.post("/adapter")
def adapter():
   try:
       mapping = load_mapping()
   except Exception as e:
       return jsonify({"error": f"Failed to load mapping: {e}"}), 500
   try:
       payload = request.get_json(force=True, silent=False)
   except Exception:
       return jsonify({"error": "Invalid JSON body"}), 400
   # required user fields
   user_fields = {
       "full_name":  as_text(payload.get("full_name") or payload.get("fullName")),
       "email":      as_text(payload.get("email")),
       "phone_number": as_text(payload.get("phone_number") or payload.get("phoneNumber") or payload.get("mobile") or payload.get("whatsapp")),
       "birth_date": as_text(payload.get("birth_date") or payload.get("dateOfBirth")),
       "request_id": as_text(payload.get("request_id") or payload.get("resultKey") or payload.get("id") or payload.get("responseId") or payload.get("requestId")),
       "result_key": as_text(payload.get("result_key") or payload.get("resultKey")),
   }
   missing = require_fields(user_fields, ["full_name","email","phone_number","birth_date","request_id","result_key"])
   if missing:
       return jsonify({
           "error": "Missing required user fields",
           "missing": missing
       }), 400
   # Gather incoming Q&A from flexible UI
   incoming = extract_ui_answers(payload)
   if not incoming:
       return jsonify({"error": "No Q&A provided. Provide questionAnswers[] or answers{}."}), 400
   # Normalize to mapping
   normalized = []
   seen = set()
   # First pass: direct matches by label
   for q_text, a_text in incoming:
       qkey = find_question_key(mapping, q_text)
       if not qkey:
           # Not a known label, but we may allow passthrough if configuration enables it
           if mapping.get("allow_unknown_questions", True):
               normalized.append({"question": q_text.strip(), "answer": a_text.strip()})
               continue
           else:
               return jsonify({"error": f"No mapping found for question '{q_text}'"}), 400
       qdef = mapping["questions"][qkey]
       answer_out = map_answer_text(qdef, a_text)
       final_question_text = qdef.get("canonical_label") or qdef["labels"][0]
       normalized.append({"question": final_question_text, "answer": answer_out})
       seen.add(qkey)
   # Conditional: if purchaser is Others, ensure relation question appears
   # Detect purchaser answer (Self/Others)
   purchaser_ans = None
   for item in normalized:
       if norm_key(item["question"]) in [norm_key(l) for l in mapping["questions"]["q1_purchasing_for"]["labels"]]:
           purchaser_ans = norm_key(item["answer"])
           break
   if purchaser_ans == "others":
       # ensure relation present
       has_relation = any(
           norm_key(it["question"]) in [norm_key(l) for l in mapping["questions"]["q1b_relation"]["labels"]]
           for it in normalized
       )
       if not has_relation:
           return jsonify({"error": "When 'Others' is selected, 'Relation' is required but missing."}), 400
   # Optional: enforce full set of mandatory questions if you want:
   must_have_keys = mapping.get("must_have_questions_keys", [])
   missing_qs = []
   if must_have_keys:
       present_keys = set()
       for it in normalized:
           # back-map canonical to qkey
           ck = find_question_key(mapping, it["question"]) or ""
           if ck:
               present_keys.add(ck)
       for k in must_have_keys:
           if k not in present_keys:
               missing_qs.append(k)
   if missing_qs:
       return jsonify({"error":"Mandatory questions missing", "missing_keys": missing_qs}), 400
   # Build XML
   xml_bytes = build_xml(mapping, user_fields, normalized)
   # test_mode:
   #   "xml_preview" -> return xml only (no backend call)
   #   "live" (default) -> call backend and return live response
   test_mode = (payload.get("test_mode") or "live").lower().strip()
   result = {
       "normalized": normalized,
       "xml": xml_bytes.decode("utf-8")
   }
   if test_mode == "xml_preview":
       return jsonify(result), 200
   # Call backend
   try:
       backend_result = call_backend(xml_bytes)
       result["backend_response"] = backend_result
       return jsonify(result), (backend_result["status_code"] or 200)
   except requests.RequestException as rexc:
       return jsonify({
           "error": "Backend call failed",
           "details": str(rexc),
           "xml": xml_bytes.decode("utf-8")
       }), 502
   except Exception as e:
       return jsonify({
           "error": "Unexpected error while calling backend",
           "details": str(e),
           "trace": traceback.format_exc(),
           "xml": xml_bytes.decode("utf-8")
       }), 500
if __name__ == "__main__":
   app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
