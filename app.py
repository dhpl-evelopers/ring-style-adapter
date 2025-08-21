import json, os, pathlib
from typing import Any, Dict, List, Optional
from fastapi import FastAPI, HTTPException, Header
import httpx
import html
app = FastAPI(title="Ring Style Adapter (flexible JSON to XML)")
# Health check endpoints
@app.get("/")
def health():
   return {"ok": True}
@app.get("/health")
def health_check():
   return {"ok": True}
# Load mapping configuration
cfg_path = pathlib.Path(__file__).with_name("mapping.config.json")
try:
   CFG = json.loads(cfg_path.read_text())
except Exception:
   # Fallback defaults if config cannot be read
   CFG = {
       "xml_root": "request",
       "result_key_field_candidates": ["request_id", "result_key", "response_id", "responseKey", "resultKey"],
       "field_map": {},
       "answer_key_to_question": {},
       "questionOrder": [],
       "defaults": {"name": "", "email": "", "phone_number": "", "birth_date": ""}
   }
BACKEND_POST_URL = os.getenv("BACKEND_POST_URL", "http://localhost:9090/createRequest")
API_KEY_REQUIRED = os.getenv("API_KEY_REQUIRED", "false").lower() == "true"
API_KEY = os.getenv("API_KEY", "")
def api_key_ok(x_api_key: Optional[str]) -> bool:
   if not API_KEY_REQUIRED:
       return True
   return x_api_key == API_KEY
@app.post("/ingest")
async def ingest(payload: Dict[str, Any], x_api_key: Optional[str] = Header(None)):
   # Check API key if required
   if not api_key_ok(x_api_key):
       raise HTTPException(status_code=401, detail="unauthorized")
   # Determine request_id from payload using candidate keys
   request_id = None
   for cand in CFG.get("result_key_field_candidates", []):
       if cand in payload:
           request_id = payload.get(cand)
           break
   if not request_id or not isinstance(request_id, str):
       raise HTTPException(status_code=400, detail="request_id (or equivalent) is required")
   # Map and extract common fields (name, email, phone_number, birth_date)
   data: Dict[str, Any] = {}
   for input_key, output_key in CFG.get("field_map", {}).items():
       if input_key in payload:
           data[output_key] = payload.get(input_key)
   name = data.get("name")
   email = data.get("email")
   phone = data.get("phone_number")
   birth_date = data.get("birth_date")
   defaults = CFG.get("defaults", {})
   # Replace None/False/blank with defaults
   if not name:
       name = defaults.get("name", "")
   if not email:
       email = defaults.get("email", "")
   if not phone:
       phone = defaults.get("phone_number", "")
   if not birth_date:
       birth_date = defaults.get("birth_date", "")
   # Validate and process answers
   answers = payload.get("answers")
   if not isinstance(answers, list) or len(answers) == 0:
       raise HTTPException(status_code=400, detail="answers list is required")
   first = answers[0]
   format_type = None
   if isinstance(first, dict):
       if "question" in first and "answer" in first:
           format_type = "structured_text"
       elif "key" in first and "value" in first:
           format_type = "keyed"
       else:
           raise HTTPException(status_code=422, detail="Invalid answer format")
   elif isinstance(first, str):
       format_type = "answers_only"
   else:
       raise HTTPException(status_code=422, detail="Invalid answer element type")
   question_answer_pairs: List[tuple] = []
   if format_type == "structured_text":
       for item in answers:
           if not isinstance(item, dict) or "question" not in item or "answer" not in item:
               raise HTTPException(status_code=422, detail="Invalid structured answer item")
           q = item.get("question") or ""
           a = item.get("answer") or ""
           question_answer_pairs.append((q, a))
   elif format_type == "keyed":
       map_key_to_q = CFG.get("answer_key_to_question", {})
       for item in answers:
           if not isinstance(item, dict) or "key" not in item or "value" not in item:
               raise HTTPException(status_code=422, detail="Invalid keyed answer item")
           key = item["key"]
           a = item.get("value") or ""
           question_text = map_key_to_q.get(key)
           if question_text is None:
               raise HTTPException(status_code=422, detail=f"Unknown key '{key}' in answers")
           question_answer_pairs.append((question_text, a))
   elif format_type == "answers_only":
       order = CFG.get("questionOrder", [])
       if len(answers) > len(order):
           raise HTTPException(status_code=422, detail={"error":"too_many_answers","expected":len(order),"got":len(answers)})
       for idx, a in enumerate(answers):
           key = order[idx]
           question_text = CFG.get("answer_key_to_question", {}).get(key)
           if question_text is None:
               raise HTTPException(status_code=422, detail=f"No question mapping for key '{key}'")
           ans = a or ""
           question_answer_pairs.append((question_text, ans))
   # Build XML payload
   root = CFG.get("xml_root", "request")
   xml_lines: List[str] = []
   xml_lines.append("<?xml version='1.0' encoding='utf-8'?>")
   xml_lines.append(f"<{root}>")
   xml_lines.append(f"  <request_id>{html.escape(request_id)}</request_id>")
   xml_lines.append(f"  <email>{html.escape(email)}</email>")
   xml_lines.append(f"  <name>{html.escape(name)}</name>")
   xml_lines.append(f"  <phone_number>{html.escape(phone)}</phone_number>")
   xml_lines.append(f"  <birth_date>{html.escape(birth_date)}</birth_date>")
   xml_lines.append(f"  <questionAnswers>")
   for q, a in question_answer_pairs:
       xml_lines.append(f"    <questionAnswer>")
       xml_lines.append(f"      <question>{html.escape(str(q))}</question>")
       xml_lines.append(f"      <answer>{html.escape(str(a))}</answer>")
       xml_lines.append(f"    </questionAnswer>")
   xml_lines.append(f"  </questionAnswers>")
   xml_lines.append(f"</{root}>")
   xml_body = "\n".join(xml_lines)
   # Submit to backend
   async with httpx.AsyncClient(timeout=30) as client:
       r = await client.post(BACKEND_POST_URL, content=xml_body, headers={"Content-Type": "application/xml"})
   if r.status_code // 100 != 2:
       raise HTTPException(status_code=502, detail={"backend_status": r.status_code, "body": r.text})
   return {"status": "ok", "result_key": request_id}
