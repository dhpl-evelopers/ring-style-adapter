import os
import json
import logging
from typing import Any, Dict, List, Optional
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import PlainTextResponse, JSONResponse
import httpx
import xml.etree.ElementTree as ET
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("adapter")
# -------------------------
# Env
# -------------------------
BACKEND_POST_URL = os.getenv("BACKEND_POST_URL", "").strip()
BACKEND_GET_URL  = os.getenv("BACKEND_GET_URL", "").strip()
API_KEY_REQUIRED = os.getenv("API_KEY_REQUIRED", "false").lower() == "true"
API_KEY          = os.getenv("API_KEY", "").strip()
if not BACKEND_POST_URL:
   log.warning("BACKEND_POST_URL is not set. POST /adapter will fail until it is set.")
# -------------------------
# Mapping config loader
# -------------------------
def load_mapping() -> Dict[str, Any]:
   path = os.path.join(os.path.dirname(__file__), "mapping.config.json")
   with open(path, "r", encoding="utf-8") as f:
       return json.load(f)
MAPPING = load_mapping()
# -------------------------
# Helpers (SAFE)
# -------------------------
def safe_text(node: ET.Element, tag: str, default: str = "") -> str:
   """Return text of tag under node, never crash."""
   el = node.find(tag)
   return el.text.strip() if (el is not None and el.text) else default
def set_text(parent: ET.Element, tag: str, value: Optional[str]):
   """Create tag under parent with text (even if value is None)."""
   el = ET.SubElement(parent, tag)
   el.text = "" if value is None else str(value)
def get_from_path(obj: Dict[str, Any], dotted: Optional[str], default: Any = None) -> Any:
   """Safely read nested dict by dot-path; returns default if missing."""
   if not dotted:
       return default
   cur: Any = obj
   for part in dotted.split("."):
       if not isinstance(cur, dict) or part not in cur:
           return default
       cur = cur[part]
   return cur
def to_pretty_xml(elem: ET.Element) -> str:
   """Element -> xml string (no declaration, single line ok for backend)."""
   return ET.tostring(elem, encoding="utf-8").decode("utf-8")
# -------------------------
# JSON -> Backend XML using mapping.config.json
# -------------------------
def json_to_backend_xml(payload: Dict[str, Any]) -> str:
   """
   Builds:
<root>
<name>..</name>
<email>..</email>
<phone_number>..</phone_number>
<request_id>..</request_id>
<birth_date>..</birth_date>
<questionAnswers>
<questionAnswer>
<question>...</question>
<answer>...</answer>
</questionAnswer>
       ...
</questionAnswers>
</root>
   """
   mp = MAPPING["mapping"]
   root = ET.Element("root")
   # Contact (all safe)
   contact_cfg = mp.get("contact", {})
   name        = get_from_path(payload, contact_cfg.get("name"))
   email       = get_from_path(payload, contact_cfg.get("email"))
   phone       = get_from_path(payload, contact_cfg.get("phone_number"))
   birth_date  = get_from_path(payload, contact_cfg.get("birth_date"))
   request_id  = get_from_path(payload, contact_cfg.get("request_id"))
   set_text(root, "name",         name)
   set_text(root, "email",        email)
   set_text(root, "phone_number", phone)
   set_text(root, "request_id",   request_id)
   set_text(root, "birth_date",   birth_date)
   # Q&A
   answers_path = mp.get("answers_path", "answers")
   incoming_answers: Any = get_from_path(payload, answers_path, [])
   qa_parent = ET.SubElement(root, "questionAnswers")
   # Two accepted shapes:
   #  A) [{ "question": "...", "answer": "..." }, ...]
   #  B) [{"id":"q1_style","value":"Solitaire"}, ...]  (we’ll map ids via "questions" table)
   questions_table = {row["key"]: row["text"] for row in mp.get("questions", [])}
   if isinstance(incoming_answers, list):
       for item in incoming_answers:
           q = a = ""
           if isinstance(item, dict):
               if "question" in item and "answer" in item:
                   q = str(item.get("question") or "")
                   a = str(item.get("answer") or "")
               elif "id" in item and "value" in item:
                   q = str(questions_table.get(str(item["id"]), str(item["id"])))
                   a = str(item.get("value") or "")
               elif "key" in item and "value" in item:
                   # some UIs send key/value
                   q = str(questions_table.get(str(item["key"]), str(item["key"])))
                   a = str(item.get("value") or "")
           qa = ET.SubElement(qa_parent, "questionAnswer")
           set_text(qa, "question", q)
           set_text(qa, "answer", a)
   else:
       # If caller sent object/dict, attempt to convert to pairs
       if isinstance(incoming_answers, dict):
           for k, v in incoming_answers.items():
               qa = ET.SubElement(qa_parent, "questionAnswer")
               set_text(qa, "question", str(questions_table.get(str(k), str(k))))
               set_text(qa, "answer", "" if v is None else str(v))
       else:
           # still create empty container (never crash)
           pass
   return to_pretty_xml(root)
# -------------------------
# Backend XML -> JSON (safe)
# -------------------------
def backend_xml_to_json(xml_text: str) -> Dict[str, Any]:
   """
   Converts the backend XML response back to JSON (defensive parsing).
   If backend already returns JSON, this isn’t used.
   """
   try:
       root = ET.fromstring(xml_text)
   except Exception:
       # Not XML, return as raw text wrapper
       return {"raw": xml_text}
   data: Dict[str, Any] = {
       "name":         safe_text(root, "name"),
       "email":        safe_text(root, "email"),
       "phone_number": safe_text(root, "phone_number"),
       "request_id":   safe_text(root, "request_id"),
       "birth_date":   safe_text(root, "birth_date"),
       "questionAnswers": []
   }
   qa_parent = root.find("questionAnswers")
   if qa_parent is not None:
       for qa in qa_parent.findall("questionAnswer"):
           data["questionAnswers"].append({
               "question": safe_text(qa, "question"),
               "answer":   safe_text(qa, "answer"),
           })
   return data
# -------------------------
# FastAPI
# -------------------------
app = FastAPI(title="Ring Style Adapter", version="2.1.0")
@app.get("/", response_class=PlainTextResponse)
async def root():
   return "Ring Style Adapter OK"
@app.get("/ping", response_class=PlainTextResponse)
async def ping():
   return "pong"
@app.post("/adapter")
async def adapter(request: Request):
   """
   1) Accept JSON or XML from the client
   2) Map to backend XML (defensively)
   3) POST to BACKEND_POST_URL (with optional API key)
   4) Return backend response; if XML and client accepts JSON, convert safely
   """
   if not BACKEND_POST_URL:
       raise HTTPException(status_code=500, detail="BACKEND_POST_URL not configured")
   content_type = (request.headers.get("content-type") or "").lower()
   accept = (request.headers.get("accept") or "application/json").lower()
   # ---- Step 1: Parse input
   body_bytes = await request.body()
   if not body_bytes:
       raise HTTPException(status_code=400, detail="Empty body")
   client_json: Optional[Dict[str, Any]] = None
   incoming_xml: Optional[str] = None
   if "application/json" in content_type:
       try:
           client_json = json.loads(body_bytes.decode("utf-8"))
       except Exception:
           raise HTTPException(status_code=400, detail="Invalid JSON")
   elif "application/xml" in content_type or "text/xml" in content_type:
       incoming_xml = body_bytes.decode("utf-8")
   else:
       # default: try JSON first then treat as XML text
       try:
           client_json = json.loads(body_bytes.decode("utf-8"))
       except Exception:
           incoming_xml = body_bytes.decode("utf-8")
   # ---- Step 2: Build backend XML (defensively)
   if client_json is not None:
       backend_xml = json_to_backend_xml(client_json)
   else:
       # If client sent XML already, do a safe normalization: parse & rebuild so missing tags don’t crash later.
       try:
           _ = ET.fromstring(incoming_xml or "")
           backend_xml = incoming_xml or "<root/>"
       except Exception:
           # If even that fails, still send a minimal root
           backend_xml = "<root/>"
   # ---- Step 3: POST to backend
   headers = {"Content-Type": "application/xml"}
   if API_KEY_REQUIRED and API_KEY:
       headers["x-api-key"] = API_KEY
   try:
       async with httpx.AsyncClient(timeout=MAPPING.get("forward", {}).get("timeout_sec", 30)) as client:
           resp = await client.post(BACKEND_POST_URL, headers=headers, content=backend_xml)
   except httpx.RequestError as e:
       log.exception("Backend call failed")
       raise HTTPException(status_code=502, detail=f"Backend unreachable: {str(e)}")
   # ---- Step 4: Return mapped/transparent response
   resp_ct = resp.headers.get("content-type", "").lower()
   # If client asked for JSON, try to provide JSON
   if "application/json" in accept:
       if "application/json" in resp_ct:
           return JSONResponse(status_code=resp.status_code, content=resp.json())
       else:
           # XML or text from backend → convert to JSON safely
           return JSONResponse(status_code=resp.status_code, content=backend_xml_to_json(resp.text))
   # Otherwise mirror backend content-type
   return PlainTextResponse(status_code=resp.status_code, content=resp.text, media_type=resp_ct or "text/plain")
