import os
import json
import logging
from typing import Any, Dict, List, Optional
import httpx
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import PlainTextResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import xml.etree.ElementTree as ET

# --------------------------------------------------
# Logging
# --------------------------------------------------
log = logging.getLogger("adapter")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# --------------------------------------------------
# FastAPI
# --------------------------------------------------
app = FastAPI(title="Ring Style Adapter", version="2.1.0")
app.add_middleware(
   CORSMiddleware,
   allow_origins=["*"],
   allow_headers=["*"],
   allow_methods=["*"],
)

# --------------------------------------------------
# Environment
# --------------------------------------------------
# Set these in Azure App Service > Configuration
BACKEND_POST_URL = os.getenv("BACKEND_POST_URL", "").strip()
BACKEND_GET_URL  = os.getenv("BACKEND_GET_URL", "").strip()
API_KEY_REQUIRED = os.getenv("API_KEY_REQUIRED", "false").lower() == "true"
API_KEY          = os.getenv("API_KEY", "").strip()
if not BACKEND_POST_URL:
   log.warning("BACKEND_POST_URL is not set")
# --------------------------------------------------
# Config loader
# --------------------------------------------------
# IMPORTANT: the filename here must match the actual file in the repo
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "mapping.config.json")
def _deep_get(d: Dict[str, Any], path: Optional[str], default: Any = None):
   """Simple dotted-path getter, returns default if any part is missing."""
   if not path:
       return d
   cur = d
   for part in path.split("."):
       if not isinstance(cur, dict) or part not in cur:
           return default
       cur = cur[part]
   return cur
def _transform(v: Any, name: Optional[str]) -> Any:
   if name == "lower" and isinstance(v, str):
       return v.lower()
   if name == "int":
       try:
           return int(v)
       except Exception:
           return v
   return v
def _to_xml(payload: Dict[str, Any]) -> str:
   """
   Build the XML your backend expects.
   It parses the 'questions' list of dicts with keys: id, text, value.
   """
   root = ET.Element("root")
   # Simple scalars first (if present)
   def add(tag: str, val: Optional[str]):
       if val is None:
           return
       el = ET.SubElement(root, tag)
       el.text = str(val)
   add("request_id", payload.get("request_id"))
   add("name",       payload.get("name"))
   add("email",      payload.get("email"))
   add("phone_number", payload.get("phone_number"))
   add("birth_date", payload.get("birth_date"))
   # QuestionAnswers block
   qa = ET.SubElement(root, "questionAnswers")
   for q in payload.get("questions", []):
       q_el = ET.SubElement(qa, "question", attrib={"id": q.get("id", "")})
       t_el = ET.SubElement(q_el, "text")
       t_el.text = str(q.get("text", "")) if q.get("text") is not None else ""
       v_el = ET.SubElement(q_el, "value")
       v_el.text = str(q.get("value", "")) if q.get("value") is not None else ""
   # Pretty string
   return ET.tostring(root, encoding="utf-8", method="xml").decode("utf-8")

# Load mapping config once at startup
try:
   with open(CONFIG_PATH, "r", encoding="utf-8") as f:
       CFG = json.load(f)
   log.info(f"Loaded mapping config from {CONFIG_PATH}")
except Exception as e:
   log.exception(f"Failed to load mapping config from {CONFIG_PATH}: {e}")
   CFG = {
       "forward": {"timeout_sec": 30, "headers": {"Accept": "application/json"}},
       "mapping": {"answers_path": "answers", "fields": {}, "questions": []},
       "response": {"type": "json", "xml_root": "BackendResponse"},
   }

# --------------------------------------------------
# Routes
# --------------------------------------------------
@app.get("/", response_class=PlainTextResponse)
async def root():
   return "Ring Style Adapter OK"

@app.get("/ping", response_class=PlainTextResponse)
async def ping():
   return "pong"

@app.post("/adapter")
async def adapter(request: Request):
   """
   1) Accept JSON or XML.
   2) If JSON: map to backend XML using mapping.config.json.
   3) POST XML to BACKEND_POST_URL (Content-Type: application/xml).
   4) Mirror backend response as JSON or text depending on config.
   """
   if not BACKEND_POST_URL:
       raise HTTPException(500, "BACKEND_POST_URL is not configured")
   ctype = request.headers.get("content-type", "").lower()
   # Prepare headers to forward
   headers = (CFG.get("forward") or {}).get("headers") or {}
   fwd_headers = {k: v for (k, v) in headers.items()}
   fwd_headers["Content-Type"] = "application/xml"
   if API_KEY_REQUIRED and API_KEY:
       fwd_headers["x-api-key"] = API_KEY
   timeout = int(((CFG.get("forward") or {}).get("timeout_sec") or 30))
   # Build XML to send to backend
   try:
       if "xml" in ctype:
           # pass-through
           body_bytes = await request.body()
           xml_to_send = body_bytes.decode("utf-8")
       else:
           # assume JSON
           data = await request.json()
           mp: Dict[str, Any] = (CFG.get("mapping") or {})
           fields: Dict[str, Any] = (mp.get("fields") or {})
           answers_path: str = mp.get("answers_path", "answers")
           q_defs: List[Dict[str, str]] = (mp.get("questions") or [])
           # 1) map scalar fields
           out: Dict[str, Any] = {}
           for out_name, spec in fields.items():
               src = spec.get("from")
               default = spec.get("default")
               transform = spec.get("transform")
               raw_val = _deep_get(data, src, default)
               out[out_name] = _transform(raw_val, transform)
           # normalize common aliases
           out["name"] = out.get("name") or out.get("customer_name")
           out["phone_number"] = out.get("phone_number") or out.get("mobile")
           # 2) map questions
           # incoming answers is a list of {"id": "...", "value": "..."}
           ans_list: List[Dict[str, Any]] = _deep_get(data, answers_path, []) or []
           text_by_id = {q["key"]: q.get("text", "") for q in q_defs if "key" in q}
           questions: List[Dict[str, Any]] = []
           for item in ans_list:
               qid = item.get("id")
               val = item.get("value")
               if not qid:
                   continue
               questions.append({
                   "id": qid,
                   "text": text_by_id.get(qid, ""),
                   "value": val
               })
           out["questions"] = questions
           xml_to_send = _to_xml(out)
       # 3) call backend
       async with httpx.AsyncClient(timeout=timeout) as client:
           resp = await client.post(BACKEND_POST_URL, data=xml_to_send, headers=fwd_headers)
       # 4) mirror/normalize response
       if (CFG.get("response") or {}).get("type") == "json":
           # try to decode as json; if fails, return text
           try:
               return JSONResponse(resp.json(), status_code=resp.status_code)
           except Exception:
               return PlainTextResponse(resp.text, status_code=resp.status_code)
       else:
           # return raw text
           return PlainTextResponse(resp.text, status_code=resp.status_code)
   except HTTPException:
       raise
   except Exception as e:
       log.exception("Adapter error")
       raise HTTPException(500, f"Adapter error: {e}")
