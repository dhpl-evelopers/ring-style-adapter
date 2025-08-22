import os
import json
import html
import logging
from typing import List, Optional
import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import PlainTextResponse, JSONResponse
from pydantic import BaseModel, Field, validator

# ------------------------------------------------------------------------------
# Logging
# ------------------------------------------------------------------------------
log = logging.getLogger("adapter")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ------------------------------------------------------------------------------
# Config (env)
# ------------------------------------------------------------------------------
BACKEND_POST_URL = os.getenv("BACKEND_POST_URL", "").strip()      # e.g. https://<backend>/createRequest
BACKEND_GET_URL  = os.getenv("BACKEND_GET_URL", "").strip()       # optional fetch endpoint
API_KEY_REQUIRED = os.getenv("API_KEY_REQUIRED", "false").lower() == "true"
API_KEY          = os.getenv("API_KEY", "").strip()
if not BACKEND_POST_URL:
   log.warning("BACKEND_POST_URL is not set. /adapter will return 500 until it is provided.")

# ------------------------------------------------------------------------------
# Mapping: load once at startup
# mapping.config.json should be deployed next to app.py
# Format: [{ "key": "<question_id>", "text": "<question label>" }, ...]
# ------------------------------------------------------------------------------
MAPPING_PATH = os.path.join(os.path.dirname(__file__), "mapping.config.json")
try:
   with open(MAPPING_PATH, "r", encoding="utf-8") as f:
       mapping_list = json.load(f)
   ID_TO_TEXT = {item["key"]: item["text"] for item in mapping_list}
   log.info("Loaded %d question mappings", len(ID_TO_TEXT))
except Exception as e:
   log.error("Failed to load mapping.config.json: %s", e)
   ID_TO_TEXT = {}

# ------------------------------------------------------------------------------
# Request models
# ------------------------------------------------------------------------------
class QA(BaseModel):
   id: str = Field(..., description="Question ID (matches mapping.config.json 'key')")
   # Accept either 'answer' or legacy 'value'
   answer: Optional[str] = None
   value: Optional[str] = None
   @property
   def final_answer(self) -> str:
       # prefer 'answer' if provided; else 'value'; else empty string
       return (self.answer if self.answer is not None else self.value) or ""

class AdapterRequest(BaseModel):
   # Mandatory fields per your confirmation
   name: str
   email: str
   phone_number: str
   birth_date: str
   request_id: str
   # Answers array is mandatory and must be non-empty
   questionAnswers: List[QA]
   # Defensive validation
   @validator("questionAnswers")
   def _non_empty_answers(cls, v: List[QA]):
       if not v:
           raise ValueError("questionAnswers must not be empty")
       return v

# ------------------------------------------------------------------------------
# App
# ------------------------------------------------------------------------------
app = FastAPI(title="Ring Style Adapter", version="2.1.0")

@app.get("/", response_class=PlainTextResponse)
def root():
   return "Ring Style Adapter OK"

@app.get("/ping", response_class=PlainTextResponse)
def ping():
   return "pong"

# ------------------------------------------------------------------------------
# Utilities
# ------------------------------------------------------------------------------
def to_backend_xml(payload: AdapterRequest) -> str:
   """
   Build backend-compatible XML:
<root>
<name>..</name>
<email>..</email>
<phone_number>..</phone_number>
<request_id>..</request_id>
<birth_date>..</birth_date>
<questionAnswers>
<questionAnswer>
<question>Full question text from mapping</question>
<answer>User answer</answer>
</questionAnswer>
       ...
</questionAnswers>
</root>
   """
   # Escape all text content
   name = html.escape(payload.name)
   email = html.escape(payload.email)
   phone = html.escape(payload.phone_number)
   birth = html.escape(payload.birth_date)
   rid = html.escape(payload.request_id)
   qa_parts: List[str] = []
   for qa in payload.questionAnswers:
       qid = qa.id
       qtext = ID_TO_TEXT.get(qid)
       if not qtext:
           # Clear, client-side error: unmapped question ID
           raise HTTPException(
               status_code=400,
               detail=f"No mapping found for question ID '{qid}'. Add it to mapping.config.json."
           )
       answer_text = html.escape(qa.final_answer)
       qtext_esc = html.escape(qtext)
       qa_parts.append(
           f"<questionAnswer><question>{qtext_esc}</question><answer>{answer_text}</answer></questionAnswer>"
       )
   qa_block = "<questionAnswers>" + "".join(qa_parts) + "</questionAnswers>"
   xml = (
       "<root>"
       f"<name>{name}</name>"
       f"<email>{email}</email>"
       f"<phone_number>{phone}</phone_number>"
       f"<request_id>{rid}</request_id>"
       f"<birth_date>{birth}</birth_date>"
       f"{qa_block}"
       "</root>"
   )
   return xml

async def forward_to_backend(xml_body: str) -> httpx.Response:
   """
   POST the XML to the backend with headers you specified.
   """
   if not BACKEND_POST_URL:
       raise HTTPException(500, "Adapter is not configured: BACKEND_POST_URL is missing.")
   headers = {
       # The backend in your screenshots expects XML. Accept JSON response is fine.
       "Content-Type": "application/xml",
       "Accept": "application/json",
   }
   if API_KEY_REQUIRED:
       if not API_KEY:
           raise HTTPException(500, "API_KEY_REQUIRED=true but API_KEY is not set.")
       headers["x-api-key"] = API_KEY
   timeout = httpx.Timeout(30.0, connect=10.0)
   async with httpx.AsyncClient(timeout=timeout, verify=True) as client:
       resp = await client.post(BACKEND_POST_URL, content=xml_body.encode("utf-8"), headers=headers)
       return resp

# ------------------------------------------------------------------------------
# Main adapter endpoint
# ------------------------------------------------------------------------------
@app.post("/adapter")
async def adapter(req: Request):
   # Enforce JSON body
   if req.headers.get("content-type", "").split(";")[0].strip().lower() != "application/json":
       raise HTTPException(415, "Unsupported Content-Type. Use application/json.")
   try:
       body = await req.json()
   except Exception:
       raise HTTPException(400, "Invalid JSON body")
   try:
       model = AdapterRequest(**body)
   except Exception as e:
       # Pydantic will describe the exact field problem
       raise HTTPException(422, detail=str(e))
   # Build XML
   xml_body = to_backend_xml(model)
   log.info("Built backend XML (%d bytes)", len(xml_body))
   # Forward
   try:
       resp = await forward_to_backend(xml_body)
   except HTTPException:
       raise
   except Exception as e:
       log.exception("Error calling backend")
       raise HTTPException(502, f"Failed to call backend: {e}")
   # Pass-through style response (helpful for debugging)
   content_type = resp.headers.get("content-type", "")
   if "application/json" in content_type:
       return JSONResponse(status_code=resp.status_code, content=resp.json())
   else:
       # When backend returns XML/plain text, surface as plain text
       return PlainTextResponse(status_code=resp.status_code, content=resp.text)
