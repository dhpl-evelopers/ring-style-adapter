import os
import json
import logging
import pathlib
from typing import Any, Dict, List, Optional, Tuple
import httpx
from fastapi import FastAPI, Body, Query, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
app = FastAPI(title="Ring Style Adapter (flex → XML)")
# ---- CORS (open by default; tighten if needed)
app.add_middleware(
   CORSMiddleware,
   allow_origins=["*"],
   allow_credentials=True,
   allow_methods=["*"],
   allow_headers=["*"],
)
# ---- Config & env
CFG_PATH = pathlib.Path(__file__).with_name("mapping.config.json")
BACKEND_POST_URL = os.getenv("BACKEND_POST_URL", "").strip()
API_KEY = os.getenv("API_KEY", "").strip()  # optional; if set we expect X-API-Key header
log = logging.getLogger("uvicorn.error")

def _load_config() -> Dict[str, Any]:
   if not CFG_PATH.exists():
       raise RuntimeError(f"mapping.config.json not found at {CFG_PATH}")
   with CFG_PATH.open("r", encoding="utf-8") as f:
       return json.load(f)

CONFIG = _load_config()

# ---------- Helpers: auth & safety
def _check_api_key(req: Request) -> None:
   """Optional simple key check: if API_KEY is set, require header X-API-Key."""
   if not API_KEY:
       return
   got = req.headers.get("x-api-key") or req.headers.get("X-Api-Key")
   if got != API_KEY:
       raise HTTPException(status_code=401, detail="Invalid API key")

def _require_backend():
   if not BACKEND_POST_URL:
       raise HTTPException(
           status_code=500,
           detail="BACKEND_POST_URL not configured in environment",
       )

# ---------- Input normalization
def _get_payload(body: Dict[str, Any]) -> Dict[str, Any]:
   """
   The UI may send either:
     { ...fields..., questionAnswers: [...], answers: [...] }
   or it may wrap everything inside { "data": { ... } }
   """
   return body.get("data", body)

def _coalesce(*vals) -> Optional[str]:
   for v in vals:
       if v is not None and v != "":
           return v
   return None

def _pick_user_fields(p: Dict[str, Any]) -> Tuple[str, str, str, str]:
   """
   Map multiple possible UI field names to the canonical names that the backend XML expects.
   - result_key -> also accept resultKey or id
   - full_name  -> also accept fullName
   - phone_number -> also accept phoneNumber
   - date       -> also accept dateOfBirth
   """
   result_key = _coalesce(p.get("result_key"), p.get("resultKey"), p.get("id"))
   if not result_key:
       raise HTTPException(status_code=400, detail="Missing mandatory field: result_key / resultKey / id")
   full_name = _coalesce(p.get("full_name"), p.get("fullName"))
   email = p.get("email", "")
   phone_number = _coalesce(p.get("phone_number"), p.get("phoneNumber"))
   date = _coalesce(p.get("date"), p.get("dateOfBirth"))
   # Fill defaults from config if missing
   defaults = CONFIG.get("defaults", {})
   full_name = full_name or defaults.get("full_name", "")
   email = email or defaults.get("email", "")
   phone_number = phone_number or defaults.get("phone_number", "")
   date = date or defaults.get("date", "")
   return result_key, full_name, email, phone_number, date

# ---------- Answers normalization
def _detect_mode(p: Dict[str, Any]) -> str:
   """
   Return one of: 'structured', 'keyed', 'answers_only', or 'none'
   """
   if isinstance(p.get("questionAnswers"), list) and p["questionAnswers"]:
       return "structured"
   ans = p.get("answers")
   if isinstance(ans, list) and ans:
       if isinstance(ans[0], dict) and "key" in ans[0] and "value" in ans[0]:
           return "keyed"
       if isinstance(ans[0], str):
           return "answers_only"
   return "none"

def _from_structured(question_answers: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
   """
   Input like: [{questionId: 70955, selectedOption:{id:299504, value:"Self"}}, ...]
   Return:     [{questionId: 70955, optionId: 299504, value: "Self"}, ...]
   """
   out = []
   for item in question_answers:
       qid = item.get("questionId")
       sel = item.get("selectedOption", {}) or {}
       out.append(
           {
               "questionId": qid,
               "optionId": sel.get("id"),
               "value": sel.get("value"),
           }
       )
   return out

def _from_keyed(keyed: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
   """
   Input like: [{key:"who", value:"Self"}, {key:"gender", value:"Female"}...]
   Use config.keyToQuestionId and config.options to resolve IDs.
   """
   key_to_qid = CONFIG.get("keyToQuestionId", {})
   options = CONFIG.get("options", {})
   out = []
   for kv in keyed:
       key = kv.get("key")
       val = kv.get("value")
       qid = key_to_qid.get(str(key))
       if not qid:
           log.warning(f"Unknown key in mapping.config.json: {key!r}")
           continue
       opt_map = options.get(str(qid), {})
       opt_id = opt_map.get(str(val)) or opt_map.get(val)
       out.append({"questionId": qid, "optionId": opt_id, "value": val})
   return out

def _from_answers_only(answers: List[str]) -> List[Dict[str, Any]]:
   """
   Input like: ["Self","Female", ...]
   Use config.questionOrder list to map by index.
   """
   order = CONFIG.get("questionOrder", [])
   out = []
   for idx, val in enumerate(answers):
       if idx >= len(order):
           log.warning("Answer index %s has no questionOrder mapping", idx)
           break
       qid = order[idx]
       opt_map = CONFIG.get("options", {}).get(str(qid), {})
       opt_id = opt_map.get(str(val)) or opt_map.get(val)
       out.append({"questionId": qid, "optionId": opt_id, "value": val})
   return out

def _build_qna(p: Dict[str, Any]) -> List[Dict[str, Any]]:
   mode = _detect_mode(p)
   if mode == "structured":
       return _from_structured(p["questionAnswers"])
   if mode == "keyed":
       return _from_keyed(p["answers"])
   if mode == "answers_only":
       return _from_answers_only(p["answers"])
   raise HTTPException(
       status_code=400,
       detail="No answers provided. Send either questionAnswers[], answers[] (keyed), or answers[] (strings).",
   )

# ---------- XML builder
def _xml_escape(s: Optional[str]) -> str:
   if s is None:
       return ""
   return (
       str(s)
       .replace("&", "&amp;")
       .replace("<", "&lt;")
       .replace(">", "&gt;")
       .replace('"', "&quot;")
       .replace("'", "&apos;")
   )

def _build_xml(
   result_key: str,
   full_name: str,
   email: str,
   phone_number: str,
   date: str,
   qna: List[Dict[str, Any]],
) -> str:
   """
   Produce exactly what your backend log stream shows:
<data>
<id>...</id>
<quiz_id>...</quiz_id>          (left empty)
<full_name>...</full_name>
<email>...</email>
<phone_number>...</phone_number>
<date>YYYY-MM-DD</date>
<custom_inputs></custom_inputs> (empty)
<products></products>           (empty)
<questionAnswers>
       [{ 'question': 'Q1. ...', 'questionId': 70955, 'isMasterQuestion': false,
          'questionType': 'singleAnswer',
          'selectedOption': {'id': 299504, 'value': 'Self', 'image': null, 'score_value': 0}} ...]
</questionAnswers>
</data>
   Note: We embed the questionAnswers JSON text inside the XML like the backend expects.
   """
   # Attempt to add 'question' text via mapping (optional)
   qid_to_text = CONFIG.get("qidToText", {})  # optional map { "70955": "Q1. Who are you purchasing for?" }
   enriched = []
   for it in qna:
       qid = it.get("questionId")
       enriched.append(
           {
               "question": qid_to_text.get(str(qid)),
               "questionId": qid,
               "isMasterQuestion": False,
               "questionType": "singleAnswer",
               "selectedOption": {
                   "id": it.get("optionId"),
                   "value": it.get("value"),
                   "image": None,
                   "score_value": 0,
               },
           }
       )
   qa_json = json.dumps(enriched, ensure_ascii=False)
   xml = (
       "<data>"
       f"<id>{_xml_escape(result_key)}</id>"
       "<quiz_id></quiz_id>"
       f"<full_name>{_xml_escape(full_name)}</full_name>"
       f"<email>{_xml_escape(email)}</email>"
       f"<phone_number>{_xml_escape(phone_number)}</phone_number>"
       f"<date>{_xml_escape(date)}</date>"
       "<custom_inputs>[]</custom_inputs>"
       "<products>[]</products>"
       f"<questionAnswers>{_xml_escape(qa_json)}</questionAnswers>"
       "</data>"
   )
   return xml

# ---------- Health + root
@app.get("/")
def root():
   return {"ok": True, "service": "ring-style-adapter", "version": "1.0.0"}

@app.get("/health")
def health():
   return {"ok": True}

# ---------- Main ingest
@app.post("/ingest")
async def ingest(
   req: Request,
   payload: Dict[str, Any] = Body(..., description="Flexible UI JSON"),
   preview: bool = Query(False, description="If true, do not call backend; just show XML"),
   echo: bool = Query(True, description="If true, include backend status + body in response"),
):
   """
   Accept flexible UI JSON -> normalize -> build XML -> call backend /createRequest.
   """
   _check_api_key(req)
   _require_backend()
   # 1) Normalize payload surface
   p = _get_payload(payload)
   # 2) User fields
   result_key, full_name, email, phone, date = _pick_user_fields(p)
   # 3) Answers → structured list [{questionId, optionId, value}]
   qna = _build_qna(p)
   if not qna:
       raise HTTPException(status_code=400, detail="No usable answers supplied.")
   # 4) Build XML
   xml_body = _build_xml(result_key, full_name, email, phone, date, qna)
   # 5) Optionally call backend
   backend_status = None
   backend_headers = None
   backend_body = None
   if not preview:
       headers = {"Content-Type": "application/xml"}
       # pass-through API key to backend if needed (optional)
       if API_KEY:
           headers["X-API-Key"] = API_KEY
       try:
           async with httpx.AsyncClient(timeout=30) as client:
               resp = await client.post(BACKEND_POST_URL, content=xml_body.encode("utf-8"), headers=headers)
           backend_status = resp.status_code
           # capture text (backend sometimes returns text/plain)
           backend_body = resp.text
           backend_headers = {"content-type": resp.headers.get("content-type")}
       except httpx.RequestError as e:
           raise HTTPException(status_code=502, detail=f"Backend request failed: {e}") from e
   # 6) Build adapter response
   out = {
       "sent_xml": xml_body,
       "context": {
           "result_key": result_key,
           "full_name": full_name,
           "email": email,
           "phone_number": phone,
           "date": date,
       },
   }
   if echo and backend_status is not None:
       out.update(
           {
               "backend_status": backend_status,
               "backend_headers": backend_headers,
               "backend_body": backend_body,
           }
       )
   return JSONResponse(out)

# ---------- Uvicorn entry (for local runs)
if __name__ == "__main__":
   import uvicorn
   uvicorn.run("app:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")), reload=True)
