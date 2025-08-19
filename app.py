import os, json, logging, pathlib
from typing import Any, Dict, List
import httpx
from fastapi import FastAPI, Request, HTTPException, Query
from fastapi.responses import JSONResponse
# ---------- Logging ----------
log = logging.getLogger("adapter")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
app = FastAPI(title="Ring Style Adapter (flex → JSON)", version="1.0.0")
# ---------- Env ----------
BACKEND_POST_URL = os.getenv("BACKEND_POST_URL", "").strip()   # e.g. https://<api>.azurewebsites.net/createRequestJSON
BACKEND_GET_URL  = os.getenv("BACKEND_GET_URL", "").strip()    # e.g. https://<api>.azurewebsites.net/fetchResponse?response_id=
API_KEY_REQUIRED = os.getenv("API_KEY_REQUIRED", "false").lower() == "true"
API_KEY          = os.getenv("API_KEY", "")
TIMEOUT_SEC      = float(os.getenv("TIMEOUT_SECONDS", "30"))
def _require_backend():
   if not BACKEND_POST_URL:
       raise HTTPException(status_code=500, detail="BACKEND_POST_URL not configured")
def _check_api_key(req: Request):
   if not API_KEY_REQUIRED:
       return
   if not API_KEY or req.headers.get("x-api-key") != API_KEY:
       raise HTTPException(status_code=401, detail="unauthorized")
# ---------- Load mapping.config.json once ----------
CFG_PATH = pathlib.Path(__file__).with_name("mapping.config.json")
log.info("Reading mapping config from %s", CFG_PATH)
CONFIG: Dict[str, Any] = {}
try:
   with open(CFG_PATH, "r", encoding="utf-8") as f:
       head = f.read(200)
       log.info("First 200 chars of mapping.config.json: %s", head)
       f.seek(0)
       CONFIG = json.load(f)
except Exception as e:
   log.error("Could not read mapping.config.json: %s", e)
   CONFIG = {}
XML_ROOT = CONFIG.get("xml_root", "request")  # unused here but kept for compatibility
FIELD_MAP: Dict[str, str] = {k.lower(): v for k, v in CONFIG.get("field_map", {}).items()}
ANS_KEY_TO_Q: Dict[str, str] = CONFIG.get("answer_key_to_question", {})
DEFAULTS: Dict[str, Any] = CONFIG.get("defaults", {})
RESULT_KEY_CANDIDATES = [s.lower() for s in CONFIG.get("result_key_field_candidates", ["result_key", "response_id"])]
# ---------- Helpers: flexible → normalized JSON ----------
def _find_result_key(d: Any) -> str:
   if not isinstance(d, dict):
       return ""
   # flat
   for k, v in d.items():
       if k.lower() in RESULT_KEY_CANDIDATES:
           return str(v)
   # nested
   for v in d.values():
       if isinstance(v, dict):
           rk = _find_result_key(v)
           if rk:
               return rk
   return ""
def _flatten(d: Any) -> Dict[str, Any]:
   out: Dict[str, Any] = {}
   def visit(pfx: str, node: Any):
       if isinstance(node, dict):
           for k, v in node.items():
               visit(f"{pfx}.{k}" if pfx else k, v)
       elif isinstance(node, list):
           for idx, v in enumerate(node):
               visit(f"{pfx}.{idx}" if pfx else str(idx), v)
       else:
           out[pfx] = node
   visit("", d)
   return out
def _normalize_fields(raw: Dict[str, Any]) -> Dict[str, Any]:
   result: Dict[str, Any] = dict(DEFAULTS)
   flat = _flatten(raw)
   # Map via FIELD_MAP (case-insensitive)
   for full_key, value in flat.items():
       tail = full_key.split(".")[-1].lower()
       if tail in FIELD_MAP:
           result[FIELD_MAP[tail]] = value
       # if field already matches a default key, keep
       elif tail in DEFAULTS and tail not in result:
           result[tail] = value
   # include result_id/request_id
   rk = _find_result_key(raw)
   if rk:
       result.setdefault("request_id", rk)  # backend expects "request_id"
   # answers → questions/answers text
   answers_structured: List[Dict[str, str]] = []
   if isinstance(raw.get("answers"), dict):
       # {'who': 'Self', 'gender': 'Male'} using mapping to friendly text
       for key, value in raw["answers"].items():
           q_text = ANS_KEY_TO_Q.get(key, key)
           answers_structured.append({"question": q_text, "answer": str(value)})
   elif isinstance(raw.get("answers"), list):
       # already structured?
       for item in raw["answers"]:
           if isinstance(item, dict) and {"question", "answer"} <= set(item.keys()):
               answers_structured.append({"question": str(item["question"]), "answer": str(item["answer"])})
   if answers_structured:
       # turn into two comma-separated strings to fit your backend’s generateQNAModel use
       q = ", ".join(a["question"] for a in answers_structured)
       a = ", ".join(a["answer"]   for a in answers_structured)
       result["questions"] = q
       result["answers"]   = a
   return result
# ---------- Routes ----------
@app.get("/")
def root():
   return {
       "ok": True,
       "version": "1.0.0",
       "requires_api_key": API_KEY_REQUIRED,
       "has_backend_post": bool(BACKEND_POST_URL),
       "has_backend_get": bool(BACKEND_GET_URL),
       "config_loaded": bool(CONFIG),
   }
@app.get("/health")
def health():
   return {"ok": True}
from fastapi import Body, Query, Request

@app.post("/ingest")

async def ingest(

    raw: dict = Body(

        ...,

        example={

            "result_key": "abc123",

            "full_name": "Sakshi",

            "email": "sakshi@example.com",

            "answers": {"who": "Self", "gender": "Male"}

        },

        description="Flexible UI JSON payload"

    ),

    preview: bool = Query(False, description="Return normalized JSON without calling backend"),

    echo: bool = Query(False, description="Return backend status + body"),

    req: Request = None,  # optional – keep if you still want access to headers (e.g., x-api-key)

):

    _check_api_key(req)

    _require_backend()

    # you no longer need: raw = await req.json()

    # 'raw' already contains the JSON body as a dict.

    # ... continue with your existing logic:

    normalized = _normalize_to_backend_json(raw)

    # sanity check & preview/echo handling exactly as before

    missing = [k for k in ["request_id", "name", "email"] if not normalized.get(k)]

    if missing:

        log.warning("Normalized JSON missing fields: %s", missing)

    if preview:

        return JSONResponse({"normalized": normalized})

    # call backend if not preview...

    # (your existing httpx call stays the same, just use `json=normalized`)
 
   _require_backend()
   # Send JSON to /createRequestJSON (your backend’s JSON endpoint)
   try:
       async with httpx.AsyncClient(timeout=TIMEOUT_SEC) as client:
           resp = await client.post(
               BACKEND_POST_URL,
               json={
                   "request_id": normalized.get("request_id"),
                   "email":      normalized.get("email"),
                   "name":       normalized.get("name"),
                   "phoneNumber":normalized.get("phoneNumber"),
                   "birth_date": normalized.get("birth_date"),
                   "questions":  normalized.get("questions", ""),
                   "answers":    normalized.get("answers", ""),
               },
               headers={"Content-Type": "application/json"},
           )
   except httpx.RequestError as e:
       return JSONResponse(status_code=502, content={"detail": {"adapter_error": "cannot_reach_backend", "msg": str(e)}})
   body_text = resp.text
   payload = {"backend_status": resp.status_code}
   if echo:
       payload["backend_body"] = body_text
   if 200 <= resp.status_code < 300:
       return JSONResponse(payload)
   # Bubble backend error as 502 so you can see it clearly
   return JSONResponse(status_code=502, content={"detail": payload})
