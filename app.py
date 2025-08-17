import os, json, logging
from typing import Any, Dict, List, Tuple
import httpx
from fastapi import FastAPI, Request, HTTPException, Body, Query
from fastapi.responses import JSONResponse, PlainTextResponse
from dicttoxml import dicttoxml
# ---------- Logging ----------
log = logging.getLogger("adapter")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
# ---------- FastAPI ----------
app = FastAPI(title="Ring Style Adapter", version="1.0.0")
# ---------- Env (set these in Azure App Service > Configuration) ----------
BACKEND_POST_URL = os.getenv("BACKEND_POST_URL", "").strip()   # e.g. https://<api>/createRequest
BACKEND_GET_URL  = os.getenv("BACKEND_GET_URL", "").strip()    # e.g. https://<api>/fetchResponse?response_id=
API_KEY_REQUIRED = os.getenv("API_KEY_REQUIRED", "false").lower() == "true"
API_KEY          = os.getenv("API_KEY", "")
# ---------- Mapping config ----------
CFG_PATH = os.path.join(os.path.dirname(__file__), "mapping.config.json")
with open(CFG_PATH, "r", encoding="utf-8") as f:
   CFG = json.load(f)
XML_ROOT = CFG.get("xml_root", "request")
FIELD_MAP: Dict[str, str] = {k.lower(): v for k, v in CFG.get("field_map", {}).items()}
ANS_KEY_TO_Q = CFG.get("answer_key_to_question", {})
RESULT_KEY_CANDIDATES = [s.lower() for s in CFG.get("result_key_field_candidates", ["result_key", "response_id"])]
DEFAULTS = CFG.get("defaults", {})
# ---------- helpers ----------
def _require_backend_urls():
   if not BACKEND_POST_URL or not BACKEND_GET_URL:
       raise HTTPException(status_code=500, detail="BACKEND_POST_URL / BACKEND_GET_URL are not configured")
def _check_api_key(req: Request):
   if not API_KEY_REQUIRED:
       return
   if not API_KEY or req.headers.get("x-api-key") != API_KEY:
       raise HTTPException(status_code=401, detail="unauthorized")
def _find_result_key(d: Dict[str, Any]) -> str:
   # Search common places for a result/response key
   for k in RESULT_KEY_CANDIDATES:
       for key in d.keys():
           if key.lower() == k:
               return str(d[key])
   # sometimes nested
   for v in d.values():
       if isinstance(v, dict):
           rk = _find_result_key(v)
           if rk:
               return rk
   return ""
def _gather_fields(d: Dict[str, Any]) -> Dict[str, Any]:
   """
   Take any free-form JSON and map/normalize to backend flat fields using FIELD_MAP + DEFAULTS.
   """
   out = dict(DEFAULTS)  # start with defaults
   # collect all flat candidates (flatten arbitrarily)
   flat: Dict[str, Any] = {}
   def visit(prefix: str, node: Any):
       if isinstance(node, dict):
           for k, v in node.items():
               visit(f"{prefix}.{k}" if prefix else k, v)
       elif isinstance(node, list):
           # store lists as JSON strings for fields (answers handled elsewhere)
           flat[prefix] = json.dumps(node, ensure_ascii=False)
       else:
           flat[prefix] = node
   visit("", d)
   # map by FIELD_MAP (case-insensitive on incoming keys; use last segment)
   for inkoming_key, value in flat.items():
       lk = inkoming_key.split(".")[-1].lower()
       if lk in FIELD_MAP:
           out[FIELD_MAP[lk]] = value
       else:
           # if same tag name already suitable (rare), keep as-is
           if lk in DEFAULTS:
               out[lk] = value
   return out
def _normalize_answers(d: Dict[str, Any]) -> List[Dict[str, str]]:
   """
   Produce backend 'answers' list in shape: [{ "question": "...", "answer": "..." }, ...]
   Accepts:
     - already-structured answers array
     - key/value dict where keys are mapped via ANS_KEY_TO_Q
     - top-level keys matching ANS_KEY_TO_Q
   """
   # Case 1: already an array of objects
   if isinstance(d.get("answers"), list) and d["answers"] and isinstance(d["answers"][0], dict):
       norm = []
       for it in d["answers"]:
           q = it.get("question") or it.get("q") or it.get("label")
           a = it.get("answer") or it.get("a") or it.get("value")
           if q is not None and a is not None:
               norm.append({"question": str(q), "answer": str(a)})
       if norm:
           return norm
   # Case 2: a dict of answers like {"who":"Self","gender":"Male"}
   guess: Dict[str, Any] = {}
   if isinstance(d.get("answers"), dict):
       guess = d["answers"]
   else:
       # look for top-level keys that match ANS_KEY_TO_Q
       for key in ANS_KEY_TO_Q.keys():
           if key in d:
               guess[key] = d[key]
   arr: List[Dict[str, str]] = []
   for k, v in guess.items():
       q = ANS_KEY_TO_Q.get(k, k)
       if isinstance(v, (dict, list)):
           v = json.dumps(v, ensure_ascii=False)
       arr.append({"question": str(q), "answer": str(v)})
   return arr
def build_backend_payload(ui_json: Dict[str, Any]) -> Tuple[Dict[str, Any], str]:
   """
   Build the dict that the backend expects (before XML conversion).
   The XML will look like:
<request>
<name>...</name>
<email>...</email>
<phoneNumber>...</phoneNumber>
<birth_date>...</birth_date>
<result_key>...</result_key>
<answers>
<item>
<question>...</question>
<answer>...</answer>
</item>
         ...
</answers>
</request>
   """
   fields = _gather_fields(ui_json)
   answers = _normalize_answers(ui_json)
   result_key = _find_result_key(ui_json)
   backend_dict: Dict[str, Any] = {**fields}
   if result_key:
       backend_dict["result_key"] = result_key
   if answers:
       backend_dict["answers"] = answers
   # fallbacks to ensure keys exist even if empty
   for k in ["name", "email", "phoneNumber", "birth_date"]:
       backend_dict.setdefault(k, DEFAULTS.get(k, ""))
   return backend_dict, result_key
# ---------- routes ----------
@app.get("/")
def root():
   return {
       "ok": True,
       "requires_api_key": API_KEY_REQUIRED,
       "post_url_set": bool(BACKEND_POST_URL),
       "get_url_set": bool(BACKEND_GET_URL),
       "xml_root": XML_ROOT
   }
@app.get("/health")
def health():
   return {"ok": True}
@app.post("/ingest")
async def ingest(
   payload: Dict[str, Any] = Body(..., description="Flexible UI JSON"),
   request: Request = None,
   preview: bool = Query(False, description="If true, return generated XML without calling backend"),
   echo: bool = Query(False, description="If true, return sent XML + backend status/body")
):
   _require_backend_urls()
   _check_api_key(request)
   backend_dict, _ = build_backend_payload(payload)
   # Convert dict -> XML
   xml_bytes = dicttoxml(backend_dict, custom_root=XML_ROOT, attr_type=False)
   xml_text = xml_bytes.decode("utf-8", errors="ignore")
   if preview:
       # Show exactly what will be sent
       return PlainTextResponse(xml_text, media_type="application/xml")
   # Send to backend
   try:
       async with httpx.AsyncClient(timeout=30) as client:
           resp = await client.post(
               BACKEND_POST_URL,
               content=xml_bytes,
               headers={
                   "Content-Type": "application/xml; charset=utf-8",
                   "Accept": "application/xml,application/json,text/plain,*/*"
               }
           )
   except httpx.RequestError as e:
       log.exception("Backend unreachable")
       raise HTTPException(status_code=502, detail={"adapter_error": "cannot_reach_backend", "msg": str(e)})
   ct = (resp.headers.get("content-type") or "").lower()
   body_text = resp.text
   if echo:
       return JSONResponse({
           "sent_xml": xml_text,
           "backend_status": resp.status_code,
           "backend_content_type": ct,
           "backend_body": body_text
       }, status_code=resp.status_code if 200 <= resp.status_code < 300 else 502)
   if 200 <= resp.status_code < 300:
       # pass through success
       return PlainTextResponse(
           body_text,
           media_type="application/xml" if "xml" in ct else "application/json"
       )
   # bubble backend failure with its body (don’t hide as generic 500)
   return JSONResponse(
       status_code=502,
       content={"detail": {"backend_status": resp.status_code, "body": body_text}}
   )
@app.get("/result")
async def result(response_id: str, request: Request = None):
   _require_backend_urls()
   _check_api_key(request)
   url = BACKEND_GET_URL
   # allow both templated and query forms
   if "{response_id}" in url:
       url = url.replace("{response_id}", response_id)
   elif "response_id=" not in url:
       join = "&" if "?" in url else "?"
       url = f"{url}{join}response_id={response_id}"
   try:
       async with httpx.AsyncClient(timeout=30) as client:
           r = await client.get(url, headers={"Accept": "application/xml,application/json,text/plain,*/*"})
   except httpx.RequestError as e:
       raise HTTPException(status_code=502, detail={"adapter_error": "cannot_reach_backend", "msg": str(e)})
   return PlainTextResponse(r.text, media_type=(r.headers.get("content-type") or "text/plain"))
