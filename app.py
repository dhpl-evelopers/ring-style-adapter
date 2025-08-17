import os, json, time
from typing import Any, Dict, List, Optional, Tuple
from fastapi import FastAPI, HTTPException, Header
from fastapi.responses import JSONResponse, PlainTextResponse
import httpx
import xml.etree.ElementTree as ET
from pathlib import Path
APP_TITLE = "Ring Style Adapter (structured + keyed)"
app = FastAPI(title=APP_TITLE)
# ---------- env ----------
BACKEND_POST_URL   = os.getenv("BACKEND_POST_URL", "")   # e.g. https://.../createRequest
BACKEND_GET_URL    = os.getenv("BACKEND_GET_URL",   "")   # e.g. https://.../fetchResponse?response_id=
API_KEY_REQUIRED   = os.getenv("API_KEY_REQUIRED", "false").lower() == "true"
API_KEY            = os.getenv("API_KEY", "")
TIMEOUT_SECONDS    = float(os.getenv("TIMEOUT_SECONDS", "30"))
POLL_TRIES         = int(os.getenv("POLL_TRIES", "6"))
POLL_SLEEP_SEC     = float(os.getenv("POLL_SLEEP_SEC", "2.0"))
# ---------- load mapping ----------
CFG_PATH = Path(__file__).with_name("mapping.config.json")
CFG: Dict[str, Any] = json.loads(CFG_PATH.read_text(encoding="utf-8"))
Q_ORDER = CFG.get("questionOrder", [])
KEY2QID = CFG.get("keyToQuestionId", {})
OPTIONS = CFG.get("options", {})
DEFAULTS = CFG.get("defaults", {})
# ---------- helpers (input normalization) ----------
def normalize_payload(ui: Dict[str, Any]) -> Tuple[str, Dict[int, Any], Dict[str, str]]:
   """
   Accept any of these shapes and return:
     result_key: str
     answers_by_qid: {question_id: answer_value_or_option_id}
     contact: {"email","full_name","phone_number","date"}
   Supported UI shapes:
     A) {"answers":[{"question":"who","answer":"Self"}, ...], "result_key":"..."}
     B) {"answers":{"who":"Self","gender":"Male"}, "result_key":"..."}
     C) {"who":"Self","gender":"Male","result_key":"..."}
   """
   result_key = str(ui.get("result_key", "") or ui.get("resultKey", "") or ui.get("id", ""))
   if not result_key:
       raise HTTPException(400, detail="result_key is required")
   # contact fields (optional)
   contact = {
       "email": ui.get("email", DEFAULTS.get("email", "")),
       "full_name": ui.get("full_name", DEFAULTS.get("full_name", "")),
       "phone_number": ui.get("phone_number", DEFAULTS.get("phone_number", "")),
       "date": ui.get("date", DEFAULTS.get("date", "")),
   }
   answers_by_qid: Dict[int, Any] = {}
   if isinstance(ui.get("answers"), list):
       # shape A
       for item in ui["answers"]:
           k = str(item.get("question", "")).strip()
           v = item.get("answer")
           if not k:
               continue
           qid = KEY2QID.get(k)
           if qid:
               answers_by_qid[int(qid)] = v
   elif isinstance(ui.get("answers"), dict):
       # shape B
       for k, v in ui["answers"].items():
           qid = KEY2QID.get(k)
           if qid:
               answers_by_qid[int(qid)] = v
   else:
       # shape C (flat)
       for k, v in ui.items():
           if k in ("result_key","resultKey","id","email","full_name","phone_number","date"):
               continue
           qid = KEY2QID.get(k)
           if qid:
               answers_by_qid[int(qid)] = v
   if not answers_by_qid:
       raise HTTPException(400, detail="answers are required")
   # Translate textual options to option IDs if mapping exists
   for qid, val in list(answers_by_qid.items()):
       opt_map = OPTIONS.get(str(qid))
       if opt_map and isinstance(val, str):
           if val in opt_map:
               answers_by_qid[qid] = int(opt_map[val])
       # else: leave value as-is (free text or already an ID)
   return result_key, answers_by_qid, contact
# ---------- XML renderer (adjust tag names here if your backend differs) ----------
def render_xml(result_key: str, answers_by_qid: Dict[int, Any], contact: Dict[str, str]) -> str:
   """
   Default XML format:
<request>
<result_key>...</result_key>
<answers>
<answer>
<question_id>70955</question_id>
<value>299504</value>
</answer>
       ...
</answers>
<contact>
<email>...</email>
<full_name>...</full_name>
<phone_number>...</phone_number>
<date>...</date>
</contact>
</request>
   """
   root = ET.Element("request")
   rk = ET.SubElement(root, "result_key")
   rk.text = result_key
   answers_el = ET.SubElement(root, "answers")
   # keep order if provided in config, else natural order
   ordered_qids = [q for q in Q_ORDER if q in answers_by_qid] + [q for q in answers_by_qid if q not in Q_ORDER]
   for qid in ordered_qids:
       val = answers_by_qid[qid]
       a = ET.SubElement(answers_el, "answer")
       q = ET.SubElement(a, "question_id"); q.text = str(qid)
       v = ET.SubElement(a, "value");       v.text = str(val)
   c = ET.SubElement(root, "contact")
   for k in ("email", "full_name", "phone_number", "date"):
       t = ET.SubElement(c, k); t.text = contact.get(k, "")
   return ET.tostring(root, encoding="unicode")
# ---------- utility ----------
def assert_api_key(x_api_key: Optional[str]):
   if API_KEY_REQUIRED and (x_api_key or "") != (API_KEY or ""):
       raise HTTPException(401, detail="unauthorized")
# ---------- routes ----------
@app.get("/")
def root():
   return {
       "ok": True,
       "version": "0.1.1",
       "requires_api_key": API_KEY_REQUIRED,
       "has_backend_urls": bool(BACKEND_POST_URL) and bool(BACKEND_GET_URL)
   }
@app.get("/health")
def health():
   return {"ok": True}
# MAIN ENDPOINT: builds XML and (if backend URLs provided) calls backend
@app.post("/ingest")
def ingest(payload: Dict[str, Any], x_api_key: Optional[str] = Header(None)):
   assert_api_key(x_api_key)
   result_key, answers_qid, contact = normalize_payload(payload)
   xml_body = render_xml(result_key, answers_qid, contact)
   # If backend URLs are not configured, just return the XML (dry-run mode)
   if not (BACKEND_POST_URL and BACKEND_GET_URL):
       return JSONResponse({"mode": "dry_run", "xml": xml_body})
   try:
       with httpx.Client(timeout=TIMEOUT_SECONDS) as client:
           # 1) createRequest
           post_r = client.post(BACKEND_POST_URL, content=xml_body, headers={"Content-Type": "application/xml"})
           if post_r.status_code >= 400:
               raise HTTPException(502, detail={"backend_status": post_r.status_code, "body": post_r.text[:1000]})
           # naive response_id extraction: try JSON key or simple text
           response_id = None
           try:
               j = post_r.json()
               response_id = j.get("response_id") or j.get("id") or j.get("responseId")
           except Exception:
               pass
           if not response_id:
               # last resort: use result_key itself if backend correlates by that
               response_id = result_key
           # 2) poll fetchResponse
           body_text = ""
           status = None
           for _ in range(POLL_TRIES):
               get_r = client.get(f"{BACKEND_GET_URL}{response_id}")
               status = get_r.status_code
               if status == 200 and get_r.text.strip():
                   body_text = get_r.text
                   break
               time.sleep(POLL_SLEEP_SEC)
           if status and status >= 400:
               raise HTTPException(502, detail={"backend_status": status, "body": body_text[:1000] or "error"})
           return PlainTextResponse(body_text or "", status_code=200)
   except HTTPException:
       raise
   except Exception as e:
       raise HTTPException(502, detail=str(e))
# OPTIONAL: helpers for testing (keep or delete)
@app.post("/ingest/dry_run")
def ingest_dry_run(payload: Dict[str, Any], x_api_key: Optional[str] = Header(None)):
   assert_api_key(x_api_key)
   rk, a, c = normalize_payload(payload)
   return JSONResponse({"xml": render_xml(rk, a, c), "meta": {"result_key": rk}})
@app.post("/ingest/_debug")
def ingest_debug(payload: Dict[str, Any], x_api_key: Optional[str] = Header(None)):
   assert_api_key(x_api_key)
   rk, a, c = normalize_payload(payload)
   xml_body = render_xml(rk, a, c)
   return JSONResponse({"xml_sent": xml_body, "meta": {"result_key": rk, "answers": a, "contact": c}})
