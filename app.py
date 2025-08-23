import json
import os
import logging
from typing import Any, Dict, List, Optional, Union
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field, EmailStr
# ------------------------------------------------------------------------------
# Logging
# ------------------------------------------------------------------------------
logger = logging.getLogger("adapter")
logging.basicConfig(level=logging.INFO)
# ------------------------------------------------------------------------------
# Config loader
# ------------------------------------------------------------------------------
def load_mapping(path: str) -> Dict[str, Any]:
   if not os.path.isfile(path):
       raise FileNotFoundError(f"Mapping file not found at {path}")
   with open(path, "r", encoding="utf-8") as f:
       cfg = json.load(f)
   # Build convenience reverse maps
   cfg.setdefault("idQtoQuestionId", {})          # e.g., {"Q1": 70955, ...} or nulls
   cfg.setdefault("aliases", {})                  # optional (string->numeric) aliases
   cfg.setdefault("options", {})                  # "70955": {"Self": 299504, ...}
   cfg.setdefault("enforceOptions", [])           # list of question keys to enforce coded choices
   cfg.setdefault("defaults", {})                 # fallback strings for user fields
   cfg.setdefault("questionOrder", [])            # order we expect/like (for UI, optional)
   # Build a normalized numeric set of enforced questions
   numeric_enforce: List[int] = []
   for k in cfg["enforceOptions"]:
       try:
           numeric_enforce.append(int(k))
       except ValueError:
           # Could be "Q1": convert via idQtoQuestionId / aliases
           qid = resolve_by_alias(k, cfg, allow_null=False)
           if qid is not None:
               numeric_enforce.append(qid)
   cfg["_enforce_numeric"] = set(numeric_enforce)
   # Pre-normalize options keys to int
   norm_opts: Dict[int, Dict[str, int]] = {}
   for k, v in cfg["options"].items():
       try:
           qid_int = int(k)
       except ValueError:
           qid_int = resolve_by_alias(k, cfg, allow_null=False) or None
       if qid_int is None:
           continue
       norm_opts[qid_int] = {str(ans_text): int(ans_id) for ans_text, ans_id in v.items()}
   cfg["_options_numeric"] = norm_opts
   # Build a question-text map if the config provides one (optional):
   # { "questionsText": { "Who are you purchasing for?": 70955, "Gender": 70956, ... } }
   # If you add this in mapping.config.json, the adapter will accept {"question": "..."} payloads.
   cfg.setdefault("questionsText", {})
   cfg["_questions_text_numeric"] = {
       qtxt.strip().lower(): int(qid) for qtxt, qid in cfg["questionsText"].items()
       if isinstance(qtxt, str)
   }
   return cfg

def resolve_by_alias(raw: Union[str, int], cfg: Dict[str, Any], allow_null: bool) -> Optional[int]:
   """
   Resolve 'Q1'/'q1'/'01'/'70955'/70955 -> numeric question id or None.
   """
   if raw is None:
       return None
   # Already numeric?
   try:
       return int(raw)
   except (TypeError, ValueError):
       pass
   rid = str(raw).strip()
   # explicit aliases
   aliases = cfg.get("aliases", {})
   if rid in aliases:
       try:
           return int(aliases[rid])
       except Exception:
           pass
   # idQtoQuestionId (Q1->70955)
   qmap = cfg.get("idQtoQuestionId", {})
   if rid in qmap:
       qv = qmap[rid]
       if qv is None and allow_null is False:
           return None
       try:
           return int(qv) if qv is not None else None
       except Exception:
           return None
   # tolerant 'Q01' / 'q1'
   if rid.upper().startswith("Q"):
       rnum = rid[1:].lstrip("0") or "0"
       key = f"Q{int(rnum)}"
       qv = qmap.get(key)
       if qv is None and allow_null is False:
           return None
       try:
           return int(qv) if qv is not None else None
       except Exception:
           return None
   # raw digits in string?
   if rid.isdigit():
       return int(rid)
   return None

def resolve_by_text(question_text: Optional[str], cfg: Dict[str, Any]) -> Optional[int]:
   if not question_text or not isinstance(question_text, str):
       return None
   return cfg["_questions_text_numeric"].get(question_text.strip().lower())

# ------------------------------------------------------------------------------
# Pydantic request/response models
# ------------------------------------------------------------------------------
class QAItem(BaseModel):
   # The UI may send id (int or "Q1") OR question (text). Only answer is always required.
   id: Optional[Union[int, str]] = Field(default=None)
   question: Optional[str] = Field(default=None)
   answer: str

class AdapterRequest(BaseModel):
   full_name: str
   email: EmailStr
   phone_number: str
   birth_date: str
   request_id: str
   result_key: str
   questionAnswers: List[QAItem]

class AdapterResponse(BaseModel):
   status: str
   request_id: str
   result_key: str
   normalized: List[Dict[str, Any]]

# ------------------------------------------------------------------------------
# FastAPI app
# ------------------------------------------------------------------------------
app = FastAPI(title="Ring Style Adapter")
MAPPING_PATH = os.getenv("MAPPING_PATH", "/home/site/wwwroot/mapping.config.json")
try:
   mapping_config = load_mapping(MAPPING_PATH)
   logger.info("Loaded mapping from %s", MAPPING_PATH)
   logger.info("Mapping version: %s", mapping_config.get("version", "n/a"))
   logger.info("questionOrder: %s", mapping_config.get("questionOrder"))
   logger.info("enforceOptions(numeric): %s", mapping_config.get("_enforce_numeric"))
except Exception as e:
   logger.exception("Failed to load mapping: %s", e)
   # Don't crash the worker; allow /health to answer while you fix mapping
   mapping_config = {
       "version": "FAILED-TO-LOAD",
       "idQtoQuestionId": {},
       "aliases": {},
       "options": {},
       "_options_numeric": {},
       "enforceOptions": [],
       "_enforce_numeric": set(),
       "defaults": {},
       "questionOrder": [],
       "questionsText": {},
       "_questions_text_numeric": {}
   }

@app.get("/health")
def health() -> Dict[str, str]:
   return {"status": "ok", "mapping_version": str(mapping_config.get("version", "n/a"))}

@app.get("/mapping")
def mapping_preview() -> Dict[str, Any]:
   # safe peek to confirm what’s loaded
   return {
       "version": mapping_config.get("version"),
       "questionOrder": mapping_config.get("questionOrder"),
       "enforceOptions": list(mapping_config.get("_enforce_numeric", [])),
       "options": mapping_config.get("_options_numeric")
   }

# ------------------------------------------------------------------------------
# Core: normalization and validation
# ------------------------------------------------------------------------------
def normalize_payload(req: AdapterRequest) -> List[Dict[str, Any]]:
   normalized: List[Dict[str, Any]] = []
   for qa in req.questionAnswers:
       # 1) prefer explicit ID
       qid = resolve_by_alias(qa.id, mapping_config, allow_null=False) if qa.id is not None else None
       # 2) or resolve by question text if provided (requires questionsText block in mapping)
       if qid is None and qa.question:
           qid = resolve_by_text(qa.question, mapping_config)
       if qid is None:
           # still not found -> construct a clear error
           missing = qa.id if qa.id is not None else (qa.question or "?")
           raise HTTPException(
               status_code=400,
               detail=f"No mapping found for question ID '{missing}'. Add it to mapping.config.json."
           )
       answer_text = qa.answer.strip() if isinstance(qa.answer, str) else str(qa.answer)
       # If this question is enforced (coded), the answer must exist in options
       if qid in mapping_config["_enforce_numeric"]:
           opts = mapping_config["_options_numeric"].get(qid, {})
           if answer_text not in opts:
               allowed = ", ".join(sorted(opts.keys())) if opts else "(no options configured)"
               raise HTTPException(
                   status_code=422,
                   detail=f"Answer '{answer_text}' is not valid for question {qid}. Allowed: {allowed}"
               )
           backend_answer_id = opts[answer_text]
           normalized.append({"id": qid, "answer_text": answer_text, "answer_id": backend_answer_id})
       else:
           # Free-text question: pass the wording through
           normalized.append({"id": qid, "answer_text": answer_text})
   return normalized

@app.post("/adapter", response_model=AdapterResponse)
async def adapter_endpoint(req: AdapterRequest, request: Request):
   # Normalize & validate
   norm = normalize_payload(req)
   # At this point you have:
   #  - numeric question IDs
   #  - answer_id for coded questions, answer_text for free text
   #
   # If you need to forward to your backend, build the exact payload shape here.
   # For demo we just echo the normalized list back.
   return AdapterResponse(
       status="ok",
       request_id=req.request_id,
       result_key=req.result_key,
       normalized=norm
   )
