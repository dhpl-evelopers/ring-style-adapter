import os
import json
import logging
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Tuple
import httpx
from fastapi import FastAPI, Body, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
# Initialize logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("adapter")
# Initialize FastAPI app
app = FastAPI(title="Ring Style Adapter", version="1.0.0")
app.add_middleware(
   CORSMiddleware,
   allow_origins=["*"], allow_credentials=True,
   allow_methods=["*"], allow_headers=["*"],
)
# Load configuration mapping file
CFG_PATH = os.path.join(os.path.dirname(__file__), "mapping.config.json")
try:
   with open(CFG_PATH, "r", encoding="utf-8") as f:
       CONFIG = json.load(f)
except Exception as e:
   log.error(f"Failed to load mapping config: {e}")
   CONFIG = {}
# Extract mapping config details
XML_ROOT               = CONFIG.get("xml_root", "request")
FIELD_MAP: Dict[str,str]= {k.lower(): v for k,v in CONFIG.get("field_map", {}).items()}
ANS_KEY_TO_Q: Dict[str,str] = CONFIG.get("answer_key_to_question", {})
ANS_KEY_TO_Q_VARIANTS: Dict[str, Dict[str,str]] = CONFIG.get("answer_key_to_question_variants", {})
RESULT_KEY_CANDIDATES  = [s.lower() for s in CONFIG.get("result_key_field_candidates", [])]
DEFAULTS               = CONFIG.get("defaults", {})
# Backend endpoint from environment
BACKEND_POST_URL = os.getenv("BACKEND_POST_URL", "").strip()
if not BACKEND_POST_URL:
   log.warning("BACKEND_POST_URL is not set – requests will fail without a valid backend URL.")
def _flatten(data: Any) -> Dict[str, Any]:
   """Flatten nested JSON structures into dot notation (to assist field mapping)."""
   flat: Dict[str, Any] = {}
   def visit(prefix: str, node: Any):
       if isinstance(node, dict):
           for k, v in node.items():
               new_pref = f"{prefix}.{k}" if prefix else k
               visit(new_pref, v)
       elif isinstance(node, list):
           flat[prefix] = node
           for i, v in enumerate(node):
               visit(f"{prefix}[{i}]", v)
       else:
           flat[prefix] = node
   visit("", data)
   return flat
def _find_result_key(obj: Dict[str, Any]) -> str:
   """Find a result/request ID in nested fields if not directly present."""
   for k, v in obj.items():
       if k.lower() in RESULT_KEY_CANDIDATES:
           return str(v)
   for v in obj.values():
       if isinstance(v, dict):
           rk = _find_result_key(v)
           if rk:
               return rk
   return ""
def _extract_identity(raw: Dict[str, Any]) -> Dict[str, Any]:
   """
   Extract identity/metadata fields (name, email, phone, birth_date, request_id)
   from the input, mapping to the backend field names using FIELD_MAP and defaults.
   """
   data = {**DEFAULTS}  # start with default empty values for each field
   flat = _flatten(raw)
   # Priority: explicit top-level fields
   for cand_in, cand_out in [
       ("request_id", "request_id"), ("result_key", "request_id"), ("response_id", "request_id"),
       ("name", "name"), ("full_name", "name"),
       ("email", "email"),
       ("phone", "phoneNumber"), ("phone_number", "phoneNumber"), ("phonenumber", "phoneNumber"),
       ("birth_date", "birth_date"), ("date", "birth_date"), ("dob", "birth_date")
   ]:
       for key, val in raw.items():
           if key.lower() == cand_in and val not in (None, "", []):
               data[cand_out] = val
   # Secondary: generic field mapping for any nested fields
   for flat_key, val in flat.items():
       leaf = flat_key.split(".")[-1].split("[")[0].lower()
       if leaf in FIELD_MAP and val not in (None, "", []):
           data[ FIELD_MAP[leaf] ] = val
   # Lastly, ensure request_id is captured if present under any nested field name
   if not data.get("request_id"):
       found = _find_result_key(raw)
       if found:
           data["request_id"] = found
   return data
def _extract_qna_from_quiz(raw: Dict[str, Any]) -> List[Tuple[str, str]]:
   """
   Extract question-answer pairs from a Quizell-style input:
   e.g. raw["questionAnswers"] = [ {"question": "...", "selectedOption": {"value": "..."}}, ... ]
   """
   pairs: List[Tuple[str, str]] = []
   qa_list = raw.get("questionAnswers") or raw.get("question_answers") or raw.get("questionanswers")
   if isinstance(qa_list, list):
       for item in qa_list:
           if isinstance(item, dict):
               q_text = str(item.get("question", "")).strip()
               # The answer may be under selectedOption or directly in the object
               ans_obj = item.get("selectedOption") or item.get("selected_option") or {}
               a_val = ans_obj.get("value") or ans_obj.get("label") or item.get("answer") or item.get("value")
               if q_text and a_val is not None:
                   pairs.append((q_text, str(a_val).strip()))
   return pairs
def _extract_qna_from_keys(raw: Dict[str, Any]) -> List[Tuple[str, str]]:
   """
   Extract question-answer pairs from a simple key-value input (e.g. {"purchase_for": "self", ...}).
   Uses ANS_KEY_TO_Q for static mappings and ANS_KEY_TO_Q_VARIANTS for context-specific phrasing.
   """
   pairs: List[Tuple[str, str]] = []
   # Determine variant context (self or others) for pronoun-based question text
   pf = str(raw.get("purchase_for", "")).strip().lower()
   gd = str(raw.get("gender", "")).strip().lower()
   variant_key = None
   if pf:
       if pf == "self":
           variant_key = "self"
       elif pf in ("others", "other"):
           if gd == "male":
               variant_key = "others_male"
           elif gd == "female":
               variant_key = "others_female"
           else:
               variant_key = "others"
   variant_map = ANS_KEY_TO_Q_VARIANTS.get(variant_key, {})
   # Map each provided key to its question text
   for key, val in raw.items():
       if val in (None, "", []):
           continue  # skip empty answers
       k = key.lower()
       if k in ANS_KEY_TO_Q:
           # Base question text mapping (override with variant if available)
           q_text = variant_map.get(k, ANS_KEY_TO_Q[k])
           pairs.append((q_text, str(val)))
       elif k in variant_map:
           # Some keys (like personality questions) only exist in variant mapping
           q_text = variant_map[k]
           pairs.append((q_text, str(val)))
   return pairs
def _map_answers_list(ans_list: List[Any]) -> List[Tuple[str, str]]:
   """
   Convert a raw answers-only list (no questions provided) into a list of (question, answer) pairs
   using the mapping config. This infers question text by position and context.
   """
   pairs: List[Tuple[str, str]] = []
   if not isinstance(ans_list, list) or not ans_list:
       return pairs
   # Normalize answer values to strings
   answers = ["" if a is None else str(a) for a in ans_list]
   # Determine context variant based on first two answers (purchase_for and gender)
   pf_val = answers[0].strip().lower() if len(answers) > 0 else ""
   gd_val = answers[1].strip().lower() if len(answers) > 1 else ""
   variant_key = None
   if pf_val == "self":
       variant_key = "self"
   elif pf_val in ("others", "other"):
       if gd_val == "male":
           variant_key = "others_male"
       elif gd_val == "female":
           variant_key = "others_female"
       else:
           variant_key = "others"
   variant_map = ANS_KEY_TO_Q_VARIANTS.get(variant_key, {})
   # Q1: purchase_for
   if len(answers) > 0 and "purchase_for" in ANS_KEY_TO_Q:
       pairs.append((ANS_KEY_TO_Q["purchase_for"], answers[0]))
   # Q2: gender
   if len(answers) > 1 and "gender" in ANS_KEY_TO_Q:
       pairs.append((ANS_KEY_TO_Q["gender"], answers[1]))
   # Subsequent questions based on scenario
   # Define the expected question keys order after the first two, for self vs others
   self_sequence   = ["profession", "occasion", "purpose", "day", "weekend",
                      "work_dress", "social_dress", "line", "painting", "word", "plan"]
   others_sequence = ["relation", "profession", "occasion", "purpose", "day", "weekend",
                      "work_dress", "social_dress", "line", "painting", "word", "plan"]
   seq_keys = self_sequence if variant_key == "self" else (others_sequence if variant_key in ("others_female","others_male","others") else [])
   # Map each remaining answer to the next key in the sequence
   for idx in range(2, len(answers)):
       if idx-2 >= len(seq_keys):
           break  # no more expected question keys
       q_key = seq_keys[idx-2]
       # Get question text from variant mapping if available, else static mapping
       q_text = variant_map.get(q_key) or ANS_KEY_TO_Q.get(q_key) or q_key
       pairs.append((q_text, answers[idx]))
   return pairs
def _ensure_qna_pairs(raw: Dict[str, Any]) -> Tuple[List[str], List[str]]:
   """
   Determine the ordered questions and answers lists from the raw input,
   regardless of the input format.
   """
   # 1. Already provided question & answers lists
   if isinstance(raw.get("questions"), list) and isinstance(raw.get("answers"), list):
       questions = [str(q) for q in raw["questions"]]
       answers   = ["" if a is None else str(a) for a in raw["answers"]]
       return questions, answers
   # 2. Quiz-style array of objects
   qa_pairs = _extract_qna_from_quiz(raw)
   if qa_pairs:
       questions = [q for (q, _) in qa_pairs]
       answers   = [a for (_, a) in qa_pairs]
       return questions, answers
   # 3. Raw answers-only list
   if isinstance(raw.get("answers"), list) and not raw.get("questions"):
       ans_list = raw.get("answers")
       pairs = _map_answers_list(ans_list)
       if pairs:
           questions = [q for (q, _) in pairs]
           answers   = [a for (_, a) in pairs]
           return questions, answers
   # 4. Key-value form (if not using "answers" field)
   key_pairs = _extract_qna_from_keys(raw)
   if key_pairs:
       questions = [q for (q, _) in key_pairs]
       answers   = [a for (_, a) in key_pairs]
       return questions, answers
   # 5. Fallback: no quiz content
   return [], []
def _build_xml_payload(raw: Dict[str, Any]) -> str:
   """
   Build XML string payload from the raw input, according to backend format.
   """
   ident = _extract_identity(raw)
   questions, answers = _ensure_qna_pairs(raw)
   # Join questions and answers with comma+space as required by backend [oai_citation:5‡file-7fyvxgwjpxymch8kmy1r84](file://file-7FYvXgwjpxymch8KMY1r84#:~:text=,join%28answers)
   q_str = ", ".join(questions)
   a_str = ", ".join(answers)
   # Construct XML document
   root = ET.Element(XML_ROOT)
   ET.SubElement(root, "questions").text    = q_str
   ET.SubElement(root, "answers").text      = a_str
   ET.SubElement(root, "request_id").text   = str(ident.get("request_id", ""))
   ET.SubElement(root, "name").text         = str(ident.get("name", ""))
   ET.SubElement(root, "email").text        = str(ident.get("email", ""))
   ET.SubElement(root, "phoneNumber").text  = str(ident.get("phoneNumber", ""))
   ET.SubElement(root, "birth_date").text   = str(ident.get("birth_date", ""))
   # Convert to string
   return ET.tostring(root, encoding="unicode")
@app.post("/ingest")
async def ingest(payload: Dict[str, Any] = Body(..., description="Flexible quiz input"), req: Request = None):
   """Accepts quiz input in various formats, forwards as XML to backend, and returns the backend response."""
   # Ensure backend URL is set
   if not BACKEND_POST_URL:
       raise HTTPException(status_code=500, detail="Backend POST URL not configured")
   # Build XML payload
   try:
       xml_body = _build_xml_payload(payload)
   except Exception as err:
       log.error(f"Failed to build XML payload: {err}")
       raise HTTPException(status_code=400, detail=f"Invalid input format: {err}")
   # Send to backend
   try:
       async with httpx.AsyncClient(timeout=30.0) as client:
           resp = await client.post(BACKEND_POST_URL, content=xml_body, headers={"Content-Type": "application/xml"})
   except Exception as err:
       log.error(f"Backend request error: {err}")
       # Return a 502 Bad Gateway for upstream errors
       raise HTTPException(status_code=502, detail=f"Failed to reach backend: {err}")
   # Return backend response directly
   return Response(content=resp.text, status_code=resp.status_code, media_type=resp.headers.get("content-type") or "text/plain")
