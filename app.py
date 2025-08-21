import os
import json
import logging
import pathlib
from typing import Any, Dict, List, Tuple

import httpx
from fastapi import FastAPI, Body, Query, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

# -------------------------
# Logging
# -------------------------
log = logging.getLogger("adapter")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# -------------------------
# FastAPI Application
# -------------------------
app = FastAPI(title="Ring Style Adapter (flex → XML)", version="1.0.0", openapi_url="/openapi.json")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

# -------------------------
# Environment Configuration
# -------------------------
# Example:
#   BACKEND_POST_URL = https://projectaiapi-xxxx.azurewebsites.net/createRequest
#   BACKEND_GET_URL  = https://projectaiapi-xxxx.azurewebsites.net/fetchResponse?response_id=
BACKEND_POST_URL = os.getenv("BACKEND_POST_URL", "").strip()
BACKEND_GET_URL  = os.getenv("BACKEND_GET_URL", "").strip()

API_KEY_REQUIRED = os.getenv("API_KEY_REQUIRED", "false").lower() == "true"
API_KEY          = os.getenv("API_KEY", "")

# -------------------------
# Mapping Configuration
# -------------------------
CFG_PATH = pathlib.Path(__file__).with_name("mapping.config.json")
def _load_config() -> Dict[str, Any]:
    """Load mapping.config.json file."""
    try:
        log.info("Reading mapping config from %s", CFG_PATH)
        with open(CFG_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        log.error("Could not read mapping.config.json: %s", e)
        return {}
CONFIG        = _load_config()
XML_ROOT      = CONFIG.get("xml_root", "request")
FIELD_MAP     = {k.lower(): v for k, v in CONFIG.get("field_map", {}).items()}
ANS_KEY_TO_Q  = CONFIG.get("answer_key_to_question", {})
VARIANT_MAPS  = CONFIG.get("answer_key_to_question_variants", {})
ID_CANDIDATES = [s.lower() for s in CONFIG.get("result_key_field_candidates", ["request_id", "result_key"])]
DEFAULTS      = CONFIG.get("defaults", {})

# -------------------------
# Helper Functions
# -------------------------
def _require_backend():
    if not BACKEND_POST_URL:
        raise HTTPException(status_code=500, detail="BACKEND_POST_URL is not configured")

def _check_api_key(req: Request):
    if not API_KEY_REQUIRED:
        return
    if not API_KEY or req.headers.get("x-api-key") != API_KEY:
        raise HTTPException(status_code=401, detail="unauthorized")

def _flatten(data: Any) -> Dict[str, Any]:
    """Flatten a nested dictionary/list structure into a single-level dict of paths -> values."""
    flat: Dict[str, Any] = {}
    def visit(prefix: str, node: Any):
        if isinstance(node, dict):
            for k, v in node.items():
                new_prefix = f"{prefix}.{k}" if prefix else k
                visit(new_prefix, v)
        elif isinstance(node, list):
            flat[prefix] = node
            for i, v in enumerate(node):
                visit(f"{prefix}[{i}]", v)
        else:
            flat[prefix] = node
    visit("", data)
    return flat

def _find_result_key(data: Dict[str, Any]) -> str:
    """Search for an ID value in nested data by known field names."""
    for k, v in data.items():
        if isinstance(v, dict):
            result = _find_result_key(v)
            if result:
                return result
        else:
            if k.lower() in ID_CANDIDATES and v not in (None, ""):
                return str(v)
    return ""

def _extract_identity(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Extract and normalize identity fields (name, email, phone, birthdate, request_id) from input JSON."""
    out = dict(DEFAULTS)  # start with default empty values
    # Check common identity fields at top-level (case-insensitive)
    for input_key, output_key in [
        ("request_id", "request_id"),
        ("result_key", "request_id"),
        ("resultkey", "request_id"),
        ("response_id", "request_id"),
        ("name", "full_name"),
        ("full_name", "full_name"),
        ("email", "email"),
        ("phone", "phone_number"),
        ("phone_number", "phone_number"),
        ("phonenumber", "phone_number"),
        ("birth_date", "birthdate"),
        ("date", "birthdate"),
        ("dob", "birthdate"),
        ("birthdate", "birthdate"),
    ]:
        for k, v in raw.items():
            if k.lower() == input_key and v not in (None, "", []):
                out[output_key] = v
    # Map any nested or remaining fields via FIELD_MAP
    flat = _flatten(raw)
    for key, val in flat.items():
        leaf = key.split(".")[-1].split("[")[0].lower()
        if leaf in FIELD_MAP:
            target = FIELD_MAP[leaf]
            # Skip mapping to result_key to avoid duplicating request_id
            if target == "result_key":
                continue
            if val not in (None, ""):
                out[target] = val
    # Ensure request_id is set
    if not str(out.get("request_id", "")).strip():
        found = _find_result_key(raw)
        if found:
            out["request_id"] = found
    return out

def _extract_qna_from_answers_only(answers_list: List[Any]) -> Tuple[List[str], List[str], Dict[str, Any]]:
    """Handle input that provides only an answers list (no questions list)."""
    answers = [str(a) if a is not None else "" for a in answers_list]
    questions: List[str] = []
    context: Dict[str, Any] = {}
    if not answers:
        return questions, answers, context
    # Determine scenario (self vs others) from first answer value
    first_ans = answers[0].strip().lower()
    is_self = "self" in first_ans
    specific_variant = "self" if is_self else "others"
    gender_answer_present = False
    if not is_self:
        # Others scenario
        if len(answers) > 1:
            sec = answers[1].strip().lower()
            gender_answer_present = True
            if "male" in sec:
                specific_variant = "others_male"
            elif "female" in sec:
                specific_variant = "others_female"
            else:
                specific_variant = "others"
        else:
            specific_variant = "others"
            gender_answer_present = False
    else:
        # Self scenario
        if len(answers) > 1 and answers[1].strip().lower() in {"male", "female", "other"}:
            gender_answer_present = True
        # specific_variant remains "self"
    # Build questions list
    questions.append(ANS_KEY_TO_Q.get("purchase_for", "Who are you purchasing for?"))
    context["purchase_for"] = answers[0]
    if (not is_self) or gender_answer_present:
        questions.append(ANS_KEY_TO_Q.get("gender", "Gender"))
        context["gender"] = answers[1] if len(answers) > 1 else ""
    # Add context-specific questions
    variant_questions = VARIANT_MAPS.get(specific_variant, {})
    for key, q_text in variant_questions.items():
        questions.append(q_text)
    # Pad or trim answers list to match the questions count
    if len(questions) > len(answers):
        answers.extend([""] * (len(questions) - len(answers)))
    elif len(answers) > len(questions):
        answers = answers[:len(questions)]
    return questions, answers, context

def _extract_qna_from_keyed_form(raw: Dict[str, Any]) -> Tuple[List[str], List[str], Dict[str, Any]]:
    """Handle input provided as a dictionary of question keys to answers (no explicit questions list)."""
    questions: List[str] = []
    answers: List[str] = []
    context: Dict[str, Any] = {}
    # Determine scenario from 'purchase_for' value
    purchase_for_val = str(raw.get("purchase_for", "")).strip().lower()
    is_self = "self" in purchase_for_val
    specific_variant = "self" if is_self else "others"
    if not is_self:
        gender_val = str(raw.get("gender", "")).strip().lower()
        if gender_val:
            if "male" in gender_val:
                specific_variant = "others_male"
            elif "female" in gender_val:
                specific_variant = "others_female"
            else:
                specific_variant = "others"
        else:
            specific_variant = "others"
    # Build questions and answers lists
    questions.append(ANS_KEY_TO_Q.get("purchase_for", "Who are you purchasing for?"))
    answers.append(str(raw.get("purchase_for", "")))
    context["purchase_for"] = raw.get("purchase_for", "")
    if not is_self or ("gender" in raw):
        questions.append(ANS_KEY_TO_Q.get("gender", "Gender"))
        answers.append(str(raw.get("gender", "")))
        context["gender"] = raw.get("gender", "")
    variant_map = VARIANT_MAPS.get(specific_variant, {})
    for key, q_text in variant_map.items():
        questions.append(q_text)
        answers.append(str(raw.get(key, "")) if raw.get(key) is not None else "")
        if key in raw:
            context[key] = raw[key]
    # Pad or trim answers to match questions count
    if len(questions) > len(answers):
        answers.extend([""] * (len(questions) - len(answers)))
    elif len(answers) > len(questions):
        answers = answers[:len(questions)]
    return questions, answers, context

def _ensure_qna_lists(raw: Dict[str, Any]) -> Tuple[List[str], List[str], Dict[str, Any]]:
    """Normalize the input into parallel questions and answers lists (and a context dictionary)."""
    # 1) Already have questions and answers lists
    if isinstance(raw.get("questions"), list) and isinstance(raw.get("answers"), list):
        qs = [str(q) for q in raw["questions"]]
        ans = [str(a) if a is not None else "" for a in raw["answers"]]
        if len(qs) > len(ans):
            ans += [""] * (len(qs) - len(ans))
        elif len(ans) > len(qs):
            qs += [""] * (len(ans) - len(qs))
        return qs, ans, {}
    # 2) Only answers list is provided
    if isinstance(raw.get("answers"), list) and not raw.get("questions"):
        return _extract_qna_from_answers_only(raw["answers"])
    # 3) Keyed form (purchase_for and related keys present)
    if raw.get("purchase_for") is not None:
        return _extract_qna_from_keyed_form(raw)
    # 4) Quizell-style questionAnswers array
    qa_list = raw.get("questionAnswers") or raw.get("question_answers") or raw.get("questionanswers")
    if isinstance(qa_list, list):
        qs, ans = [], []
        for item in qa_list:
            if not isinstance(item, dict):
                continue
            q_text = str(item.get("question", "")).strip()
            if q_text.lower().startswith("q") and "." in q_text:
                try:
                    _prefix, q_text = q_text.split(" ", 1)
                    q_text = q_text.lstrip(". ")
                except Exception:
                    pass
            ans_val = None
            sel = item.get("selectedOption") or item.get("selected_option")
            if isinstance(sel, dict):
                ans_val = sel.get("value") or sel.get("label")
            if ans_val is None:
                ans_val = item.get("answer") or item.get("value")
            if q_text and ans_val is not None:
                qs.append(q_text)
                ans.append(str(ans_val).strip())
        if qs and ans:
            return qs, ans, {}
    # 5) Fallback: return empty lists if format not recognized
    return [], [], {}

def _build_xml_payload(raw: Dict[str, Any]) -> bytes:
    """Convert the normalized data into XML format for the backend request."""
    ident = _extract_identity(raw)
    questions, answers, _ = _ensure_qna_lists(raw)
    if not questions or not answers:
        raise HTTPException(status_code=400, detail="No question-answer data provided")
    import xml.etree.ElementTree as ET
    root = ET.Element(XML_ROOT)
    # Identity fields
    ET.SubElement(root, "request_id").text   = str(ident.get("request_id", "")) if ident.get("request_id") is not None else ""
    ET.SubElement(root, "full_name").text    = str(ident.get("full_name", "")) if ident.get("full_name") is not None else ""
    ET.SubElement(root, "email").text        = str(ident.get("email", "")) if ident.get("email") is not None else ""
    ET.SubElement(root, "phone_number").text = str(ident.get("phone_number", "")) if ident.get("phone_number") is not None else ""
    ET.SubElement(root, "birthdate").text    = str(ident.get("birthdate", "")) if ident.get("birthdate") is not None else ""
    # Question-answer pairs
    qa_section = ET.SubElement(root, "questionAnswers")
    for q_text, a_text in zip(questions, answers):
        qa = ET.SubElement(qa_section, "questionAnswer")
        ET.SubElement(qa, "question").text = str(q_text) if q_text is not None else ""
        ET.SubElement(qa, "answer").text   = str(a_text) if a_text is not None else ""
    xml_bytes = ET.tostring(root, encoding="utf-8", xml_declaration=True)
    return xml_bytes

# -------------------------
# API Routes
# -------------------------
@app.get("/")
def root():
    return {"ok": True, "service": "Ring Style Adapter (flex → XML)"}

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/ingest")
async def ingest(
    payload: Dict[str, Any] = Body(..., description="UI payload in simplified JSON format"),
    preview: bool = Query(False, description="If true, return the transformed XML without calling the backend"),
    echo: bool = Query(False, description="If true, include backend response details in the output"),
    req: Request = None,
):
    """Adapter endpoint: accepts UI JSON, transforms to XML, and forwards to /createRequest backend."""
    _check_api_key(req)
    _require_backend()
    try:
        ident = _extract_identity(payload)
        xml_payload = _build_xml_payload(payload)
        if preview:
            return {"sent_xml": xml_payload.decode("utf-8")}
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(BACKEND_POST_URL, data=xml_payload, headers={"Content-Type": "application/xml; charset=utf-8"})
            status = resp.status_code
            body_text = resp.text
            try:
                body_json = resp.json()
            except Exception:
                body_json = None
        if echo:
            return {
                "sent_xml": xml_payload.decode("utf-8"),
                "backend_status": status,
                "backend_headers": {"content-type": resp.headers.get("content-type", "")},
                "backend_body": body_json if body_json is not None else body_text
            }
        if status >= 400:
            log.error("Backend /createRequest returned %d: %s", status, body_text)
            error_content = body_json if body_json is not None else {"error": body_text}
            return JSONResponse(status_code=status, content=error_content)
        return {"status": "ok", "request_id": str(ident.get("request_id", ""))}
    except HTTPException:
        raise
    except Exception as e:
        log.exception("Adapter ingest failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/fetchResponse")
async def fetch_response(response_id: str):
    """Forward a GET request to the backend to retrieve the result for the given response_id."""
    if not BACKEND_GET_URL:
        raise HTTPException(status_code=501, detail="BACKEND_GET_URL is not configured")
    url = BACKEND_GET_URL
    if not url.endswith("="):
        url = url + ("&response_id=" if "?" in url else "?response_id=")
    url += response_id
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(url)
        try:
            data = resp.json()
        except Exception:
            data = {"body": resp.text}
    return JSONResponse(status_code=resp.status_code, content=data)
