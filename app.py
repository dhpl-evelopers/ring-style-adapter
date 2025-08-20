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
# FastAPI App Initialization
# -------------------------
app = FastAPI(title="Ring Style Adapter (flex → JSON)", version="1.0.0", openapi_url="/openapi.json")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

# -------------------------
# Environment Variables
# -------------------------
# Expected environment variables (with examples):
#   BACKEND_POST_URL = https://projectaiapi-xxxx.azurewebsites.net/createRequestJSON
#   BACKEND_GET_URL  = https://projectaiapi-xxxx.azurewebsites.net/fetchResponse?response_id=
BACKEND_POST_URL = os.getenv("BACKEND_POST_URL", "").strip()
BACKEND_GET_URL  = os.getenv("BACKEND_GET_URL", "").strip()

API_KEY_REQUIRED = os.getenv("API_KEY_REQUIRED", "false").lower() == "true"
API_KEY          = os.getenv("API_KEY", "")

# -------------------------
# Load Config (mapping.config.json)
# -------------------------
CFG_PATH = pathlib.Path(__file__).with_name("mapping.config.json")

def _load_config() -> Dict[str, Any]:
    """Load mapping.config.json from the same folder as app.py"""
    try:
        log.info("Reading mapping config from %s", CFG_PATH)
        with open(CFG_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        log.error("Could not read mapping.config.json: %s", e)
        return {}

CONFIG                 = _load_config()
XML_ROOT               = CONFIG.get("xml_root", "request")
FIELD_MAP: Dict[str, str] = {k.lower(): v for k, v in CONFIG.get("field_map", {}).items()}
ANS_KEY_TO_Q: Dict[str, str] = {k.lower(): v for k, v in CONFIG.get("answer_key_to_question", {}).items()}
ANS_KEY_TO_Q_VARIANTS: Dict[str, Dict[str, str]] = CONFIG.get("answer_key_to_question_variants", {})
RESULT_KEY_CANDIDATES  = [s.lower() for s in CONFIG.get("result_key_field_candidates", ["result_key", "response_id", "resultkey", "responsekey"])]
DEFAULTS               = CONFIG.get("defaults", {})

# -------------------------
# Helper Functions
# -------------------------
def _require_backend():
    """Ensure the backend URL is configured, otherwise raise 500 error."""
    if not BACKEND_POST_URL:
        raise HTTPException(status_code=500, detail="BACKEND_POST_URL is not configured")

def _check_api_key(req: Request):
    """If API key auth is required, verify the incoming header."""
    if not API_KEY_REQUIRED:
        return
    if not API_KEY or req.headers.get("x-api-key") != API_KEY:
        raise HTTPException(status_code=401, detail="unauthorized")

def _flatten(node: Any) -> Dict[str, Any]:
    """
    Flatten a nested dictionary or list structure into a single-level dict of paths to values.
    e.g., {"a": {"b": 1}} -> {"a.b": 1}
    """
    flat: Dict[str, Any] = {}
    def visit(prefix: str, item: Any):
        if isinstance(item, dict):
            for k, v in item.items():
                new_prefix = f"{prefix}.{k}" if prefix else k
                visit(new_prefix, v)
        elif isinstance(item, list):
            flat[prefix] = item
            for i, v in enumerate(item):
                visit(f"{prefix}[{i}]", v)
        else:
            flat[prefix] = item
    visit("", node)
    return flat

def _find_result_key(data: Dict[str, Any]) -> str:
    """Recursively search for any result-key-like field in the data."""
    # Check top-level keys first
    for k, v in data.items():
        if k.lower() in RESULT_KEY_CANDIDATES:
            return str(v)
    # Search nested dictionaries
    for v in data.values():
        if isinstance(v, dict):
            rk = _find_result_key(v)
            if rk:
                return rk
    return ""

def _extract_identity(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract and map identity fields (name, email, phone, birth date, request_id) from a flexible payload.
    Uses FIELD_MAP and known synonyms to populate a dict of identity info.
    """
    out: Dict[str, Any] = dict(DEFAULTS)  # start with default empty values
    flat = _flatten(raw)

    # 1. Check common field names at top-level (including synonyms)
    for cand_in, cand_out in [
        ("request_id", "request_id"), ("result_key", "request_id"), ("resultkey", "request_id"), ("response_id", "request_id"),
        ("name", "name"), ("full_name", "name"),
        ("email", "email"),
        ("phone", "phoneNumber"), ("phone_number", "phoneNumber"), ("phonenumber", "phoneNumber"),
        ("birth_date", "birth_date"), ("date", "birth_date"), ("dob", "birth_date")
    ]:
        for k, v in raw.items():
            if k.lower() == cand_in and v not in (None, "", []):
                out[cand_out] = v

    # 2. Use generic FIELD_MAP on all flattened keys (to catch nested or other cases)
    for path, val in flat.items():
        leaf = path.split(".")[-1].split("[")[0].lower()
        if leaf in FIELD_MAP and val not in (None, "", []):
            out[FIELD_MAP[leaf]] = val

    # 3. If request_id still missing, search nested structures for any result key
    if not out.get("request_id"):
        rk = _find_result_key(raw)
        if rk:
            out["request_id"] = rk

    return out

def _extract_qna_from_quizell(raw: Dict[str, Any]) -> List[Tuple[str, str]]:
    """
    Extract Q&A pairs from a Quizell-style payload:
      raw["questionAnswers"] = [
        {"question": "Q1. ...", "selectedOption": {"value": "Answer"}}, ...
      ]
    Returns a list of (question, answer) tuples.
    """
    pairs: List[Tuple[str, str]] = []
    qa_list = raw.get("questionAnswers") or raw.get("question_answers") or raw.get("questionanswers")
    if isinstance(qa_list, list):
        for item in qa_list:
            if not isinstance(item, dict):
                continue
            q_text = str(item.get("question", "")).strip()
            # Determine the answer text from either selectedOption or direct fields
            ans_val = None
            selected = item.get("selectedOption") or item.get("selected_option")
            if isinstance(selected, dict):
                ans_val = selected.get("value") or selected.get("label")
            if ans_val is None:
                ans_val = item.get("answer") or item.get("value")
            if q_text and ans_val is not None:
                pairs.append((q_text, str(ans_val).strip()))
    return pairs

def _extract_qna_from_simple_keys(raw: Dict[str, Any]) -> List[Tuple[str, str]]:
    """
    Extract Q&A pairs from a simple key/value payload (non-array).
    Uses ANS_KEY_TO_Q and variant mappings to translate keys into question text.
    """
    pairs: List[Tuple[str, str]] = []
    # Determine the context variant based on purchase_for and gender
    variant_key = "self"
    pf_val = raw.get("purchase_for") or raw.get("purchaseFor") or raw.get("purchasefor")
    if pf_val:
        pf_val_low = str(pf_val).lower()
        if "self" in pf_val_low:
            variant_key = "self"
        elif "other" in pf_val_low:
            # If purchasing for someone else, use gender to pick the correct pronoun variant
            gender_val = raw.get("gender") or raw.get("Gender") or raw.get("gender_identity")
            gender_low = str(gender_val).lower() if gender_val else ""
            if gender_low in ["female", "woman", "girl", "her"]:
                variant_key = "others_female"
            elif gender_low in ["male", "man", "boy", "his"]:
                variant_key = "others_male"
            else:
                variant_key = "others"
    # Build a lookup for variant question text (keys lowercased for matching)
    variant_map_raw = ANS_KEY_TO_Q_VARIANTS.get(variant_key, {})
    variant_map = {k.lower(): v for k, v in variant_map_raw.items()}
    log.info("Answer variant context selected: %s", variant_key)
    # Translate each answer key to its question text
    for key, val in raw.items():
        if val in (None, "", []):
            continue
        k_low = key.lower()
        if k_low in ANS_KEY_TO_Q:  # direct mapping from base questions map
            pairs.append((ANS_KEY_TO_Q[k_low], str(val).strip()))
        elif k_low in variant_map:  # mapping from the selected variant questions
            pairs.append((variant_map[k_low], str(val).strip()))
    return pairs

def _ensure_qna_lists(raw: Dict[str, Any]) -> Tuple[List[str], List[str], Dict[str, Any]]:
    """
    Normalize the incoming payload to ordered questions and answers lists.
    Also returns a context dict of relevant fields (purchase_for/gender/occasion/purpose).
    """
    # 1) Already-provided questions & answers lists
    if isinstance(raw.get("questions"), list) and isinstance(raw.get("answers"), list):
        qs = [str(q) for q in raw["questions"]]
        ans = [str(a) if a is not None else "" for a in raw["answers"]]
        return qs, ans, {}
    # 2) Quizell-style questionAnswers array
    qa_pairs = _extract_qna_from_quizell(raw)
    if not qa_pairs:
        # If not at top-level, check if answers are nested under an "answers" key
        answers_section = raw.get("answers")
        if isinstance(answers_section, dict):
            qa_pairs = _extract_qna_from_quizell(answers_section)
    if qa_pairs:
        qs = [q for (q, _) in qa_pairs]
        ans = [a for (_, a) in qa_pairs]
        return qs, ans, {}
    # 3) Keyed simple form (flexible key/value answers)
    # If answers are nested in an "answers" sub-dict, use that; otherwise use raw directly
    answers_src = raw["answers"] if isinstance(raw.get("answers"), dict) else raw
    simple_pairs = _extract_qna_from_simple_keys(answers_src)
    if simple_pairs:
        # Determine variant context again for ordering (same logic as above)
        pf_val = answers_src.get("purchase_for") or answers_src.get("purchaseFor") or answers_src.get("purchasefor")
        variant_key = "self"
        if pf_val:
            pf_val_low = str(pf_val).lower()
            if "self" in pf_val_low:
                variant_key = "self"
            elif "other" in pf_val_low:
                gender_val = answers_src.get("gender") or answers_src.get("Gender") or answers_src.get("gender_identity")
                gender_low = str(gender_val).lower() if gender_val else ""
                if gender_low in ["female", "woman", "girl", "her"]:
                    variant_key = "others_female"
                elif gender_low in ["male", "man", "boy", "his"]:
                    variant_key = "others_male"
                else:
                    variant_key = "others"
        variant_map_raw = ANS_KEY_TO_Q_VARIANTS.get(variant_key, {})
        # Establish desired question order: all base questions, then all variant questions
        order: List[str] = []
        for q_text in ANS_KEY_TO_Q.values():
            order.append(q_text)
        for q_text in variant_map_raw.values():
            order.append(q_text)
        # Map each question text to its answer for quick lookup
        got = {q: a for (q, a) in simple_pairs}
        qs: List[str] = []
        ans: List[str] = []
        for q in order:
            qs.append(q)
            ans.append(got.get(q, ""))
        # Collect a context dict for debugging (if those keys were present in input)
        ctx: Dict[str, Any] = {}
        for key in ["purchase_for", "gender", "occasion", "purpose"]:
            if key in answers_src:
                ctx[key] = answers_src[key]
            elif key in raw:
                ctx[key] = raw[key]
        return qs, ans, ctx
    # 4) Fallback: no recognizable Q&A structure
    return [], [], {}

def _build_backend_payload(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Construct the final JSON payload expected by the backend (/createRequestJSON).
    Fields:
      - questions: "Q1 text, Q2 text, ..., QN text"
      - answers:   "A1, A2, ..., AN"
      - request_id, name, email, phoneNumber, birth_date
    """
    ident = _extract_identity(raw)
    questions, answers, _ = _ensure_qna_lists(raw)
    # Join questions and answers with comma+space delimiter (backend splits on commas)
    q_str = ", ".join(questions)
    a_str = ", ".join(answers)
    # Assemble the payload with required fields
    payload = {
        "questions":   q_str,
        "answers":     a_str,
        "request_id":  ident.get("request_id", ""),
        "name":        ident.get("name", ""),
        "email":       ident.get("email", ""),
        "phoneNumber": ident.get("phoneNumber", ""),
        "birth_date":  ident.get("birth_date", "")
    }
    return payload

# -------------------------
# FastAPI Routes
# -------------------------
@app.get("/")
def root():
    return {"ok": True, "service": "Ring Style Adapter (flex → JSON)"}

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/ingest")
async def ingest(
    raw: Dict[str, Any] = Body(..., description="Flexible UI JSON"),
    preview: bool = Query(False, description="Return normalized JSON without calling backend"),
    echo: bool = Query(False, description="Return backend status + body"),
    req: Request = None,
):
    """
    Ingest a flexible UI payload, normalize it, and optionally forward to the backend.
    - Set `preview=true` to return the transformed JSON without calling the backend.
    - Set `echo=true` to include the backend's response in the output (for debugging).
    """
    _check_api_key(req)
    _require_backend()
    try:
        # 1. Normalize input to the expected backend JSON format
        backend_json = _build_backend_payload(raw)
        # Log a warning if important identity fields are missing
        missing = [field for field in ["request_id", "name", "email"] if not str(backend_json.get(field, "")).strip()]
        if missing:
            log.warning("Normalized JSON missing fields: %s", missing)
        # 2. If preview mode, return the JSON without calling the backend
        if preview:
            # Provide context info (if any) along with the normalized JSON
            _, _, ctx = _ensure_qna_lists(raw)
            return {"sent_json": backend_json, "context": ctx}
        # 3. Call the backend /createRequestJSON endpoint
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(BACKEND_POST_URL, json=backend_json)
            status = resp.status_code
            body_text = resp.text
            try:
                body_json = resp.json()
            except Exception:
                body_json = None
        # 4. If echo is requested, include backend response details
        if echo:
            return {
                "sent_json": backend_json,
                "backend_status": status,
                "backend_headers": {
                    "content-type": resp.headers.get("content-type", "")
                },
                "backend_body": body_json if body_json is not None else body_text
            }
        # 5. Otherwise, return a minimal success response
        return {"status": "ok", "request_id": backend_json.get("request_id", "")}
    except HTTPException:
        raise
    except Exception as e:
        log.exception("ingest failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))

# -------------------------
# Optional passthrough endpoints for testing
# -------------------------
@app.post("/createRequestJSON")
async def passthrough_create_request_json(body: Dict[str, Any] = Body(...)):
    """
    (Testing only) Forward a pre-formatted request JSON directly to the backend.
    """
    _require_backend()
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(BACKEND_POST_URL, json=body)
        try:
            return JSONResponse(status_code=resp.status_code, content=resp.json())
        except Exception:
            return JSONResponse(status_code=resp.status_code, content={"body": resp.text})

@app.get("/fetchResponse")
async def passthrough_fetch_response(response_id: str):
    """
    (Testing only) Forward a fetchResponse call to the backend (if BACKEND_GET_URL is set).
    """
    if not BACKEND_GET_URL:
        raise HTTPException(status_code=501, detail="BACKEND_GET_URL is not configured")
    url = BACKEND_GET_URL
    # Ensure the URL ends with a query parameter for response_id
    if not url.endswith("="):
        if "response_id=" not in url:
            url += "&response_id=" if "?" in url else "?response_id="
    url += response_id
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(url)
        try:
            return JSONResponse(status_code=resp.status_code, content=resp.json())
        except Exception:
            return JSONResponse(status_code=resp.status_code, content={"body": resp.text})
