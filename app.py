# app.py
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
# FastAPI
# -------------------------
app = FastAPI(title="Ring Style Adapter (flex → JSON)", version="1.0.0", openapi_url="/openapi.json")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

# -------------------------
# Environment
# -------------------------
# Example:
#   BACKEND_POST_URL=https://projectaiapi-xxxx.azurewebsites.net/createRequestJSON
#   BACKEND_GET_URL =https://projectaiapi-xxxx.azurewebsites.net/fetchResponse?response_id=
BACKEND_POST_URL = os.getenv("BACKEND_POST_URL", "").strip()
BACKEND_GET_URL  = os.getenv("BACKEND_GET_URL", "").strip()

API_KEY_REQUIRED = os.getenv("API_KEY_REQUIRED", "false").lower() == "true"
API_KEY          = os.getenv("API_KEY", "")

# -------------------------
# Config (mapping.config.json)
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
FIELD_MAP: Dict[str,str]= {k.lower(): v for k,v in CONFIG.get("field_map", {}).items()}
ANS_KEY_TO_Q: Dict[str,str] = CONFIG.get("answer_key_to_question", {})
RESULT_KEY_CANDIDATES  = [s.lower() for s in CONFIG.get("result_key_field_candidates", ["result_key","response_id","resultkey","responsekey"])]
DEFAULTS               = CONFIG.get("defaults", {})

# -------------------------
# Helpers
# -------------------------
def _require_backend():
    if not BACKEND_POST_URL:
        raise HTTPException(status_code=500, detail="BACKEND_POST_URL is not configured")

def _check_api_key(req: Request):
    if not API_KEY_REQUIRED:
        return
    if not API_KEY or req.headers.get("x-api-key") != API_KEY:
        raise HTTPException(status_code=401, detail="unauthorized")

def _flatten(d: Any) -> Dict[str, Any]:
    flat: Dict[str, Any] = {}
    def visit(prefix: str, node: Any):
        if isinstance(node, dict):
            for k, v in node.items():
                visit(f"{prefix}.{k}" if prefix else k, v)
        elif isinstance(node, list):
            flat[prefix] = node
            for i, v in enumerate(node):
                visit(f"{prefix}[{i}]", v)
        else:
            flat[prefix] = node
    visit("", d)
    return flat

def _find_result_key(d: Dict[str, Any]) -> str:
    # look top-level first
    for k in d.keys():
        if k.lower() in RESULT_KEY_CANDIDATES:
            return str(d[k])
    # search nested
    for v in d.values():
        if isinstance(v, dict):
            rk = _find_result_key(v)
            if rk:
                return rk
    return ""

def _extract_identity(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map any identity-like fields from flexible payload into the backend keys
    according to FIELD_MAP and DEFAULTS.
    """
    out = dict(DEFAULTS)
    flat = _flatten(raw)

    # Prefer explicit top-level names commonly used
    for cand_in, cand_out in [
        ("request_id", "request_id"),
        ("result_key", "request_id"),    # sometimes called result_key
        ("resultkey", "request_id"),
        ("response_id", "request_id"),
        ("name", "name"),
        ("full_name", "name"),
        ("email", "email"),
        ("phone", "phoneNumber"),
        ("phone_number", "phoneNumber"),
        ("phonenumber", "phoneNumber"),
        ("birth_date", "birth_date"),
        ("date", "birth_date"),
        ("dob", "birth_date"),
    ]:
        for k, v in raw.items():
            if k.lower() == cand_in and v not in (None, "", []):
                out[cand_out] = v

    # Then run generic map by FIELD_MAP on any flat keys
    for k, v in flat.items():
        leaf = k.split(".")[-1].split("[")[0].lower()
        if leaf in FIELD_MAP:
            out[FIELD_MAP[leaf]] = v

    # If request_id still missing, try nested result key
    if "request_id" not in out or not str(out.get("request_id")).strip():
        rk = _find_result_key(raw)
        if rk:
            out["request_id"] = rk

    return out

def _extract_qna_from_quizell(raw: Dict[str, Any]) -> List[Tuple[str, str]]:
    """
    Accepts Quizell-style structure:
      raw["questionAnswers"] = [
        {"question": "Q1. Who are you purchasing for?", "selectedOption": {"value": "Self"}},
        ...
      ]
    Returns list of (question, answer)
    """
    pairs: List[Tuple[str, str]] = []
    qa_list = raw.get("questionAnswers") or raw.get("question_answers") or raw.get("questionanswers")
    if isinstance(qa_list, list):
        for item in qa_list:
            if not isinstance(item, dict):
                continue
            q = str(item.get("question", "")).strip()
            ans = None
            sel = item.get("selectedOption") or item.get("selected_option")
            if isinstance(sel, dict):
                ans = sel.get("value") or sel.get("label")
            if ans is None:
                # fallback to direct value if present
                ans = item.get("answer") or item.get("value")
            if q and ans is not None:
                pairs.append((q, str(ans).strip()))
    return pairs

def _extract_qna_from_simple_keys(raw: Dict[str, Any]) -> List[Tuple[str, str]]:
    """
    Accepts simple UI forms like:
      {"purchase_for": "self", "gender": "female", "occasion": "engagement", "purpose": "daily wear"}
    Uses ANS_KEY_TO_Q mapping from mapping.config.json to translate keys -> question text
    """
    pairs: List[Tuple[str, str]] = []
    for k, v in raw.items():
        map_q = ANS_KEY_TO_Q.get(k.lower())
        if map_q and v not in (None, ""):
            pairs.append((map_q, str(v)))
    return pairs

def _ensure_qna_lists(raw: Dict[str, Any]) -> Tuple[List[str], List[str], Dict[str, Any]]:
    """
    Produce ordered questions & answers lists from whatever the UI sent.
    Also returns a 'context' dict with extra mapped fields (purchase_for/gender/occasion/purpose if given).
    """
    # 1) Direct lists already supplied
    if isinstance(raw.get("questions"), list) and isinstance(raw.get("answers"), list):
        qs = [str(q) for q in raw["questions"]]
        ans = [str(a) if a is not None else "" for a in raw["answers"]]
        return qs, ans, {}

    # 2) Quizell-like array
    qa_pairs = _extract_qna_from_quizell(raw)
    if qa_pairs:
        qs = [q for (q, _) in qa_pairs]
        ans = [a for (_, a) in qa_pairs]
        return qs, ans, {}

    # 3) Keyed simple form
    simple_pairs = _extract_qna_from_simple_keys(raw)
    if simple_pairs:
        # Keep the order defined by ANS_KEY_TO_Q values appearance
        order = [ANS_KEY_TO_Q[k] for k in ANS_KEY_TO_Q]  # question text in desired order
        # Build a map question->answer from what user provided
        got = {q: a for (q, a) in simple_pairs}
        qs, ans = [], []
        ctx = {}
        for q in order:
            qs.append(q)
            ans.append(got.get(q, ""))

        # also expose a compact context for debugging
        # reverse-map back to friendly keys if present in raw
        for k in ANS_KEY_TO_Q.keys():
            if k in raw:
                ctx[k] = raw[k]
        return qs, ans, ctx

    # 4) Fallback: nothing recognized
    return [], [], {}

def _build_backend_payload(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build the final JSON your backend expects:
      {
        "questions": "Q1..., Q2..., ...",
        "answers":   "ans1, ans2, ...",
        "request_id": "...",
        "email": "...", "name": "...", "phoneNumber": "...", "birth_date": "..."
      }
    """
    ident = _extract_identity(raw)
    questions, answers, ctx = _ensure_qna_lists(raw)

    # Join with comma+space, because backend splits on comma.
    q_str = ", ".join(questions)
    a_str = ", ".join(answers)

    payload = {
        "questions":  q_str,
        "answers":    a_str,
        "request_id": ident.get("request_id", ""),
        "email":      ident.get("email", ""),
        "name":       ident.get("name", ""),
        "phoneNumber":ident.get("phoneNumber", ""),
        "birth_date": ident.get("birth_date", ""),
    }

    return payload

# -------------------------
# Routes
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
    Accept flexible UI JSON, normalize to backend expected JSON and optionally call backend.
    """
    _check_api_key(req)
    _require_backend()

    try:
        # normalize
        backend_json = _build_backend_payload(raw)

        # quick sanity (these are what backend logs actually use)
        missing = [k for k in ["request_id", "name", "email"] if not str(backend_json.get(k, "")).strip()]
        if missing:
            log.warning("Normalized JSON missing fields: %s", missing)

        # If only preview, do not call backend
        if preview:
            return {"sent_json": backend_json, "context": raw.get("context", {})}

        # Call backend /createRequestJSON
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(BACKEND_POST_URL, json=backend_json)
            status = resp.status_code
            body_txt = resp.text
            try:
                body_json = resp.json()
            except Exception:
                body_json = None

        # Optionally echo backend result
        if echo:
            return {
                "sent_json": backend_json,
                "backend_status": status,
                "backend_headers": {"content-type": resp.headers.get("content-type", "")},
                "backend_body": body_json if body_json is not None else body_txt,
            }

        # Default minimal response
        return {"status": "ok", "request_id": backend_json.get("request_id", "")}

    except HTTPException:
        raise
    except Exception as e:
        log.exception("ingest failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


# -------------------------
# (Optional) simple passthroughs for testing
# -------------------------
@app.post("/createRequestJSON")
async def passthrough_create_request_json(body: Dict[str, Any] = Body(...)):
    """
    If you want to call the adapter with exact backend shape (for direct tests),
    this endpoint simply forwards to BACKEND_POST_URL.
    """
    _require_backend()
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.post(BACKEND_POST_URL, json=body)
        try:
            return JSONResponse(status_code=r.status_code, content=r.json())
        except Exception:
            return JSONResponse(status_code=r.status_code, content={"body": r.text})


@app.get("/fetchResponse")
async def passthrough_fetch_response(response_id: str):
    """Optional helper that forwards to the backend GET endpoint if provided."""
    if not BACKEND_GET_URL:
        raise HTTPException(status_code=501, detail="BACKEND_GET_URL is not configured")
    url = BACKEND_GET_URL
    if not url.endswith("="):
        # allow both .../fetchResponse?response_id=  OR a templated URL
        if "response_id=" not in url:
            if "?" in url:
                url = url + "&response_id="
            else:
                url = url + "?response_id="
    url = url + response_id

    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.get(url)
        try:
            return JSONResponse(status_code=r.status_code, content=r.json())
        except Exception:
            return JSONResponse(status_code=r.status_code, content={"body": r.text})
