import os

import json

from typing import Any, Dict, List, Tuple

import httpx

from fastapi import FastAPI, HTTPException, Request

from fastapi.responses import JSONResponse, PlainTextResponse

from fastapi.middleware.cors import CORSMiddleware

import xml.etree.ElementTree as ET

# -----------------------------------------------------------------------------

# Config from Environment

# -----------------------------------------------------------------------------

BACKEND_POST_URL = os.getenv("BACKEND_POST_URL", "").strip()

BACKEND_GET_URL = os.getenv("BACKEND_GET_URL", "").strip()

API_KEY_REQUIRED = os.getenv("API_KEY_REQUIRED", "false").strip().lower() == "true"

API_KEY = os.getenv("API_KEY", "").strip()

if not BACKEND_POST_URL:

    # You'll still be able to hit / and /ping, but /adapter will 500 with a clear msg

    print("[WARN] BACKEND_POST_URL is not set; /adapter will fail until you set it.")

# -----------------------------------------------------------------------------

# CORS + App

# -----------------------------------------------------------------------------

app = FastAPI(title="Ring Style Adapter", version="2.2.0")

app.add_middleware(

    CORSMiddleware,

    allow_origins=["*"],  # tighten if needed

    allow_credentials=True,

    allow_methods=["*"],

    allow_headers=["*"],

)

# -----------------------------------------------------------------------------

# Helpers

# -----------------------------------------------------------------------------

def _nz(v: Any, default: str = "") -> str:

    """Normalize: None/empty -> default; else str(value)."""

    if v is None:

        return default

    s = str(v).strip()

    return s if s else default

def _set_text(parent: ET.Element, tag: str, value: Any, default: str = "") -> ET.Element:

    el = ET.SubElement(parent, tag)

    el.text = _nz(value, default)

    return el

def _load_mapping() -> Dict[str, str]:

    """

    Load mapping.config.json if present and build:

      - id/key  -> human question text

    Structure expected (your screenshot):

      {

        "mapping": {

          ...

          "answers_path": "answers",

          "questions": [

            {"key": "q1", "text": "Who are you purchasing for?"},

            ...

          ]

        }

      }

    We’ll accept either {"key": "q1_style"} or id-like strings ("q1_style").

    """

    path = os.path.join(os.getcwd(), "mapping.config.json")

    if not os.path.isfile(path):

        print("[INFO] mapping.config.json not found; will pass IDs as question text.")

        return {}

    try:

        with open(path, "r", encoding="utf-8") as f:

            cfg = json.load(f)

        questions = (((cfg or {}).get("mapping") or {}).get("questions")) or []

        out = {}

        for q in questions:

            key = (q or {}).get("key")

            text = (q or {}).get("text")

            if key and text:

                out[str(key)] = str(text)

        print(f"[INFO] Loaded {len(out)} question text mappings from mapping.config.json")

        return out

    except Exception as e:

        print(f"[WARN] Unable to read mapping.config.json: {e}")

        return {}

QUESTION_TEXTS: Dict[str, str] = _load_mapping()

def _normalize_payload(body: Dict[str, Any]) -> Dict[str, Any]:

    """

    Accept several shapes and normalize to:

    {

      "full_name": "...",

      "email": "...",

      "phone_number": "...",

      "birth_date": "YYYY-MM-DD",

      "request_id": "...",

      "questionAnswers": [ {"question": "...", "answer": "..."} ]

    }

    Supported inputs:

      A) Direct (already normalized)

         - keys: full_name, email, phone_number, birth_date, request_id, questionAnswers

      B) Customer-style + 'answers' list from prior attempts:

         - customer.name / customer.email / customer.mobile / customer.birth_date

         - sessionId or request_id

         - answers: [{id|key, value}] or [{question, answer}]

    """

    out: Dict[str, Any] = {

        "full_name":    body.get("full_name"),

        "email":        body.get("email"),

        "phone_number": body.get("phone_number"),

        "birth_date":   body.get("birth_date"),

        "request_id":   body.get("request_id") or body.get("requestId") or body.get("session_id") or body.get("sessionId"),

        "questionAnswers": []

    }

    # If top-level fields missing, try customer.* fallbacks

    cust = body.get("customer") or {}

    out["full_name"]    = out["full_name"]    or cust.get("name")

    out["email"]        = out["email"]        or cust.get("email")

    out["phone_number"] = out["phone_number"] or cust.get("mobile")

    out["birth_date"]   = out["birth_date"]   or cust.get("birth_date")

    # Gather question/answers from either "questionAnswers" or "answers"

    qa_list: List[Dict[str, Any]] = []

    if isinstance(body.get("questionAnswers"), list):

        # Already in desired form

        for item in body.get("questionAnswers", []):

            qa_list.append({

                "question": (item or {}).get("question"),

                "answer":   (item or {}).get("answer"),

            })

    elif isinstance(body.get("answers"), list):

        # Several possibilities:

        #  - [{ "id": "q1_style", "value": "Solitaire" }, ...]

        #  - [{ "key": "q1_style", "value": "Solitaire" }, ...]

        #  - [{ "question": "Who are you purchasing for?", "answer": "Self" }, ...]

        for item in body.get("answers", []):

            if not isinstance(item, dict):

                continue

            if "question" in item and "answer" in item:

                qa_list.append({"question": item["question"], "answer": item["answer"]})

                continue

            qid = item.get("id") or item.get("key")

            val = item.get("value")

            # Map ID to human text if we know it; otherwise use the ID itself

            qtext = QUESTION_TEXTS.get(str(qid), str(qid) if qid else "")

            qa_list.append({"question": qtext, "answer": val})

    out["questionAnswers"] = qa_list

    return out

def build_backend_xml(normalized: Dict[str, Any]) -> bytes:

    """

    Produce the backend XML. Always safe — never calls .text on missing nodes.

    """

    root = ET.Element("root")

    _set_text(root, "name",         normalized.get("full_name"))

    _set_text(root, "email",        normalized.get("email"))

    _set_text(root, "phone_number", normalized.get("phone_number"))

    _set_text(root, "request_id",   normalized.get("request_id"))

    _set_text(root, "birth_date",   normalized.get("birth_date"))

    qa_root = ET.SubElement(root, "questionAnswers")

    for item in normalized.get("questionAnswers") or []:

        qa = ET.SubElement(qa_root, "questionAnswer")

        _set_text(qa, "question", (item or {}).get("question"))

        _set_text(qa, "answer",   (item or {}).get("answer"))

    return ET.tostring(root, encoding="utf-8", method="xml")

# -----------------------------------------------------------------------------

# Routes

# -----------------------------------------------------------------------------

@app.get("/", response_class=PlainTextResponse)

def root() -> str:

    return "Ring Style Adapter OK"

@app.get("/ping", response_class=PlainTextResponse)

def ping() -> str:

    return "pong"

@app.post("/adapter")

async def adapter(request: Request):

    # Enforce optional API key (via header: X-Api-Key)

    if API_KEY_REQUIRED:

        client_key = request.headers.get("X-Api-Key", "")

        if not client_key or client_key != API_KEY:

            raise HTTPException(status_code=401, detail="Invalid or missing API key.")

    # Content-Type must be application/json

    ctype = (request.headers.get("content-type") or "").lower()

    if "application/json" not in ctype:

        raise HTTPException(status_code=415, detail="Unsupported Content-Type. Use application/json.")

    try:

        body = await request.json()

        if not isinstance(body, dict):

            raise ValueError("Body must be a JSON object.")

    except Exception:

        raise HTTPException(status_code=400, detail="Invalid JSON payload.")

    # Normalize + build XML

    normalized = _normalize_payload(body)

    xml_bytes = build_backend_xml(normalized)

    # Forward to backend

    if not BACKEND_POST_URL:

        raise HTTPException(status_code=500, detail="BACKEND_POST_URL is not configured.")

    headers = {

        "Content-Type": "application/xml",

        "Accept": "application/json"

    }

    try:

        async with httpx.AsyncClient(timeout=30) as client:

            resp = await client.post(BACKEND_POST_URL, content=xml_bytes, headers=headers)

    except httpx.TimeoutException:

        raise HTTPException(status_code=504, detail="Backend timed out.")

    except httpx.HTTPError as e:

        raise HTTPException(status_code=502, detail=f"Backend error: {e}")

    # Mirror backend response

    ctype = (resp.headers.get("content-type") or "").lower()

    if "application/json" in ctype:

        # If backend returns invalid JSON we still fall back to text

        try:

            return JSONResponse(status_code=resp.status_code, content=resp.json())

        except Exception:

            return PlainTextResponse(status_code=resp.status_code, content=resp.text)

    return PlainTextResponse(status_code=resp.status_code, content=resp.text)
 
