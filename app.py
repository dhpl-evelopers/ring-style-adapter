# app.py

import os

import json

import logging

import pathlib

from typing import Any, Dict, List, Tuple, Union

import httpx

from fastapi import FastAPI, Body, Query, Request, HTTPException

from fastapi.responses import JSONResponse

# ----------------- Logging -----------------

log = logging.getLogger("adapter")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ----------------- App -----------------

app = FastAPI(title="Ring Style Adapter (flex → JSON)", version="1.0.0")

# ----------------- Env -----------------

BACKEND_POST_URL = os.getenv("BACKEND_POST_URL", "").strip()  # .../createRequestJSON

API_KEY_REQUIRED = os.getenv("API_KEY_REQUIRED", "false").lower() == "true"

API_KEY = os.getenv("API_KEY", "")

# ----------------- Config -----------------

CFG_PATH = pathlib.Path(__file__).with_name("mapping.config.json")

def _load_config() -> Dict[str, Any]:

    try:

        with open(CFG_PATH, "r", encoding="utf-8") as f:

            cfg = json.load(f)

        # normalize keys for case-insensitive lookup

        cfg["field_map"] = {k.lower(): v for k, v in cfg.get("field_map", {}).items()}

        cfg["answer_key_to_question"] = cfg.get("answer_key_to_question", {})

        cfg["result_key_field_candidates"] = [

            s.lower() for s in cfg.get("result_key_field_candidates", ["result_key", "response_id"])

        ]

        cfg["defaults"] = cfg.get("defaults", {})

        return cfg

    except Exception as e:

        log.error("Could not read mapping.config.json: %s", e)

        return {

            "field_map": {},

            "answer_key_to_question": {},

            "result_key_field_candidates": ["result_key", "response_id"],

            "defaults": {},

        }

CONFIG = _load_config()

FIELD_MAP: Dict[str, str] = CONFIG["field_map"]

ANS_KEY_TO_Q: Dict[str, str] = CONFIG["answer_key_to_question"]

RESULT_CANDS: List[str] = CONFIG["result_key_field_candidates"]

DEFAULTS: Dict[str, Any] = CONFIG["defaults"]

# ----------------- Utils -----------------

def _require_backend():

    if not BACKEND_POST_URL:

        raise HTTPException(status_code=500, detail="BACKEND_POST_URL is not configured")

def _check_api_key(req: Request):

    if not API_KEY_REQUIRED:

        return

    if not API_KEY or req.headers.get("x-api-key") != API_KEY:

        raise HTTPException(status_code=401, detail="unauthorized")

def _first_present(d: Dict[str, Any], keys: List[str]) -> Tuple[str, Any]:

    """Return (key,value) for the first matching key (case-insensitive) in d (recursively)."""

    low_targets = [k.lower() for k in keys]

    stack = [d]

    while stack:

        cur = stack.pop()

        if isinstance(cur, dict):

            for k, v in cur.items():

                if k.lower() in low_targets:

                    return (k, v)

                if isinstance(v, (dict, list)):

                    stack.append(v)

        elif isinstance(cur, list):

            for v in cur:

                if isinstance(v, (dict, list)):

                    stack.append(v)

    return ("", None)

def _find_request_id(payload: Dict[str, Any]) -> str:

    _, val = _first_present(payload, RESULT_CANDS + ["request_id"])

    return str(val) if val not in (None, "") else ""

def _flatten(src: Any, prefix: str = "", out: Dict[str, Any] = None) -> Dict[str, Any]:

    """Flatten nested dict/list into dotted-key dict for permissive mapping."""

    if out is None:

        out = {}

    if isinstance(src, dict):

        for k, v in src.items():

            _flatten(v, f"{prefix}.{k}" if prefix else k, out)

    elif isinstance(src, list):

        for i, v in enumerate(src):

            _flatten(v, f"{prefix}[{i}]", out)

    else:

        out[prefix] = src

    return out

def _normalize_core_fields(raw: Dict[str, Any]) -> Dict[str, Any]:

    """

    Use FIELD_MAP + DEFAULTS to pull out standard fields:

    name, email, phoneNumber, birth_date (or phone, phone_number, date), etc.

    """

    out = dict(DEFAULTS)

    flat = _flatten(raw)

    # prefer explicit keys from FIELD_MAP

    for dotted_key, val in flat.items():

        leaf = dotted_key.split(".")[-1].replace("[", "_").replace("]", "")

        lk = leaf.lower()

        if lk in FIELD_MAP:

            out[FIELD_MAP[lk]] = val

        else:

            # fallback: keep common synonyms if not already set

            if lk in ("name", "email", "phonenumber", "phone", "birth_date", "date"):

                key_map = {

                    "phonenumber": "phoneNumber",

                    "phone": "phoneNumber",

                    "date": "birth_date",

                    "birth_date": "birth_date",

                    "name": "name",

                    "email": "email",

                }

                out.setdefault(key_map[lk], val)

    # request_id (aka result key)

    req_id = _find_request_id(raw)

    if req_id:

        out["request_id"] = req_id

    # normalize to strings (your backend expects strings)

    for k in list(out.keys()):

        if out[k] is None:

            out[k] = ""

        elif not isinstance(out[k], str):

            out[k] = str(out[k])

    return out

def _extract_answers(raw: Dict[str, Any]) -> List[Tuple[str, str]]:

    """

    Build a list of (question_text, answer_text).

    Accepts:

      - raw["answers"] as a list: ["self","female",...]

      - raw["answers"] as an object: {"who":"self","gender":"female",...}

      - quizell-like arrays under keys such as "questionAnswers"

      - individual fields that map via ANS_KEY_TO_Q

    """

    pairs: List[Tuple[str, str]] = []

    # Case A: explicit structure {answers:[{question:"...", answer:"..."}]}

    if isinstance(raw.get("answers"), list) and raw["answers"] and isinstance(raw["answers"][0], dict):

        for item in raw["answers"]:

            q = str(item.get("question", "")).strip()

            a = str(item.get("answer", "") or item.get("value", "")).strip()

            if q and a:

                pairs.append((q, a))

    # Case B: Quizell style "questionAnswers": [{question:"Q1...", selectedOption:{value:"..."}}]

    if not pairs:

        qa = raw.get("questionAnswers") or raw.get("questions")  # some payloads use "questions"

        if isinstance(qa, list):

            for item in qa:

                q = str(item.get("question", "")).strip()

                sel = item.get("selectedOption") or {}

                a = str(sel.get("value", "")).strip()

                if q and a:

                    pairs.append((q, a))

    # Case C: flat answers dict + ANS_KEY_TO_Q mapping

    if not pairs:

        ans_obj = raw.get("answers")

        if isinstance(ans_obj, dict):

            for key, val in ans_obj.items():

                q = ANS_KEY_TO_Q.get(key) or ANS_KEY_TO_Q.get(key.lower(), "")

                if q and val not in (None, ""):

                    pairs.append((q, str(val)))

    # Case D: simple list + we look up question order from ANS_KEY_TO_Q (if keys "order" exist)

    if not pairs and isinstance(raw.get("answers"), list):

        # If you want a custom order, you can encode it in config later; for now we can’t name questions.

        # We'll fall back to just returning values without questions (backend tolerates? No, it expects text.)

        # So we leave this path unused unless questions are provided elsewhere.

        pass

    # Deduplicate while preserving order

    seen = set()

    dedup: List[Tuple[str, str]] = []

    for q, a in pairs:

        key = (q.strip().lower(), a.strip().lower())

        if key in seen:

            continue

        seen.add(key)

        dedup.append((q.strip(), a.strip()))

    return dedup

def _to_backend_json(raw: Dict[str, Any]) -> Dict[str, Any]:

    """

    Final shape for /createRequestJSON (strings for questions/answers, not lists).

    """

    core = _normalize_core_fields(raw)

    qa_pairs = _extract_answers(raw)

    # If the UI just sent a short-form like:

    # {"purchase_for":"self","gender":"female","occasion":"engagement","purpose":"daily wear"}

    # and mapping.config.json has answer_key_to_question for these keys,

    # _extract_answers will produce pairs accordingly.

    questions: List[str] = [q for q, _ in qa_pairs]

    answers:   List[str] = [a for _, a in qa_pairs]

    # Comma-separated strings (backend does split(","))

    # Use ", " to match natural text; backend split(",") will still work.

    questions_str = ", ".join(questions)

    answers_str   = ", ".join(answers)

    return {

        "questions": questions_str,

        "answers": answers_str,

        # The rest are simple strings:

        "request_id": core.get("request_id", ""),

        "email":      core.get("email", ""),

        "name":       core.get("name", ""),

        "phoneNumber": core.get("phoneNumber", ""),

        "birth_date":  core.get("birth_date", ""),

    }

# ----------------- Routes -----------------

@app.get("/")

def root():

    return {

        "ok": True,

        "requires_api_key": API_KEY_REQUIRED,

        "has_backend_post_url": bool(BACKEND_POST_URL),

        "backend_post_url": BACKEND_POST_URL,

    }

@app.get("/health")

def health():

    return {"ok": True}

@app.post("/ingest")

async def ingest(

    req: Request,

    raw: Dict[str, Any] = Body(..., description="Flexible UI JSON"),

    preview: bool = Query(False, description="Return normalized JSON without calling backend"),

    echo: bool = Query(False, description="Return backend status + body"),

):

    """

    Accept flexible UI JSON, normalize to backend expected JSON (strings for questions/answers),

    POST to BACKEND_POST_URL (/createRequestJSON). Use ?preview=true to see normalized payload only.

    """

    _check_api_key(req)

    _require_backend()

    try:

        # Normalize

        payload = _to_backend_json(raw)

    except Exception as e:

        log.exception("Normalization failed")

        raise HTTPException(status_code=422, detail=f"Normalization error: {e}")

    if preview:

        return {"sent_json": payload, "context": raw}

    # Call backend

    try:

        async with httpx.AsyncClient(timeout=30) as client:

            resp = await client.post(

                BACKEND_POST_URL,

                json=payload,

                headers={"Content-Type": "application/json"},

            )

    except httpx.RequestError as e:

        return JSONResponse(

            status_code=502,

            content={"detail": {"adapter_error": "cannot_reach_backend", "msg": str(e), "sent_json": payload}},

        )

    body_text = resp.text

    if 200 <= resp.status_code < 300:

        if echo:

            return {

                "sent_json": payload,

                "backend_status": resp.status_code,

                "backend_headers": {"content-type": resp.headers.get("content-type", "")},

                "backend_body": body_text,

            }

        # normal success: return response body as-is (try JSON)

        try:

            return json.loads(body_text)

        except Exception:

            return {"body": body_text}

    # surface backend error

    return JSONResponse(

        status_code=502,

        content={

            "sent_json": payload,

            "backend_status": resp.status_code,

            "backend_headers": {"content-type": resp.headers.get("content-type", "")},

            "backend_body": body_text or "<empty>",

        },

    )




