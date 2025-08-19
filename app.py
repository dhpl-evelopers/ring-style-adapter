import os, json, logging

from typing import Any, Dict, List, Tuple

import httpx

from fastapi import FastAPI, Request, HTTPException, Query

from fastapi.responses import JSONResponse

log = logging.getLogger("adapter")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

app = FastAPI(title="Ring Style Adapter (flex → JSON)", version="1.0.0")

# ---------- Env (set these in Azure) ----------

# Point this to your backend JSON endpoint:

#   e.g. https://projectaiapi-xxxxx.azurewebsites.net/createRequestJSON

BACKEND_POST_URL = os.getenv("BACKEND_POST_URL", "").strip()

BACKEND_GET_URL  = os.getenv("BACKEND_GET_URL", "").strip()  # e.g. https://.../fetchResponse?response_id=

API_KEY_REQUIRED = os.getenv("API_KEY_REQUIRED", "false").lower() == "true"

API_KEY          = os.getenv("API_KEY", "")

# ---------- Config (synonyms + key→question map) ----------

# You can also move this into a JSON file if you prefer.

CFG: Dict[str, Any] = {

    "field_synonyms": {

        "request_id":  ["request_id", "result_key", "response_id", "id", "reqid", "requestid"],

        "name":        ["name", "full_name", "customer_name"],

        "email":       ["email", "email_id"],

        "phone_number":["phone_number", "phone", "mobile", "phoneNo", "phone_no"],

        "birth_date":  ["birth_date", "dob", "date"],

    },

    # For flat answer maps like {"who":"Self","gender":"Male"}

    "answer_key_to_question": {

        "who":    "Q1. Who are you?/question",

        "gender": "Q2. gender/question"

    }

}

# ---------- helpers ----------

def _require_backend():

    if not BACKEND_POST_URL:

        raise HTTPException(status_code=500, detail="BACKEND_POST_URL is not configured")

def _check_api_key(req: Request):

    if not API_KEY_REQUIRED:

        return

    if not API_KEY or req.headers.get("x-api-key") != API_KEY:

        raise HTTPException(status_code=401, detail="unauthorized")

def _flatten(data: Any, prefix: str = "", out: Dict[str, Any] | None = None) -> Dict[str, Any]:

    """Flatten nested dict/list into dotted keys so we can search loosely."""

    if out is None:

        out = {}

    if isinstance(data, dict):

        for k, v in data.items():

            _flatten(v, f"{prefix}.{k}" if prefix else k, out)

    elif isinstance(data, list):

        for i, v in enumerate(data):

            _flatten(v, f"{prefix}[{i}]", out)

    else:

        out[prefix] = data

    return out

def _pick_field(flat: Dict[str, Any], synonyms: List[str]) -> Any | None:

    """Return first value from flattened dict whose leaf key matches any synonym (case‑insensitive)."""

    # check leaf tokens (after last dot/bracket)

    syn_l = [s.lower() for s in synonyms]

    for k, v in flat.items():

        leaf = k.split(".")[-1].split("[")[0].lower()

        if leaf in syn_l:

            return v

    return None

def _strings(lst: List[Any]) -> List[str]:

    return [str(x) if x is not None else "" for x in lst]

def _normalize_answers(raw: Dict[str, Any], flat: Dict[str, Any]) -> Tuple[List[str], List[str]]:

    """

    Produce questions[] and answers[] from flexible shapes.

    Supported:

      1) {"answers":[{"question":"...","answer":"..."}]}

      2) {"answers":[{"question":"...","selectedOption":{"value":"..."}}]}

      3) {"answers":{"who":"Self","gender":"Male"}}  (uses answer_key_to_question map)

      4) {"qa":[{"q":"...","a":"..."}]}  (a common alt shape)

    """

    questions: List[str] = []

    answers:   List[str] = []

    # case 1/2: answers is a list of objects

    ans = raw.get("answers")

    if isinstance(ans, list):

        for item in ans:

            if not isinstance(item, dict):

                continue

            q = item.get("question") or item.get("q") or item.get("title")

            a = item.get("answer") or item.get("a")

            if a is None and isinstance(item.get("selectedOption"), dict):

                a = item["selectedOption"].get("value")

            if q is not None and a is not None:

                questions.append(str(q))

                answers.append(str(a))

    # case 3: answers is a dict map

    elif isinstance(ans, dict):

        key_to_q: Dict[str, str] = {k.lower(): v for k, v in CFG.get("answer_key_to_question", {}).items()}

        for k, v in ans.items():

            q = key_to_q.get(str(k).lower())

            if q:

                questions.append(q)

                # handle selectedOption-like shapes here too

                if isinstance(v, dict) and "value" in v:

                    answers.append(str(v["value"]))

                else:

                    answers.append(str(v))

    # case 4: "qa" list of {"q": "...","a":"..."}

    elif isinstance(raw.get("qa"), list):

        for item in raw["qa"]:

            if not isinstance(item, dict):

                continue

            q = item.get("q") or item.get("question")

            a = item.get("a") or item.get("answer")

            if q is not None and a is not None:

                questions.append(str(q)); answers.append(str(a))

    # As a final fallback, see if we have known flat keys -> questions (rare)

    if not questions and not answers:

        key_to_q = {k.lower(): v for k, v in CFG.get("answer_key_to_question", {}).items()}

        for k, v in flat.items():

            leaf = k.split(".")[-1].split("[")[0].lower()

            if leaf in key_to_q:

                questions.append(key_to_q[leaf])

                answers.append(str(v))

    return questions, answers

def _normalize_to_backend_json(payload: Dict[str, Any]) -> Dict[str, Any]:

    """Convert any flex JSON to the exact JSON body expected by createRequestJSON."""

    flat = _flatten(payload)

    syn = CFG["field_synonyms"]

    request_id  = _pick_field(flat, syn["request_id"])   or ""

    name        = _pick_field(flat, syn["name"])         or ""

    email       = _pick_field(flat, syn["email"])        or ""

    phone       = _pick_field(flat, syn["phone_number"]) or ""

    birth_date  = _pick_field(flat, syn["birth_date"])   or ""

    questions, answers = _normalize_answers(payload, flat)

    return {

        "request_id":   str(request_id),

        "name":         str(name),

        "email":        str(email),

        "phone_number": str(phone),

        "birth_date":   str(birth_date),

        "questions":    _strings(questions),

        "answers":      _strings(answers),

    }

# ---------- routes ----------

@app.get("/")

def root():

    return {

        "ok": True,

        "expects": "createRequestJSON (normalized)",

        "backend_post_url_set": bool(BACKEND_POST_URL),

        "backend_get_url_set": bool(BACKEND_GET_URL),

        "api_key_required": API_KEY_REQUIRED,

        "version": "1.0.0"

    }

@app.get("/health")

def health():

    return {"ok": True}

@app.post("/ingest")

async def ingest(req: Request, preview: bool = Query(False, description="Return normalized JSON without calling backend"),

                 echo: bool = Query(False, description="Return backend status + body")):

    _check_api_key(req)

    _require_backend()

    try:

        raw = await req.json()

        if not isinstance(raw, dict):

            raise ValueError("Body must be a JSON object")

    except Exception as e:

        raise HTTPException(status_code=400, detail=f"Invalid JSON: {e}")

    normalized = _normalize_to_backend_json(raw)

    # quick sanity checks

    missing = [k for k in ["request_id","name","email"] if not normalized.get(k)]

    if missing:

        log.warning("Normalized JSON missing fields: %s", missing)

    if preview:

        # Don't call backend, just show what we'd send

        return JSONResponse(normalized)

    # Call backend /createRequestJSON

    try:

        async with httpx.AsyncClient(timeout=30) as client:

            resp = await client.post(BACKEND_POST_URL, json=normalized)

    except httpx.RequestError as e:

        return JSONResponse(

            status_code=502,

            content={"detail": {"adapter_error": "cannot_reach_backend", "msg": str(e)}},

        )

    body_txt = resp.text

    if echo:

        return JSONResponse(

            status_code=200,

            content={"normalized": normalized, "backend_status": resp.status_code, "backend_body": body_txt},

        )

    # Pass-thru on success; bubble backend errors as 502

    if 200 <= resp.status_code < 300:

        # Backend usually returns JSON; hand it back as-is

        try:

            return JSONResponse(json.loads(body_txt))

        except Exception:

            return JSONResponse({"body": body_txt, "backend_status": resp.status_code})

    return JSONResponse(

        status_code=502,

        content={"detail": {"backend_status": resp.status_code, "body": body_txt, "normalized": normalized}},

    )
 
