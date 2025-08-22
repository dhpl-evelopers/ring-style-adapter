import os
import json
from fastapi import FastAPI, Request
import uvicorn
import xmltodict
from dicttoxml import dicttoxml
import os, json, logging, uuid, hashlib
from typing import Any, Dict, List, Optional, Tuple

import httpx
from fastapi import FastAPI, Request, HTTPException, Body
from fastapi.responses import PlainTextResponse

# ---------------- Logging ----------------
log = logging.getLogger("adapter")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ---------------- FastAPI ----------------
app = FastAPI(title="Ring Style Adapter", version="2.0.0")

# ---------------- Environment ----------------
# Set these in Azure App Service > Configuration
BACKEND_POST_URL = os.getenv("BACKEND_POST_URL", "").strip()   # e.g. https://<your-backend>/createRequest
API_KEY_REQUIRED = os.getenv("API_KEY_REQUIRED", "false").lower() == "true"
API_KEY          = os.getenv("API_KEY", "")

# ---------------- Load mapping.config.json ----------------
CFG_PATH = os.path.join(os.path.dirname(_file_), "mapping.config.json")
try:
    with open(CFG_PATH, "r", encoding="utf-8") as f:
        CFG = json.load(f)
except Exception as e:
    log.error("Failed to load mapping.config.json: %s", e)
    CFG = {}

# Canonical XML root expected by backend
XML_ROOT = CFG.get("xml_root", "data")

# Flexible field aliases the UI might send -> canonical keys for the backend XML
FIELD_ALIASES: Dict[str, str] = {k.lower(): v for k, v in CFG.get("field_map", {}).items()}

# Default values if UI omits them
DEFAULTS: Dict[str, Any] = CFG.get("defaults", {})

# Optional static mapping for short answer keys -> full question text
# (You said “Yes” to generating ids, so questions can be free-form; this is only used if provided)
ANSWER_KEY_TO_QUESTION: Dict[str, str] = CFG.get("answer_key_to_question", {})

# ---------------- Helpers ----------------
def _require_backend():
    if not BACKEND_POST_URL:
        raise HTTPException(status_code=500, detail="BACKEND_POST_URL not configured")

def _check_api_key(req: Request):
    if not API_KEY_REQUIRED:
        return
    if not API_KEY or req.headers.get("x-api-key") != API_KEY:
        raise HTTPException(status_code=401, detail="unauthorized")

def _norm_phone(v: str) -> str:
    if not isinstance(v, str):
        return str(v or "")
    return v.strip()

def _norm_date(v: str) -> str:
    # Pass-through; your backend accepts both dd/mm/yyyy and yyyy-mm-dd based on earlier logs.
    return (v or "").strip()

def _canon_key(k: str) -> str:
    lk = (k or "").strip().lower()
    return FIELD_ALIASES.get(lk, lk)

def _gather_identity(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Pull name, email, phone, birth_date, result_key, quizId, id, etc. from any flexible shape.
    """
    out = dict(DEFAULTS)

    # flatten one level
    def visit(prefix, node):
        if isinstance(node, dict):
            for kk, vv in node.items():
                visit(f"{prefix}.{kk}" if prefix else kk, vv)
        else:
            out_key = _canon_key(prefix.split(".")[-1])
            out[out_key] = vv_to_str(vv=node)

    def vv_to_str(vv: Any) -> Any:
        if vv is None:
            return ""
        return vv

    visit("", raw)

    # Normalize common identity fields
    if "phone_number" in out and not out.get("phoneNumber"):
        out["phoneNumber"] = _norm_phone(out.pop("phone_number"))
    if "dob" in out and not out.get("dateOfBirth"):
        out["dateOfBirth"] = _norm_date(out.pop("dob"))
    if "birth_date" in out and not out.get("dateOfBirth"):
        out["dateOfBirth"] = _norm_date(out.pop("birth_date"))
    if "fullName" not in out and "name" in out:
        out["fullName"] = (out.get("name") or "").strip()

    # Ensure required keys exist (empty allowed – backend handles)
    for k in ["id","quizId","fullName","email","phoneNumber","dateOfBirth","resultKey",
              "termsConditions","website","organization","address1","address2","city",
              "country","state","zipCode"]:
        out.setdefault(k, "")

    # Make some sensible defaults if missing
    if not out["id"]:
        out["id"] = str(uuid.uuid4().int)[:6]  # short numeric-ish id
    if not out["quizId"]:
        out["quizId"] = "12952"  # fallback; adjust if you have a fixed quiz
    if out["termsConditions"] in ("", None):
        out["termsConditions"] = 0

    return out

def _mk_qid(question_text: str, base: int = 70000) -> int:
    """
    Stable, deterministic questionId from question text.
    You said “Yes” to generating ids; this will be consistent per text, no config required.
    """
    h = hashlib.blake2b(question_text.encode("utf-8"), digest_size=4).hexdigest()
    return base + (int(h, 16) % 10000)

def _mk_option_id(answer_value: str, base: int = 290000) -> int:
    """
    Stable option id from answer value (deterministic).
    """
    h = hashlib.blake2b((answer_value or "").encode("utf-8"), digest_size=4).hexdigest()
    return base + (int(h, 16) % 100000)

def _from_answers_only(answers: List[Any], questions: Optional[List[str]]) -> List[Dict[str, Any]]:
    """
    Build questionAnswers from either:
      - answers only + parallel 'questions' list (same length), or
      - answers only (we fallback to pos-based generic labels Q1..., Q2...)
    """
    qa = []
    for idx, ans in enumerate(answers):
        if isinstance(ans, dict) and "question" in ans and "selectedOption" in ans:
            # already QnA block
            question = str(ans.get("question") or "").strip()
            sel = ans.get("selectedOption") or {}
            value = sel.get("value")
            img = sel.get("image")
        else:
            # answers-only; infer question text
            if questions and idx < len(questions):
                qtxt = str(questions[idx] or "").strip()
            else:
                # try map via ANSWER_KEY_TO_QUESTION if ans is a short key
                key = str(ans or "").strip().lower()
                qtxt = ANSWER_KEY_TO_QUESTION.get(key, f"Q{idx+1}. (Auto)")

            question = qtxt
            value = ans
            img = None

        qid = _mk_qid(question)
        opt_id = _mk_option_id(str(value or ""))

        qa.append({
            "question": question,
            "questionId": qid,
            "selectedOption": {
                "id": opt_id,
                "value": value,
                "image": img
            }
        })
    return qa

def _from_questionAnswers(questionAnswers: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Normalize already well-formed 'questionAnswers' into the shape backend expects.
    Auto-fill missing questionId/selectedOption.id deterministically.
    """
    qa = []
    for item in questionAnswers:
        q = str(item.get("question") or "").strip()
        qid = item.get("questionId") or _mk_qid(q)
        sel = item.get("selectedOption") or {}
        val = sel.get("value")
        img = sel.get("image")
        opt_id = sel.get("id") or _mk_option_id(str(val or ""))

        qa.append({
            "question": q,
            "questionId": int(qid),
            "selectedOption": {
                "id": int(opt_id),
                "value": val,
                "image": img
            }
        })
    return qa

def _build_xml(identity: Dict[str, Any], questionAnswers: List[Dict[str, Any]]) -> str:
    """
    Build the exact XML your backend log stream shows (no dicttoxml; manual to control tags).
    """
    # Helper to escape basic XML
    def esc(s: Any) -> str:
        x = "" if s is None else str(s)
        return (x.replace("&","&amp;")
                 .replace("<","&lt;")
                 .replace(">","&gt;")
                 .replace('"',"&quot;")
                 .replace("'","&apos;"))

    # Identity block exactly like your backend expects (lowerCamelCase keys)
    parts = []
    parts.append(f"<{XML_ROOT}>")
    parts.append(f"<id>{esc(identity.get('id'))}</id>")
    parts.append(f"<quiz_id>{esc(identity.get('quizId'))}</quiz_id>")
    parts.append(f"<full_name>{esc(identity.get('fullName'))}</full_name>")
    parts.append(f"<email>{esc(identity.get('email'))}</email>")
    parts.append(f"<phone_number>{esc(identity.get('phoneNumber'))}</phone_number>")
    parts.append(f"<date>{esc(identity.get('dateOfBirth'))}</date>")
    parts.append(f"<terms_conditions>{esc(identity.get('termsConditions'))}</terms_conditions>")
    parts.append(f"<website>{esc(identity.get('website'))}</website>")
    parts.append(f"<organisation>{esc(identity.get('organization'))}</organisation>")
    parts.append(f"<address1>{esc(identity.get('address1'))}</address1>")
    parts.append(f"<address2>{esc(identity.get('address2'))}</address2>")
    parts.append(f"<city>{esc(identity.get('city'))}</city>")
    parts.append(f"<country>{esc(identity.get('country'))}</country>")
    parts.append(f"<state>{esc(identity.get('state'))}</state>")
    parts.append(f"<zip_code>{esc(identity.get('zipCode'))}</zip_code>")

    # Result key (your backend treats as request_id)
    parts.append(f"<result_key>{esc(identity.get('resultKey'))}</result_key>")

    # Empty arrays the backend tolerates
    parts.append("<custom_inputs>[]</custom_inputs>")
    parts.append("<products>[]</products>")

    # questionAnswers block — the backend logs show this as a stringified array; we’ll format it like that
    qa_entries = []
    for qa in questionAnswers:
        q = esc(qa.get("question"))
        qid = qa.get("questionId")
        sel = qa.get("selectedOption") or {}
        sid = sel.get("id")
        sval = esc(sel.get("value"))
        simg = esc(sel.get("image"))
        qa_entries.append(
            "{&apos;question&apos;: &apos;%s&apos;,&apos;questionId&apos;: %s,&apos;isMasterQuestion&apos;: false,"
            "&apos;questionType&apos;: &apos;singleAnswer&apos;,"
            "&apos;selectedOption&apos;: {&apos;id&apos;: %s,&apos;value&apos;: &apos;%s&apos;,&apos;image&apos;: &apos;%s&apos;}}"
            % (q, int(qid), int(sid), sval, simg)
        )
    parts.append("<questionAnswers>[{}]</questionAnswers>".format(", ".join(qa_entries)))

    parts.append("</{0}>".format(XML_ROOT))
    # Event wrapper you showed in logs often sits outside <data>, but backend accepts payload with just <data>.
    return "".join(parts)

def _extract_qna(raw: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """
    Accept these flexible shapes:
      A) { "answers": [...], "questions": [...], <identity fields> }
      B) { "questionAnswers": [ { question, questionId?, selectedOption{ id?, value, image? } } ], <identity fields> }
      C) Flat key/value: { "gender":"Female", "purpose":"Daily wear", ... } + optional 'answer_key_to_question' mapping
    Always returns (identity, questionAnswers[])
    """
    identity = _gather_identity(raw)

    # Case A
    if isinstance(raw.get("answers"), list):
        qa = _from_answers_only(raw["answers"], raw.get("questions"))
        return identity, qa

    # Case B
    if isinstance(raw.get("questionAnswers"), list):
        qa = _from_questionAnswers(raw["questionAnswers"])
        return identity, qa

    # Case C: dictionary of short keys -> answers
    # Use mapping: answer_key_to_question; otherwise derive generic Qx text
    if raw:
        items = []
        for idx, (k, v) in enumerate(raw.items()):
            if k in ("answers","questions","questionAnswers"):
                continue
            if isinstance(v, (dict, list)):
                continue
            qtxt = ANSWER_KEY_TO_QUESTION.get(str(k).lower(), f"Q{idx+1}. {str(k)}")
            items.append(v)
        qa = _from_answers_only(items, None)
        return identity, qa

    # fallback
    return identity, []

# ---------------- Routes ----------------

@app.get("/", response_class=PlainTextResponse)
async def root():
    return "Ring Style Adapter is up"

@app.post("/ingest")  # Flexible UI entry — ALWAYS posts to backend
async def ingest(raw: dict = Body(...), req: Request = None):
    _check_api_key(req)
    _require_backend()

    log.info("Inbound raw: %s", json.dumps(raw, ensure_ascii=False)[:2000])

    identity, qas = _extract_qna(raw)
    if not identity.get("resultKey"):
        # Generate a resultKey if UI didn’t send it
        identity["resultKey"] = f"{uuid.uuid4()}_ms"

    xml_payload = _build_xml(identity, qas)
    log.info("XML to backend (first 2KB): %s", xml_payload[:2000])

    # Always call backend /createRequest (XML)
    headers = {"Content-Type": "application/xml"}
    if API_KEY_REQUIRED and API_KEY:
        headers["x-api-key"] = API_KEY

    try:
        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.post(BACKEND_POST_URL, content=xml_payload.encode("utf-8"), headers=headers)
        log.info("Backend status: %s", resp.status_code)
        text = resp.text
        log.info("Backend body (first 2KB): %s", text[:2000])

        # If backend throws, surface it so you see it in Swagger/Postman
        if resp.status_code >= 400:
            raise HTTPException(status_code=resp.status_code, detail={"backend_error": text})

        # Success: give you identity/resultKey and backend echo
        return {
            "ok": True,
            "result_key": identity["resultKey"],
            "forwarded_to": BACKEND_POST_URL,
            "backend_status": resp.status_code,
            "backend_body": text
        }

    except httpx.RequestError as e:
        log.exception("Network error calling backend")
        raise HTTPException(status_code=502, detail=f"Network error calling backend: {e}") from e
    except Exception as e:
        log.exception("Unhandled error")
        raise HTTPException(status_code=500, detail=f"Adapter error: {e}") from e
