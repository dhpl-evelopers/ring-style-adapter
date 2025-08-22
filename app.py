import os

import json

import logging

from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Request, HTTPException

from fastapi.responses import PlainTextResponse, JSONResponse

import httpx

import xml.etree.ElementTree as ET

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

log = logging.getLogger("adapter")

app = FastAPI(title="Ring Style Adapter", version="2.1.0")

# --- Env ---

BACKEND_POST_URL = os.getenv("BACKEND_POST_URL", "").strip()

BACKEND_GET_URL  = os.getenv("BACKEND_GET_URL", "").strip()

API_KEY_REQUIRED = os.getenv("API_KEY_REQUIRED", "false").lower() == "true"

API_KEY_HEADER   = os.getenv("API_KEY", "").strip()

# --- Load mapping.config.json (shipped alongside app.py) ---

CFG_PATH = os.path.join(os.path.dirname(__file__), "mapping.config.json")

try:

    with open(CFG_PATH, "r", encoding="utf-8") as f:

        CFG: Dict[str, Any] = json.load(f)

except Exception as e:

    log.error("Failed to load mapping.config.json: %s", e)

    CFG = {}

# ---------------------------

# Helpers

# ---------------------------

def _dig(d: Dict[str, Any], path: Optional[str]) -> Any:

    """Safely get nested value via dotted path."""

    if not path:

        return d

    cur: Any = d

    for key in path.split("."):

        if isinstance(cur, dict) and key in cur:

            cur = cur[key]

        else:

            return None

    return cur

def _xml_text(elem: Optional[ET.Element]) -> str:

    return elem.text if elem is not None and elem.text is not None else ""

def ensure_xml(value: str) -> str:

    # Minimal escaping for XML text nodes

    return (value or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def make_backend_xml(payload: Dict[str, Any]) -> str:

    """

    Build the XML the backend expects:
<root>
<request_id>...</request_id>
<full_name>...</full_name>
<email>...</email>
<phone_number>...</phone_number>
<birth_date>...</birth_date>
<questionAnswers>
<question><id>..</id><value>..</value></question>*
</questionAnswers>
</root>

    """

    mapping = CFG.get("mapping", {})

    fields_map: Dict[str, Any] = mapping.get("fields", {})

    answers_path: str = mapping.get("answers_path", "answers")

    answer_id_key: str = mapping.get("answer_id_key", "id")

    answer_val_key: str = mapping.get("answer_value_key", "value")

    # 1) Pull top‑level fields safely (with defaults)

    request_id = _dig(payload, fields_map.get("request_id", {}).get("from", "sessionId")) or ""

    full_name  = _dig(payload, fields_map.get("full_name", {}).get("from", "customer.name")) or ""

    email      = _dig(payload, fields_map.get("email", {}).get("from", "")) or ""

    phone      = _dig(payload, fields_map.get("phone_number", {}).get("from", "customer.mobile")) or ""

    birth_date = _dig(payload, fields_map.get("birth_date", {}).get("from", "")) or ""

    # 2) Build QnA list

    answers_raw = _dig(payload, answers_path) or []

    qna: List[Dict[str, str]] = []

    if isinstance(answers_raw, list):

        for a in answers_raw:

            qid = (a or {}).get(answer_id_key, "")

            val = (a or {}).get(answer_val_key, "")

            if qid != "" or val != "":

                qna.append({"id": str(qid), "value": str(val)})

    # 3) Compose XML

    root = ET.Element("root")

    ET.SubElement(root, "request_id").text   = request_id

    ET.SubElement(root, "full_name").text    = full_name

    ET.SubElement(root, "email").text        = email

    ET.SubElement(root, "phone_number").text = phone

    ET.SubElement(root, "birth_date").text   = birth_date

    qa_root = ET.SubElement(root, "questionAnswers")

    for item in qna:

        q = ET.SubElement(qa_root, "question")

        ET.SubElement(q, "id").text    = item["id"]

        ET.SubElement(q, "value").text = item["value"]

    # Serialize compact XML string

    xml_bytes = ET.tostring(root, encoding="utf-8")

    return xml_bytes.decode("utf-8")

async def forward_to_backend(xml_payload: str) -> httpx.Response:

    if not BACKEND_POST_URL:

        raise HTTPException(status_code=500, detail="BACKEND_POST_URL is not configured.")

    headers = {

        "Content-Type": "application/xml",

        "Accept": "application/json",

    }

    if API_KEY_REQUIRED and API_KEY_HEADER:

        headers["x-api-key"] = API_KEY_HEADER

    timeout = httpx.Timeout(CFG.get("forward", {}).get("timeout_sec", 30.0))

    async with httpx.AsyncClient(timeout=timeout, verify=True) as client:

        return await client.post(BACKEND_POST_URL, content=xml_payload.encode("utf-8"), headers=headers)

# ---------------------------

# Routes

# ---------------------------

@app.get("/", response_class=PlainTextResponse)

async def root():

    return "Ring Style Adapter OK"

@app.get("/ping", response_class=PlainTextResponse)

async def ping():

    return "pong"

@app.post("/adapter")

async def adapter(request: Request):

    """

    Accepts JSON (preferred) or XML.

    - If JSON: map to backend XML using mapping.config.json and forward.

    - If XML: pass through to backend (but still ensure the required tags exist).

    """

    ctype = (request.headers.get("content-type") or "").split(";")[0].strip().lower()

    try:

        if ctype == "application/json":

            body: Dict[str, Any] = await request.json()

            xml_payload = make_backend_xml(body)

        elif ctype in ("application/xml", "text/xml"):

            # Pass‑through but make sure it is valid XML

            raw = await request.body()

            try:

                # Validate / normalize

                _ = ET.fromstring(raw.decode("utf-8"))

                xml_payload = raw.decode("utf-8")

            except ET.ParseError as e:

                raise HTTPException(status_code=400, detail=f"Invalid XML: {e}")

        else:

            raise HTTPException(status_code=415, detail="Unsupported Content-Type. Use application/json or application/xml.")

        log.info("Forwarding mapped XML to backend (%d bytes)", len(xml_payload))

        resp = await forward_to_backend(xml_payload)

        # Mirror backend response (assumed JSON)

        return JSONResponse(status_code=resp.status_code, content=resp.json() if resp.content else {})

    except HTTPException:

        raise

    except Exception as e:

        log.exception("Adapter error")

        raise HTTPException(status_code=500, detail=f"Adapter error: {e}")
log.info
This domain may be for sale!
 
