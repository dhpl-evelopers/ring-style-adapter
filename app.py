import os
import json
import time
import uuid
from typing import Any, Dict, Optional

import requests
from flask import Flask, request, jsonify, g
from werkzeug.middleware.proxy_fix import ProxyFix
from xml.etree.ElementTree import Element, SubElement, tostring

# ─────────── Config (env) ───────────aimport os
import json
import time
import uuid
import logging
from typing import Any, Dict, List, Optional

import requests
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from xml.etree.ElementTree import Element, SubElement, tostring

# ─────────────────────────────────────────────
# Config (env vars)
# ─────────────────────────────────────────────

def _getenv(name: str, default: Optional[str] = None, required: bool = False) -> str:
    v = os.getenv(name, default)
    if required and not (v and str(v).strip()):
        raise RuntimeError(f"Missing required env var: {name}")
    return (v or "").strip()

BACKEND_BASE_URL   = _getenv("BACKEND_BASE_URL", required=True).rstrip("/")
CREATE_PATH        = _getenv("BACKEND_CREATE_PATH", "/createRequest")
FETCH_PATH         = _getenv("BACKEND_FETCH_PATH", "/fetchResponse")
BACKEND_TIMEOUT_S  = int(_getenv("BACKEND_TIMEOUT_SEC", "25"))
XML_QA_FORMAT      = _getenv("XML_QA_FORMAT", "json_text").lower()  # "json_text" | "elements"
BACKEND_AUTH_KEY   = _getenv("BACKEND_AUTH_KEY", "")  # optional (Functions key or API key)
LOG_LEVEL          = _getenv("LOG_LEVEL", "INFO").upper()

# ─────────────────────────────────────────────
# App + logging + HTTP session
# ─────────────────────────────────────────────
app = FastAPI()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("adapter")

HTTP = requests.Session()

# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def _ensure_result_key(v: Optional[str]) -> str:
    v = (v or "").strip()
    return v if v else f"rk-{int(time.time()*1000)}-{uuid.uuid4().hex[:6]}"

def _build_xml_payload(body: Dict[str, Any]) -> str:
    """
    Build the XML expected by the backend.
    Default (XML_QA_FORMAT=json_text): embed the Q&A list as a JSON string inside <questionAnswers>.
    Alternate (XML_QA_FORMAT=elements): emit one <item> per Q/A.
    """
    root = Element("Request")

    # Required/expected user fields
    SubElement(root, "full_name").text    = str(body.get("full_name", "") or body.get("name", ""))
    SubElement(root, "email").text        = str(body.get("email", ""))
    SubElement(root, "phone_number").text = str(body.get("phone_number", "") or body.get("contact", ""))
    SubElement(root, "result_key").text   = _ensure_result_key(body.get("result_key"))
    SubElement(root, "date").text         = str(body.get("birth_date", "") or body.get("dob", "") or body.get("date", ""))

    qas: List[Dict[str, Any]] = body.get("questionAnswers") or []
    qa_node = SubElement(root, "questionAnswers")

    if XML_QA_FORMAT == "elements":
        # <questionAnswers><item><question>..</question><selectedOption><value>..</value></selectedOption></item>...</questionAnswers>
        for qa in qas:
            it = SubElement(qa_node, "item")
            SubElement(it, "question").text = str(qa.get("question", ""))
            so = SubElement(it, "selectedOption")
            SubElement(so, "value").text = str(qa.get("answer", ""))
    else:
        # Default: JSON array string inside the node (this is what your backend is parsing)
        qa_payload = [
            {"question": str(qa.get("question", "")),
             "selectedOption": {"value": str(qa.get("answer", ""))}}
            for qa in qas
        ]
        qa_node.text = json.dumps(qa_payload, ensure_ascii=False)

    # Add correlation + timestamp (useful in logs)
    SubElement(root, "correlation_id").text = str(uuid.uuid4())
    SubElement(root, "timestamp_utc").text  = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    xml_str = tostring(root, encoding="unicode")
    log.debug("XML built: %s", xml_str[:500])
    return xml_str

def _extract_response_id(create_json: Optional[Dict[str, Any]]) -> Optional[str]:
    if not isinstance(create_json, dict):
        return None
    for k in ("response_id","ResponseId","responseId","id","Id","code","Code","requestId","request_id"):
        v = create_json.get(k)
        if v and str(v).strip():
            return str(v).strip()
    return None

def _call_create(xml_body: str) -> requests.Response:
    url = f"{BACKEND_BASE_URL}{CREATE_PATH}"
    headers = {"Content-Type": "application/xml", "Accept": "application/json, */*"}
    if BACKEND_AUTH_KEY:
        headers["x-functions-key"] = BACKEND_AUTH_KEY
    log.info("POST %s", url)
    return HTTP.post(url, data=xml_body.encode("utf-8"), headers=headers, timeout=BACKEND_TIMEOUT_S)

def _poll_fetch(response_id: str) -> Dict[str, Any]:
    url = f"{BACKEND_BASE_URL}{FETCH_PATH}"
    headers = {"Accept": "application/json, */*"}
    if BACKEND_AUTH_KEY:
        headers["x-functions-key"] = BACKEND_AUTH_KEY

    deadline = time.time() + BACKEND_TIMEOUT_S
    last: Any = None

    while time.time() < deadline:
        r = HTTP.get(url, params={"response_id": response_id}, headers=headers, timeout=BACKEND_TIMEOUT_S)
        ctype = (r.headers.get("Content-Type") or "").lower()

        # 202 = still processing; back off a bit
        if r.status_code == 202:
            time.sleep(0.6)
            continue

        # Got JSON; check for completion
        if r.ok and "application/json" in ctype:
            try:
                data = r.json()
            except Exception:
                return {"backend_raw": r.text, "status_code": r.status_code}
            last = data
            status = str(data.get("status") or data.get("Status") or "").lower()
            if status in ("done","completed","ok","success","ready","finished") or data.get("done") is True or "data" in data:
                return {"backend_final": data}
            time.sleep(0.6)
            continue

        # Non-JSON or error; return raw
        return {"backend_raw": r.text, "status_code": r.status_code}

    return {"backend_fetch_timeout": True, "last": last}

# ─────────────────────────────────────────────
# Routes
# ─────────────────────────────────────────────

@app.get("/health")
def health() -> Dict[str, Any]:
    return {
        "status": "ok",
        "service": "adapter",
        "backend": BACKEND_BASE_URL,
        "qa_format": XML_QA_FORMAT
    }

@app.post("/adapter")
async def adapter(req: Request):
    # Parse JSON
    try:
        body = await req.json()
        if not isinstance(body, dict):
            raise ValueError("Body must be a JSON object")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON: {e}")

    # Input must include an array of questionAnswers
    qas = body.get("questionAnswers")
    if not isinstance(qas, list):
        raise HTTPException(status_code=400, detail="Missing or invalid 'questionAnswers' array")

    # Build XML
    xml_body = _build_xml_payload(body)
    log.info("XML payload length: %s", len(xml_body))

    # Call backend /createRequest
    try:
        r = _call_create(xml_body)
    except Exception as e:
        log.exception("createRequest failed")
        raise HTTPException(status_code=502, detail=f"Backend unreachable: {e}")

    # If createRequest failed -> 5xx to client
    if r.status_code >= 400:
        return JSONResponse(status_code=502, content={
            "status": "error",
            "stage": "createRequest",
            "http_status": r.status_code,
            "body": r.text
        })

    # If backend already returns final JSON, forward it
    create_json: Optional[Dict[str, Any]] = None
    try:
        create_json = r.json()
    except Exception:
        # Not JSON; return raw response body
        return JSONResponse(content={
            "status": "ok",
            "backend": {"backend_raw": r.text}
        })

    # Determine response id to poll with
    response_id = _extract_response_id(create_json) or _ensure_result_key(body.get("result_key"))

    # If create already contains final data, return it
    status = str(create_json.get("status") or create_json.get("Status") or "").lower()
    if "data" in create_json or status in ("done","completed","ok","success","ready","finished") or create_json.get("done") is True:
        return JSONResponse(content={
            "status": "ok",
            "request_id": response_id,
            "backend": {"backend_final": create_json}
        })

    # Poll /fetchResponse
    polled = _poll_fetch(response_id)

    return JSONResponse(content={
        "status": "ok",
        "request_id": response_id,
        # Echo back normalized Q&A so you can confirm what we sent
        "normalized": [
            {"question": str(qa.get("question","")), "answer": str(qa.get("answer",""))}
            for qa in (qas or [])
        ],
        "backend": polled
    })

# ─────────────────────────────────────────────
# Local main (so you can: python app.py)
# ─────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8000"))
    log.info("Starting adapter on http://0.0.0.0:%s  (backend=%s, qa_format=%s)", port, BACKEND_BASE_URL, XML_QA_FORMAT)
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=True)


def _req_env(name: str, default: Optional[str] = None) -> str:
    v = os.getenv(name, default)
    if v is None or str(v).strip() == "":
        raise RuntimeError(f"Missing required env var: {name}")
    return str(v).strip()

BACKEND_BASE_URL  = _req_env("BACKEND_BASE_URL").rstrip("/")
CREATE_PATH       = os.getenv("BACKEND_CREATE_PATH", "/createRequest")
FETCH_PATH        = os.getenv("BACKEND_FETCH_PATH", "/fetchResponse")
BACKEND_TIMEOUT_S = int(os.getenv("BACKEND_TIMEOUT_SEC", "25"))

# ─────────── App ───────────

app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_port=1, x_prefix=1)  # type: ignore
session = requests.Session()

# ─────────── Helpers ───────────

def _build_xml_payload(body: Dict[str, Any]) -> str:
    """
    Build the XML expected by the backend.
    We keep `questionAnswers` as a JSON string inside the XML (based on your backend’s current behavior).
    """
    root = Element("Request")

    # Required user fields
    SubElement(root, "full_name").text    = str(body.get("full_name", "") or body.get("name", ""))
    SubElement(root, "email").text        = str(body.get("email", ""))
    SubElement(root, "phone_number").text = str(body.get("phone_number", "") or body.get("contact", ""))
    SubElement(root, "result_key").text   = str(body.get("result_key", "") or body.get("request_id", "") or str(uuid.uuid4()))
    SubElement(root, "date").text         = str(body.get("birth_date", "") or body.get("dob", "") or body.get("date", ""))

    # Keep original Q&A list as JSON string (the backend currently parses it this way)
    qas = body.get("questionAnswers") or []
    SubElement(root, "questionAnswers").text = json.dumps(qas, ensure_ascii=False)

    # Optional: add a correlation id / timestamp for traceability
    SubElement(root, "correlation_id").text = request.headers.get("x-request-id", str(uuid.uuid4()))
    SubElement(root, "timestamp_utc").text  = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    return tostring(root, encoding="unicode")

def _extract_response_id(obj: Optional[Dict[str, Any]]) -> Optional[str]:
    if not isinstance(obj, dict):
        return None
    for k in ("response_id", "ResponseId", "responseId", "id", "requestId", "request_id", "code"):
        v = obj.get(k)
        if v:
            return str(v)
    return None

def _poll_fetch(response_id: str) -> Dict[str, Any]:
    """Poll /fetchResponse until done or timeout window is reached."""
    fetch_url = f"{BACKEND_BASE_URL}{FETCH_PATH}"
    deadline = time.time() + BACKEND_TIMEOUT_S
    last: Any = None

    while time.time() < deadline:
        r = session.get(fetch_url, params={"response_id": response_id}, timeout=BACKEND_TIMEOUT_S)
        ctype = (r.headers.get("Content-Type") or "").lower()

        if r.status_code == 202:
            time.sleep(0.7)
            continue

        if r.ok and "application/json" in ctype:
            data = r.json()
            # Heuristic “done” checks common in your backend
            status = str(data.get("status") or data.get("Status") or "").lower()
            if status in ("done", "completed", "success", "ok", "ready", "finished") or data.get("done") is True:
                return {"backend_final": data}
            last = data
            time.sleep(0.7)
            continue

        # Non-JSON or non-200/202: return raw body so you can see what the backend sent
        return {"backend_raw": r.text, "status_code": r.status_code}

    return {"backend_fetch_timeout": True, "last": last}

# ─────────── Routes ───────────

@app.before_request
def _add_cid():
    g.cid = request.headers.get("x-request-id") or str(uuid.uuid4())

@app.after_request
def _add_headers(resp):
    resp.headers["X-Request-Id"] = g.get("cid", "")
    resp.headers["X-Content-Type-Options"] = "nosniff"
    return resp

@app.get("/health")
def health():
    return jsonify({"status": "ok", "service": "adapter", "backend": BACKEND_BASE_URL})

@app.post("/adapter")
def adapter():
    # 1) parse JSON
    try:
        body = request.get_json(force=True)
        if not isinstance(body, dict):
            return jsonify({"status": "error", "error": "Body must be a JSON object", "correlation_id": g.cid}), 400
    except Exception as e:
        return jsonify({"status": "error", "error": f"Invalid JSON: {e}", "correlation_id": g.cid}), 400

    # 2) build XML
    xml_payload = _build_xml_payload(body)

    # 3) send to backend /createRequest
    create_url = f"{BACKEND_BASE_URL}{CREATE_PATH}"
    try:
        r = session.post(create_url, data=xml_payload.encode("utf-8"),
                         headers={"Content-Type": "application/xml", "Accept": "application/json, */*"},
                         timeout=BACKEND_TIMEOUT_S)
    except Exception as e:
        return jsonify({"status": "error", "error": f"Backend unreachable: {e}", "correlation_id": g.cid}), 502

    # 4) handle immediate failure
    if r.status_code >= 400:
        return jsonify({
            "status": "error",
            "error": f"createRequest failed: {r.status_code}",
            "body": r.text,
            "correlation_id": g.cid
        }), 502

    # 5) if create already returns final JSON, just forward it
    create_json: Optional[Dict[str, Any]] = None
    try:
        create_json = r.json()
    except Exception:
        # Not JSON – return raw for debugging
        return jsonify({
            "status": "ok",
            "backend": {"backend_raw": r.text},
            "correlation_id": g.cid
        })

    # If the create call is already final, return it
    status = str(create_json.get("status") or create_json.get("Status") or "").lower()
    if "data" in create_json or status in ("done", "completed", "success", "ok", "ready", "finished") or create_json.get("done") is True:
        return jsonify({
            "status": "ok",
            "backend": {"backend_final": create_json},
            "correlation_id": g.cid
        })

    # 6) otherwise poll /fetchResponse using the response id (fallback to result_key)
    response_id = _extract_response_id(create_json) or str(body.get("result_key", ""))
    if not response_id:
        # no way to poll – return what we have
        return jsonify({
            "status": "ok",
            "backend": {"backend_create": create_json},
            "correlation_id": g.cid
        })

    polled = _poll_fetch(response_id)

    # 7) final envelope (include request echo to help debugging)
    return jsonify({
        "status": "ok",
        "request_id": body.get("result_key") or body.get("request_id"),
        "backend": polled,
        "correlation_id": g.cid
    })

# ─────────── Local run ───────────
if __name__ == "__main__":
    # For local testing:  http://127.0.0.1:8000/adapter
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
