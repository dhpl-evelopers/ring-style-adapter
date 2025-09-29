import os
import json
import time
import uuid
from typing import Any, Dict, Optional

import requests
from flask import Flask, request, jsonify, g
from werkzeug.middleware.proxy_fix import ProxyFix
from xml.etree.ElementTree import Element, SubElement, tostring

# ─────────── Config (env) ───────────

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
