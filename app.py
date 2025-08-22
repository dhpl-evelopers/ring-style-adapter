from fastapi import FastAPI, Request, HTTPException

from fastapi.responses import JSONResponse, PlainTextResponse

import os, json, xmltodict, httpx

app = FastAPI(title="Ring Style Adapter", version="2.1.0")

# --- config / helpers ---------------------------------------------------------

BACKEND_POST_URL = os.getenv("BACKEND_POST_URL", "").strip()

BACKEND_GET_URL  = os.getenv("BACKEND_GET_URL", "").strip()

API_KEY_REQUIRED = os.getenv("API_KEY_REQUIRED", "false").lower() == "true"

API_KEY          = os.getenv("API_KEY", "").strip()

def _json_safe(obj):

    try:

        json.dumps(obj)

        return obj

    except Exception:

        return {"raw": str(obj)}

def _map_incoming(payload: dict) -> dict:

    """

    Very simple example mapper that matches the sample you shared.

    Replace/extend this with your existing mapping logic if needed.

    """

    # Expecting:

    # {

    #   "sessionId": "...",

    #   "customer": {"name": "...", "mobile": "..."},

    #   "answers": [{"id":"q1_style","value":"..."}, ...]

    # }

    answers = {a.get("id"): a.get("value") for a in payload.get("answers", [])}

    return {

        "session_id": payload.get("sessionId"),

        "customer": {

            "name":   payload.get("customer", {}).get("name"),

            "mobile": payload.get("customer", {}).get("mobile"),

        },

        "preferences": {

            "style":      answers.get("q1_style"),

            "metal":      (answers.get("q2_metal") or "").lower() or None,

            "budget":     int(answers["q3_budget"]) if str(answers.get("q3_budget", "")).isdigit() else None,

            "ring_size":  answers.get("q4_size"),

            "profile":    answers.get("q5_profile"),

        }

    }

def _forward_headers(req: Request) -> dict:

    # Start from client Accept, default to JSON

    accept = req.headers.get("accept", "application/json")

    headers = {"Accept": accept}

    if API_KEY_REQUIRED and API_KEY:

        headers["x-api-key"] = API_KEY

    return headers

# --- endpoints ----------------------------------------------------------------

@app.post("/adapter")

async def adapter_adapter(request: Request):

    if not BACKEND_POST_URL:

        raise HTTPException(status_code=500, detail="BACKEND_POST_URL is not configured.")

    # 1) Parse inbound (JSON or XML)

    content_type = request.headers.get("content-type", "").lower()

    try:

        if "xml" in content_type:

            raw = await request.body()

            incoming = xmltodict.parse(raw)

            # If your XML has a root, adjust extraction here

        else:

            incoming = await request.json()

    except Exception as e:

        raise HTTPException(status_code=400, detail=f"Invalid request body: {e}")

    # 2) Map inbound to backend schema

    try:

        backend_payload = _map_incoming(incoming)

    except Exception as e:

        raise HTTPException(status_code=400, detail=f"Mapping error: {e}")

    # 3) POST to backend (never overwrite the HTTP response object)

    timeout = httpx.Timeout(float(os.getenv("FORWARD_TIMEOUT", "30")))

    f_headers = _forward_headers(request)

    try:

        async with httpx.AsyncClient(timeout=timeout) as client:

            resp = await client.post(

                BACKEND_POST_URL,

                json=backend_payload,

                headers=f_headers,

            )

    except httpx.RequestError as e:

        # Network / DNS / TLS errors

        raise HTTPException(status_code=502, detail=f"Backend not reachable: {e}") from e

    # Keep raw response values for diagnostics

    status_code = resp.status_code

    raw_text    = resp.text

    # 4) Try to parse backend JSON; fall back to raw text

    try:

        backend_json = resp.json()

    except Exception:

        backend_json = {"raw": raw_text}

    # 5) (Optional) map backend_json to outward schema.

    # For now, just mirror what backend returned.

    outward = _json_safe(backend_json)

    # 6) Mirror client Accept (default JSON)

    accept = request.headers.get("accept", "application/json").lower()

    if "xml" in accept:

        # If you actually need XML outward, convert here

        # and return PlainTextResponse with 'application/xml'

        xml_str = xmltodict.unparse({"BackendResponse": outward}, pretty=True)

        return PlainTextResponse(xml_str, status_code=status_code, media_type="application/xml")

    return JSONResponse(outward, status_code=status_code)
 
