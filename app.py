from fastapi import FastAPI, Request, HTTPException

from fastapi.responses import JSONResponse, PlainTextResponse

import os

import httpx

import dicttoxml

app = FastAPI(title="Ring Style Adapter", version="0.1.1")

# ----- Config via environment (set these in Azure) -----

BACKEND_POST_URL = os.getenv("BACKEND_POST_URL", "").strip()

BACKEND_GET_URL  = os.getenv("BACKEND_GET_URL", "").strip()

API_KEY_REQUIRED = os.getenv("API_KEY_REQUIRED", "false").lower() == "true"

API_KEY          = os.getenv("API_KEY", "")

@app.get("/")

def root():

    return {

        "ok": True,

        "requires_api_key": API_KEY_REQUIRED,

        "has_backend_urls": bool(BACKEND_POST_URL and BACKEND_GET_URL),

        "post_url": bool(BACKEND_POST_URL),

        "get_url": bool(BACKEND_GET_URL),

    }

@app.get("/health")

def health():

    return {"ok": True}

def _check_api_key(req: Request):

    if not API_KEY_REQUIRED:

        return

    user_key = req.headers.get("x-api-key", "")

    if not API_KEY or user_key != API_KEY:

        raise HTTPException(status_code=401, detail="unauthorized")

@app.post("/ingest")

async def ingest(req: Request):

    """

    Accept arbitrary JSON, convert to XML, forward to your backend createRequest,

    and return backend response. If backend fails, bubble its status/body back.

    """

    _check_api_key(req)

    if not BACKEND_POST_URL:

        raise HTTPException(status_code=500, detail="BACKEND_POST_URL is not configured")

    try:

        payload = await req.json()

    except Exception:

        # if someone posts non-JSON by mistake

        payload = {}

    xml_bytes = dicttoxml.dicttoxml(payload, custom_root='root', attr_type=False)

    try:

        async with httpx.AsyncClient(timeout=30) as client:

            resp = await client.post(

                BACKEND_POST_URL,

                content=xml_bytes,

                headers={"Content-Type": "application/xml"},

            )

    except httpx.RequestError as e:

        # Adapter can't reach backend (DNS, SSL, timeout, etc.)

        return JSONResponse(

            status_code=502,

            content={"detail": {"adapter_error": "cannot_reach_backend", "msg": str(e)}},

        )

    # Surface backend response as-is; if backend returned a non-2xx,

    # forward that status with body so you can tell backend vs adapter issues.

    ct = resp.headers.get("content-type", "")

    body = resp.text

    if 200 <= resp.status_code < 300:

        # happy path

        if "xml" in ct:

            return PlainTextResponse(body, media_type="application/xml")

        return JSONResponse({"backend_status": resp.status_code, "body": body})

    # bubble up backend errors (do NOT hide them as a generic 500)

    return JSONResponse(

        status_code=502,  # 502 makes it clear the backend failed, not the adapter

        content={"detail": {"backend_status": resp.status_code, "body": body}},

    )
 
