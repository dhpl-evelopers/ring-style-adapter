import os
import json
import logging
from typing import Any, Dict, Optional
import httpx
from fastapi import FastAPI, Request, HTTPException, Query
from fastapi.responses import JSONResponse, PlainTextResponse, Response
# ---------------- Logging ----------------
log = logging.getLogger("adapter")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
# ---------------- App ----------------
app = FastAPI(title="Ring Style Adapter (JSON passthrough)", version="1.0.0")
# ---------------- Env ----------------
# Set these in Azure > Your Web App > Settings > Environment variables
BACKEND_POST_URL = os.getenv("BACKEND_POST_URL", "").strip()  # e.g. https://<api>.azurewebsites.net/createRequest
BACKEND_GET_URL  = os.getenv("BACKEND_GET_URL", "").strip()   # e.g. https://<api>.azurewebsites.net/fetchResponse?response_id=
API_KEY_REQUIRED = os.getenv("API_KEY_REQUIRED", "false").lower() == "true"
API_KEY          = os.getenv("API_KEY", "")
# ---------------- Helpers ----------------
def _need_backend_urls():
   if not BACKEND_POST_URL or not BACKEND_GET_URL:
       raise HTTPException(status_code=500, detail="BACKEND_POST_URL / BACKEND_GET_URL are not configured")
def _check_api_key(req: Request):
   if not API_KEY_REQUIRED:
       return
   user_key = req.headers.get("x-api-key")
   if not API_KEY or not user_key or user_key != API_KEY:
       raise HTTPException(status_code=401, detail="unauthorized")
def _build_fetch_url(response_id: str) -> str:
   """
   Build a GET URL for fetchResponse from the env template.
   Supports any of:
     - '.../fetchResponse?response_id='  (we'll append the id)
     - '.../fetchResponse?foo=bar'       (we'll add &response_id=<id>)
     - '.../fetchResponse/{id}'          (use {id} placeholder)
   """
   if "{id}" in BACKEND_GET_URL:
       return BACKEND_GET_URL.replace("{id}", response_id)
   if BACKEND_GET_URL.endswith("="):
       return BACKEND_GET_URL + response_id
   joiner = "&" if ("?" in BACKEND_GET_URL) else "?"
   return f"{BACKEND_GET_URL}{joiner}response_id={response_id}"
async def _call_backend_post(payload: Dict[str, Any]) -> httpx.Response:
   headers = {"Content-Type": "application/json"}
   async with httpx.AsyncClient(timeout=30) as client:
       return await client.post(BACKEND_POST_URL, json=payload, headers=headers)
async def _call_backend_get(url: str) -> httpx.Response:
   async with httpx.AsyncClient(timeout=30) as client:
       return await client.get(url)
# ---------------- Routes ----------------
@app.get("/")
def root():
   return {
       "ok": True,
       "version": "1.0.0",
       "requires_api_key": API_KEY_REQUIRED,
       "has_backend_urls": bool(BACKEND_POST_URL and BACKEND_GET_URL),
       "post_url_configured": bool(BACKEND_POST_URL),
       "get_url_configured": bool(BACKEND_GET_URL),
   }
@app.get("/health")
def health():
   return {"ok": True}
@app.post("/ingest")
async def ingest(
   req: Request,
   preview: bool = Query(False, description="If true, just echo JSON without calling backend"),
   echo: bool = Query(False, description="If true, include the sent JSON and backend status/body")
):
   """
   Accept arbitrary JSON and forward it **as JSON** to your backend /createRequest.
   - preview=true  -> return the JSON you posted (no backend call)
   - echo=true     -> include the JSON + backend status/body in the response (for debugging)
   """
   _need_backend_urls()
   _check_api_key(req)
   # Parse JSON body (never assume schema)
   try:
       payload = await req.json()
       if not isinstance(payload, dict):
           # wrap primitives/arrays so backend always receives an object
           payload = {"data": payload}
   except Exception as e:
       raise HTTPException(status_code=400, detail=f"Invalid JSON body: {e}")
   if preview:
       # Just confirm what we would send
       return JSONResponse({"preview": True, "json": payload})
   # Call backend /createRequest (JSON -> JSON)
   try:
       resp = await _call_backend_post(payload)
   except httpx.RequestError as e:
       # Network/DNS/timeout/etc between adapter and backend
       return JSONResponse(
           status_code=502,
           content={"detail": {"adapter_error": "cannot_reach_backend", "msg": str(e)}},
       )
   # Pass through backend success
   if 200 <= resp.status_code < 300:
       ctype = resp.headers.get("content-type", "")
       if echo:
           return JSONResponse(
               {
                   "json": payload,
                   "backend_status": resp.status_code,
                   "backend_body": resp.json() if "json" in ctype else resp.text,
               }
           )
       # Stream JSON or plain text back transparently
       if "json" in ctype:
           return JSONResponse(resp.json())
       return Response(content=resp.text, media_type=ctype or "text/plain")
   # Bubble backend failures as 502 with backend status+body (so you can see it's the backend)
   return JSONResponse(
       status_code=502,
       content={"detail": {"backend_status": resp.status_code, "body": resp.text}},
   )
@app.get("/result")
async def result(response_id: str = Query(..., description="Response / result key to fetch")):
   """
   Fetch a result from backend /fetchResponse using ?response_id=<id>
   or a {id} placeholder provided via BACKEND_GET_URL.
   """
   _need_backend_urls()
   url = _build_fetch_url(response_id)
   try:
       resp = await _call_backend_get(url)
   except httpx.RequestError as e:
       return JSONResponse(
           status_code=502,
           content={"detail": {"adapter_error": "cannot_reach_backend", "msg": str(e)}},
       )
   if 200 <= resp.status_code < 300:
       ctype = resp.headers.get("content-type", "")
       if "json" in ctype:
           return JSONResponse(resp.json())
       return Response(content=resp.text, media_type=ctype or "text/plain")
   return JSONResponse(
       status_code=502,
       content={"detail": {"backend_status": resp.status_code, "body": resp.text}},
   )
