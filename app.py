from fastapi import FastAPI, Request, HTTPException, Body
from fastapi.responses import JSONResponse, PlainTextResponse
import os, httpx, dicttoxml, uuid
app = FastAPI(title="Ring Style Adapter", version="0.1.1")
BACKEND_POST_URL = os.getenv("BACKEND_POST_URL", "").strip()
BACKEND_GET_URL  = os.getenv("BACKEND_GET_URL", "").strip()
API_KEY_REQUIRED = os.getenv("API_KEY_REQUIRED", "false").lower() == "true"
API_KEY          = os.getenv("API_KEY", "")
def _check_api_key(req: Request):
   if not API_KEY_REQUIRED:
       return
   if req.headers.get("x-api-key", "") != API_KEY:
       raise HTTPException(status_code=401, detail="unauthorized")
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
@app.post("/ingest")
async def ingest(
   payload: dict = Body(
       ...,
       examples={
           "minimal": {
               "summary": "Any JSON you like",
               "value": {"name": "Sakshi", "occasion": "Anniversary"}
           }
       },
   ),
   request: Request = None,
):
   _check_api_key(request)
   if not BACKEND_POST_URL:
       raise HTTPException(status_code=500, detail="BACKEND_POST_URL is not configured")
   # add a result_key we can echo back
   result_key = str(uuid.uuid4())
   # JSON -> XML
   xml_bytes = dicttoxml.dicttoxml(payload, custom_root="root", attr_type=False)
   # forward to backend
   try:
       async with httpx.AsyncClient(timeout=30) as client:
           resp = await client.post(
               BACKEND_POST_URL,
               content=xml_bytes,
               headers={"Content-Type": "application/xml"},
           )
   except httpx.RequestError as e:
       return JSONResponse(
           status_code=502,
           content={
               "detail": {"adapter_error": "cannot_reach_backend", "msg": str(e)},
               "result_key": result_key,
           },
       )
   ct = resp.headers.get("content-type", "")
   body = resp.text
   if 200 <= resp.status_code < 300:
       if "xml" in ct:
           # include result_key in header so you still see it
           return PlainTextResponse(body, media_type="application/xml", headers={"x-result-key": result_key})
       return JSONResponse({"backend_status": resp.status_code, "body": body, "result_key": result_key})
   return JSONResponse(
       status_code=502,
       content={"detail": {"backend_status": resp.status_code, "body": body}, "result_key": result_key},
   )
