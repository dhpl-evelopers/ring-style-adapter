from fastapi import FastAPI, Request, HTTPException, Body
from fastapi.responses import JSONResponse, PlainTextResponse
import os
import httpx
import dicttoxml
app = FastAPI(title="Ring Style Adapter", version="0.1.2")
BACKEND_POST_URL = os.getenv("BACKEND_POST_URL", "").strip()
@app.post("/ingest")
async def ingest(payload: dict = Body(...)):
   """
   Accept arbitrary JSON, convert to XML, forward to backend.
   """
   # Convert JSON → XML
   xml_bytes = dicttoxml.dicttoxml(payload, custom_root='root', attr_type=False)
   # Call backend
   async with httpx.AsyncClient(timeout=30) as client:
       resp = await client.post(
           BACKEND_POST_URL,
           content=xml_bytes,
           headers={"Content-Type": "application/xml"},
       )
   return {"backend_status": resp.status_code, "body": resp.text}
