from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse
import xml.etree.ElementTree as ET
import httpx
import json
import logging
app = FastAPI()
logger = logging.getLogger("adapter")
logger.setLevel(logging.INFO)
# Load mapping configuration
with open("mapping.config.json") as f:
   MAPPING = json.load(f)
# Endpoint to ingest flexible JSON and forward as XML
@app.post("/ingest")
async def ingest(request: Request):
   try:
       data = await request.json()
   except Exception as e:
       logger.error(f"Invalid JSON input: {e}")
       raise HTTPException(status_code=400, detail="Invalid JSON input")
   # Validate and map mandatory fields
   field_map = MAPPING.get("field_map", {})
   # Determine result_key (request_id) field
   result_candidates = MAPPING.get("result_key_field_candidates", [])
   result_key = None
   for cand in result_candidates:
       if cand in data:
           result_key = data.get(cand)
           break
   if not result_key:
       raise HTTPException(status_code=400, detail="Missing mandatory field: result_key/request_id")
   # Map main fields to backend field names
   backend_fields = {}
   for ui_key, backend_key in field_map.items():
       if ui_key in data:
           backend_fields[backend_key] = data[ui_key]
   # Check mandatory backend fields
   mandatory = ["full_name", "email", "phone_number", "birth_date", "result_key"]
   for field in mandatory:
       if field == "result_key":
           # already captured above
           if not result_key:
               raise HTTPException(status_code=400, detail=f"Missing mandatory field: {field}")
           backend_fields["result_key"] = result_key
       else:
           if field not in backend_fields or not backend_fields.get(field):
               raise HTTPException(status_code=400, detail=f"Missing mandatory field: {field}")
   # Build the XML payload
   root = ET.Element("inputs")
   # Add basic fields
   ET.SubElement(root, "full_name").text = str(backend_fields["full_name"])
   ET.SubElement(root, "email").text = str(backend_fields["email"])
   ET.SubElement(root, "phone_number").text = str(backend_fields["phone_number"])
   ET.SubElement(root, "birth_date").text = str(backend_fields["birth_date"])
   ET.SubElement(root, "result_key").text = str(backend_fields["result_key"])
   # Handle questions/answers
   qas = []
   # If UI provided combined "questionAnswers" list
   if "questionAnswers" in data:
       qas = data["questionAnswers"]
   else:
       # If UI provided separate "questions" and "answers" lists
       questions = data.get("questions", [])
       answers = data.get("answers", [])
       # Pair them by index if both lists present
       for idx in range(min(len(questions), len(answers))):
           q_text = questions[idx].get("question") or questions[idx]
           a_text = answers[idx].get("answer") or answers[idx]
           qa_obj = {"question": q_text, "answer": a_text}
           qas.append(qa_obj)
   # Add questionAnswer elements
   qa_parent = ET.SubElement(root, "questionAnswers")
   for qa in qas:
       qa_elem = ET.SubElement(qa_parent, "questionAnswer")
       # If mapping for question text to ID exists, use it
       q_text = qa.get("question") or ""
       mapping_q = MAPPING.get("question_map", {}).get(q_text, {})
       question_id = mapping_q.get("questionId", "")
       # Use provided ID if any, else empty
       ET.SubElement(qa_elem, "questionId").text = question_id
       ET.SubElement(qa_elem, "question").text = q_text
       ET.SubElement(qa_elem, "answer").text = str(qa.get("answer", ""))
       # Optionally include score or options if needed (not shown here)
   # Convert XML tree to string
   try:
       xml_bytes = ET.tostring(root, encoding="utf-8", method="xml")
   except Exception as e:
       logger.error(f"XML generation error: {e}")
       raise HTTPException(status_code=500, detail="Failed to generate XML payload")
   # POST XML to backend
   backend_url = "https://projectaiapi.azurewebsites.net/createRequest"
   headers = {"Content-Type": "application/xml"}
   try:
       async with httpx.AsyncClient() as client:
           resp = await client.post(backend_url, content=xml_bytes, headers=headers, timeout=30.0)
           resp.raise_for_status()
   except httpx.HTTPError as e:
       logger.error(f"Backend request failed: {e}")
       raise HTTPException(status_code=502, detail="Backend request failed")
   # Return backend's response (assuming XML -> JSON or status)
   # Here we assume backend returns JSON or empty 200; adjust as needed
   try:
       resp_json = resp.json()
       return JSONResponse(content=resp_json, status_code=resp.status_code)
   except ValueError:
       # If response is XML or not JSON
       return Response(content=resp.text, media_type=resp.headers.get("Content-Type", "text/plain"),
                       status_code=resp.status_code)
