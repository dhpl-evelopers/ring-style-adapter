# app.py
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
import xml.etree.ElementTree as ET
import requests
import json
import logging
app = FastAPI()
logging.basicConfig(level=logging.INFO)
# Load mapping config if needed
try:
   with open("mapping.config.json") as f:
       config = json.load(f)
except FileNotFoundError:
   config = {}
question_key_map = config.get("answer_key_to_question", {})
xml_root_tag = config.get("xml_root", "request")
# Backend endpoint URL (update with actual backend address)
BACKEND_URL = "http://localhost:8001/createRequest"
@app.post("/ingest")
async def ingest(request: Request):
   data = await request.json()
   # Required fields
   required = ["full_name", "email", "phone_number", "birth_date", "request_id"]
   missing = [f for f in required if f not in data or data.get(f) is None]
   if missing:
       raise HTTPException(status_code=400, detail=f"Missing fields: {', '.join(missing)}")
   # Create XML root
   root = ET.Element(xml_root_tag)
   # Add mandatory fields as subelements
   for field in required:
       elem = ET.SubElement(root, field)
       elem.text = str(data[field])
   # Collect question-answer pairs
   qa_list = []
   # Case 1: separate 'questions' and 'answers' lists
   if "questions" in data and "answers" in data:
       questions = data["questions"]
       answers = data["answers"]
       # Zip together; use minimum length
       for q_item, a_item in zip(questions, answers):
           # Extract question text
           if isinstance(q_item, dict):
               q_text = q_item.get("question") or ""
           else:
               q_text = str(q_item)
           # Extract answer text
           if isinstance(a_item, dict):
               a_text = a_item.get("answer") or ""
           else:
               a_text = str(a_item)
           qa_list.append((q_text, a_text))
   # Case 2: 'questions' list of objects with 'question' and 'answer'
   elif "questions" in data:
       for item in data["questions"]:
           if isinstance(item, dict):
               q_text = item.get("question", "")
               a_text = item.get("answer", "")
               qa_list.append((q_text, a_text))
   # Case 3: only 'answers' list present
   elif "answers" in data:
       for a_item in data["answers"]:
           if isinstance(a_item, dict):
               a_text = a_item.get("answer", "")
           else:
               a_text = str(a_item)
           qa_list.append(("", a_text))
   # Case 4: use mapping keys (e.g. "who": "Alice")
   else:
       for key, mapped_q in question_key_map.items():
           if key in data:
               qa_list.append((mapped_q, str(data[key])))
   # Add questionAnswers elements to XML
   for q_text, a_text in qa_list:
       qa_elem = ET.SubElement(root, "questionAnswers")
       q_elem = ET.SubElement(qa_elem, "question")
       q_elem.text = q_text
       a_elem = ET.SubElement(qa_elem, "answer")
       a_elem.text = a_text
   # Generate XML string with declaration
   tree = ET.ElementTree(root)
   xml_bytes = ET.tostring(root, encoding='utf-8', xml_declaration=True)
   xml_str = xml_bytes.decode('utf-8')
   # Send XML to backend
   headers = {"Content-Type": "application/xml"}
   try:
       response = requests.post(BACKEND_URL, data=xml_str.encode('utf-8'), headers=headers)
       backend_status = response.status_code
       backend_text = response.text
       if backend_status != 200:
           logging.error(f"Backend returned status {backend_status}: {backend_text}")
           return JSONResponse(
               status_code=502,
               content={
                   "status": "error",
                   "message": f"Backend error {backend_status}",
                   "xml": xml_str,
                   "backend_response": backend_text
               }
           )
   except Exception as e:
       logging.error(f"Error sending to backend: {e}")
       raise HTTPException(status_code=502, detail=str(e))
   # On success, return XML and backend response
   return {"status": "success", "xml": xml_str, "backend_response": backend_text}
