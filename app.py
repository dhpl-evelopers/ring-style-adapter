import os
import json
import httpx
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from xml.etree.ElementTree import Element, SubElement, tostring
app = FastAPI()
# Load mapping config on startup
with open("mapping.config.json") as f:
   mapping = json.load(f)
# Backend endpoint (could be set via environment variable)
BACKEND_URL = os.getenv("BACKEND_URL", "http://backend-service") + "/createRequest"
@app.post("/ingest")
async def ingest(request: Request, preview: bool = False, echo: bool = False):
   try:
       payload = await request.json()
   except Exception as e:
       raise HTTPException(status_code=400, detail=f"Invalid JSON: {e}")
   # Extract and validate the result key (maps to <result_key> tag)
   result_key = None
   for candidate in mapping.get("result_key_field_candidates", []):
       if candidate in payload:
           result_key = payload[candidate]
           break
   if not result_key:
       raise HTTPException(status_code=400, detail="Missing result_key field")
   # Normalize and map input fields to output tag names
   normalized = {}
   for in_key, out_key in mapping["field_map"].items():
       if in_key in payload:
           normalized[out_key] = payload[in_key]
   # Check mandatory normalized fields
   for field in ["full_name", "email", "phone_number", "date"]:
       if field not in normalized or not normalized[field]:
           raise HTTPException(status_code=400, detail=f"Missing mandatory field: {field}")
   # Build XML structure
   root = Element(mapping.get("xml_root", "request"))
   # Mandatory fields as XML elements
   SubElement(root, "result_key").text = str(result_key)
   SubElement(root, "full_name").text = str(normalized["full_name"])
   SubElement(root, "email").text = str(normalized["email"])
   SubElement(root, "phone_number").text = str(normalized["phone_number"])
   SubElement(root, "date").text = str(normalized["date"])
   # Prepare question-answer list
   qa_list = []
   # Case 1: Separate 'questions' and 'answers' lists
   if isinstance(payload.get("questions"), list) and isinstance(payload.get("answers"), list):
       questions = payload["questions"]
       answers = payload["answers"]
       if len(questions) != len(answers):
           raise HTTPException(status_code=400, detail="Questions and answers list lengths do not match")
       for q, a in zip(questions, answers):
           qa_list.append({"question": q, "selectedOption": {"value": a}})
   # Case 2: Single list of Q/A objects in payload['questionAnswers'] or ['answers']
   elif isinstance(payload.get("questionAnswers"), list):
       qa_raw = payload["questionAnswers"]
       for item in qa_raw:
           if "question" not in item:
               continue
           q = item["question"]
           if "selectedOption" in item and "value" in item["selectedOption"]:
               ans = item["selectedOption"]["value"]
           elif "answer" in item:
               ans = item["answer"]
           else:
               continue
           qa_list.append({"question": q, "selectedOption": {"value": ans}})
   elif isinstance(payload.get("answers"), list) and payload.get("answers") and isinstance(payload["answers"][0], dict):
       # If payload['answers'] is a list of objects
       for item in payload["answers"]:
           if "question" not in item:
               continue
           q = item["question"]
           if "selectedOption" in item and "value" in item["selectedOption"]:
               ans = item["selectedOption"]["value"]
           elif "answer" in item:
               ans = item["answer"]
           else:
               continue
           qa_list.append({"question": q, "selectedOption": {"value": ans}})
   else:
       # Case 3: Flat answer fields (keys that map to known questions)
       for key, question_text in mapping.get("answer_key_to_question", {}).items():
           if key in payload:
               qa_list.append({"question": question_text, "selectedOption": {"value": payload[key]}})
   # Insert questionAnswers JSON as a single XML element
   qa_json = json.dumps(qa_list)
   SubElement(root, "questionAnswers").text = qa_json
   # Convert XML tree to string
   xml_bytes = tostring(root, encoding="utf-8", method="xml")
   xml_str = xml_bytes.decode("utf-8")
   # If preview mode, do not call backend, just return XML
   if preview:
       result = {"xml": xml_str}
       return JSONResponse(content=result, status_code=200)
   # Send XML to backend
   try:
       headers = {"Content-Type": "application/xml"}
       resp = httpx.post(BACKEND_URL, content=xml_str.encode("utf-8"), headers=headers)
   except Exception as e:
       raise HTTPException(status_code=502, detail=f"Backend request failed: {e}")
   # If echo is requested, include details in response
   if echo:
       return {
           "sent_xml": xml_str,
           "backend_status": resp.status_code,
           "backend_headers": dict(resp.headers),
           "backend_body": resp.text
       }
   # Otherwise just return status or empty success
   if resp.status_code != 200:
       raise HTTPException(status_code=resp.status_code, detail=f"Backend error: {resp.text}")
   return JSONResponse(content={"status": "success"}, status_code=200)
