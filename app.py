# FastAPI Application for ingesting quiz data and forwarding as XML
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
import logging
import json
import requests
app = FastAPI()
# Configure logging
logging.basicConfig(level=logging.INFO)
# Load mapping configuration
try:
   with open("mapping.config.json", "r") as f:
       MAPPING_CONFIG = json.load(f)
except Exception as e:
   logging.error("Failed to load mapping.config.json: %s", e)
   MAPPING_CONFIG = {}
# Determine answer keys order
ANSWER_KEYS_ORDER = ["purchase_for", "gender", "profession", "occasion", "purpose",
                   "day", "weekend", "work_dress", "social_dress", "line", "painting", "word", "plan"]
# Backend endpoint URL (base)
BACKEND_URL = "http://your-backend-url"  # Example base URL for backend
@app.post("/ingest")
async def ingest(request: Request):
   """
   Accepts JSON payload with user metadata and quiz responses, constructs an XML, and forwards it to backend.
   """
   # Parse request JSON
   try:
       data = await request.json()
   except Exception as e:
       logging.error("Invalid JSON payload: %s", e)
       raise HTTPException(status_code=400, detail="Invalid JSON payload")
   # Use field_map from config to map input fields to output XML tags
   field_map = MAPPING_CONFIG.get("field_map", {})
   defaults = MAPPING_CONFIG.get("defaults", {})
   result_key_candidates = MAPPING_CONFIG.get("result_key_field_candidates", [])
   # Get result_key from input (either 'request_id' or 'result_key')
   result_key_val = None
   for key in result_key_candidates:
       if key in data:
           result_key_val = data[key]
           break
   if result_key_val is None:
       logging.error("No request_id or result_key provided in input")
       raise HTTPException(status_code=400, detail="Missing request identifier (request_id)")
   # Map user metadata fields
   output_fields = {}
   for input_field, output_field in field_map.items():
       if input_field in data:
           output_fields[output_field] = data[input_field]
   # Handle phone_number if present (since field_map might use 'phone')
   if "phone_number" in data:
       output_fields["phone_number"] = data["phone_number"]
   # Set result_key field
   output_fields["result_key"] = result_key_val
   # Fill missing fields with defaults
   for out_field, default_val in defaults.items():
       if out_field not in output_fields:
           output_fields[out_field] = default_val
   # Prepare questions and answers lists
   questions_list = []
   answers_list = []
   if "questions" in data and data.get("questions") is not None:
       # Both questions and answers provided
       questions_list = data.get("questions", [])
       answers_list = data.get("answers", [])
       if not isinstance(answers_list, list):
           logging.error("Answers provided are not in list format")
           raise HTTPException(status_code=400, detail="Answers must be a list")
       if len(questions_list) != len(answers_list):
           logging.error("Questions count (%d) != answers count (%d)", len(questions_list), len(answers_list))
           raise HTTPException(status_code=400, detail="Questions and answers count do not match")
   elif "answers" in data:
       # Only answers provided, derive questions using mapping
       answers_list = data.get("answers", [])
       if not isinstance(answers_list, list):
           logging.error("Answers provided are not in list format")
           raise HTTPException(status_code=400, detail="Answers must be a list")
       if not ANSWER_KEYS_ORDER or "answer_key_to_question" not in MAPPING_CONFIG:
           logging.error("Answer-to-question mapping configuration is missing")
           raise HTTPException(status_code=500, detail="Server configuration error: question mapping missing")
       # Determine question variant (self, others_male, others_female, or others) based on first two answers
       variant_key = "self"
       if len(answers_list) > 0:
           first_ans = str(answers_list[0]).strip().lower()
           if "self" in first_ans:
               variant_key = "self"
           else:
               # Not self, assume others (someone else)
               if len(answers_list) > 1:
                   gender_ans = str(answers_list[1]).strip().lower()
                   if "male" in gender_ans:
                       variant_key = "others_male"
                   elif "female" in gender_ans:
                       variant_key = "others_female"
                   else:
                       variant_key = "others"
               else:
                   variant_key = "others"
       logging.info("Using question variant: %s", variant_key)
       # Build questions_list
       for idx, ans in enumerate(answers_list):
           if idx >= len(ANSWER_KEYS_ORDER):
               logging.error("More answers provided (%d) than expected (%d)", len(answers_list), len(ANSWER_KEYS_ORDER))
               raise HTTPException(status_code=400, detail="Too many answers provided")
           key = ANSWER_KEYS_ORDER[idx]
           if idx < 2:
               # Use general mapping for first two
               question_text = MAPPING_CONFIG["answer_key_to_question"].get(key, "")
           else:
               question_text = MAPPING_CONFIG.get("answer_key_to_question_variants", {}).get(variant_key, {}).get(key, "")
           if question_text == "":
               logging.warning("Question text for key '%s' (variant '%s') not found in mapping", key, variant_key)
           questions_list.append(question_text)
   else:
       logging.error("No answers provided in input")
       raise HTTPException(status_code=400, detail="No answers provided")
   # Pair up questions and answers into a list of dicts
   qa_pairs = []
   for q, a in zip(questions_list, answers_list):
       qa_pairs.append({"question": str(q), "answer": str(a)})
   # Construct XML string
   xml_header = '<?xml version="1.0" encoding="utf-8"?>\n'
   xml_body = "<root>\n"
   xml_body += f"  <full_name>{output_fields.get('full_name', '')}</full_name>\n"
   xml_body += f"  <email>{output_fields.get('email', '')}</email>\n"
   xml_body += f"  <phone_number>{output_fields.get('phone_number', '')}</phone_number>\n"
   # Use birth_date tag (if mapping used birthdate key, prefer that or birth_date)
   birth_val = output_fields.get('birth_date') if 'birth_date' in output_fields else output_fields.get('birthdate', '')
   xml_body += f"  <birth_date>{birth_val}</birth_date>\n"
   xml_body += f"  <result_key>{output_fields.get('result_key', '')}</result_key>\n"
   # Serialize questionAnswers list as JSON string and escape special XML characters
   qa_json = json.dumps(qa_pairs)
   qa_xml_text = qa_json.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
   xml_body += f"  <questionAnswers>{qa_xml_text}</questionAnswers>\n"
   xml_body += "</root>"
   xml_content = xml_header + xml_body
   # Send XML to backend
   try:
       response = requests.post(f"{BACKEND_URL}/createRequest", data=xml_content, headers={"Content-Type": "application/xml"})
   except Exception as e:
       logging.error("Error sending request to backend: %s", e)
       raise HTTPException(status_code=502, detail="Failed to send data to backend")
   if response.status_code >= 400:
       logging.error("Backend responded with status %d: %s", response.status_code, response.text)
       raise HTTPException(status_code=response.status_code, detail="Backend service error")
   logging.info("XML forwarded to backend successfully, status %d", response.status_code)
   return JSONResponse(content={"message": "Request ingested successfully"}, status_code=200)
