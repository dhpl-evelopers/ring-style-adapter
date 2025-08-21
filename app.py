import json
from uuid import uuid4
import xml.etree.ElementTree as ET
import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import Response
# Load mapping configuration from JSON file
with open("mapping.config.json", "r") as f:
   config = json.load(f)
field_map = config["field_map"]
id_candidates = config.get("result_key_field_candidates", [])
defaults = config.get("defaults", {})
answer_map = config.get("answer_key_to_question", {})
variant_map_all = config.get("answer_key_to_question_variants", {})
# Helper to determine variant key based on 'purchase_for' and 'gender' answers
def get_variant_key(purchase_for_value, gender_value):
   if not purchase_for_value:
       return None
   pf_val = str(purchase_for_value).strip().lower()
   if pf_val in ["self", "myself"]:
       return "self"
   if pf_val in ["others", "other", "someone else"]:
       # If purchasing for someone else, choose variant by gender
       if gender_value:
           g_val = str(gender_value).strip().lower()
           if g_val == "male":
               return "others_male"
           elif g_val == "female":
               return "others_female"
       return "others"
   # If purchase_for value directly matches a variant key (e.g., "others_male"), use it
   if pf_val in variant_map_all:
       return pf_val
   return None
app = FastAPI()
@app.post("/createRequest")
async def create_request(input_data: dict):
   # Map metadata fields from input to output (using field_map)
   metadata = {}
   for in_field, out_field in field_map.items():
       if in_field in input_data:
           metadata[out_field] = input_data[in_field]
   # Apply default values for missing metadata fields
   for field, default_val in defaults.items():
       metadata.setdefault(field, default_val)
   # Determine or generate request ID
   request_id_val = None
   for cand in id_candidates:
       if cand in input_data and str(input_data[cand]).strip():
           request_id_val = input_data[cand]
           break
   if not request_id_val:
       request_id_val = str(uuid4())
   metadata["request_id"] = str(request_id_val)
   # Build list of question-answer pairs
   question_answer_list = []
   if "questionAnswers" in input_data:
       # Input is an array of question-answer objects
       qa_list = input_data["questionAnswers"]
       if not isinstance(qa_list, list):
           raise HTTPException(status_code=400, detail="'questionAnswers' must be a list")
       # Determine context (purchase_for and gender) if present
       pf_val = None
       gender_val = None
       for qa in qa_list:
           q_field = qa.get("question")
           if q_field == answer_map.get("purchase_for") or q_field == "purchase_for":
               pf_val = qa.get("answer")
           if q_field == answer_map.get("gender") or q_field == "gender":
               gender_val = qa.get("answer")
       variant_key = get_variant_key(pf_val, gender_val)
       # Map each question field to text if needed
       for qa in qa_list:
           q_field = qa.get("question")
           ans_text = qa.get("answer", "")
           if q_field is None:
               continue
           # Determine the question text based on mapping
           q_text = None
           if variant_key:
               if isinstance(q_field, str) and q_field.lower() in variant_map_all.get(variant_key, {}):
                   q_text = variant_map_all[variant_key][q_field.lower()]
               elif isinstance(q_field, str) and q_field in answer_map:
                   q_text = answer_map[q_field]
           else:
               if isinstance(q_field, str) and q_field in answer_map:
                   q_text = answer_map[q_field]
           if q_text is None:
               q_text = str(q_field)
           question_answer_list.append((q_text, ans_text))
   elif "questions" in input_data and "answers" in input_data:
       # Input provides separate lists for questions and answers
       questions = input_data["questions"]
       answers = input_data["answers"]
       if not isinstance(questions, list) or not isinstance(answers, list):
           raise HTTPException(status_code=400, detail="'questions' and 'answers' must be lists of equal length")
       if len(questions) != len(answers):
           raise HTTPException(status_code=400, detail="Mismatch between number of questions and answers")
       # Determine context for variant
       pf_val = None
       gender_val = None
       if "purchase_for" in questions:
           idx = questions.index("purchase_for")
           pf_val = answers[idx] if idx < len(answers) else None
       elif answer_map.get("purchase_for") in questions:
           idx = questions.index(answer_map["purchase_for"])
           pf_val = answers[idx] if idx < len(answers) else None
       if "gender" in questions:
           idx = questions.index("gender")
           gender_val = answers[idx] if idx < len(answers) else None
       elif answer_map.get("gender") in questions:
           idx = questions.index(answer_map["gender"])
           gender_val = answers[idx] if idx < len(answers) else None
       variant_key = get_variant_key(pf_val, gender_val)
       # Pair each question with its answer, mapping question text if necessary
       for q_field, ans_text in zip(questions, answers):
           q_text = None
           if variant_key:
               if isinstance(q_field, str) and q_field.lower() in variant_map_all.get(variant_key, {}):
                   q_text = variant_map_all[variant_key][q_field.lower()]
               elif isinstance(q_field, str) and q_field in answer_map:
                   q_text = answer_map[q_field]
           else:
               if isinstance(q_field, str) and q_field in answer_map:
                   q_text = answer_map[q_field]
           if q_text is None:
               q_text = str(q_field)
           question_answer_list.append((q_text, ans_text))
   elif "answers" in input_data:
       # Input is a list of answers only (in order)
       answers = input_data["answers"]
       if not isinstance(answers, list):
           raise HTTPException(status_code=400, detail="'answers' must be a list")
       if len(answers) < 1:
           raise HTTPException(status_code=400, detail="No answers provided")
       pf_val = answers[0] if len(answers) > 0 else None
       gender_val = answers[1] if len(answers) > 1 else None
       variant_key = get_variant_key(pf_val, gender_val)
       # Base questions: purchase_for and gender
       if pf_val is not None:
           q_text = answer_map.get("purchase_for", "Who are you purchasing for?")
           question_answer_list.append((q_text, pf_val))
       if gender_val is not None:
           q_text = answer_map.get("gender", "Gender")
           question_answer_list.append((q_text, gender_val))
       # Variant-specific questions
       if variant_key and variant_key in variant_map_all:
           variant_questions = variant_map_all[variant_key]
           # Assign remaining answers in order to each variant question
           for i, (q_key, q_text) in enumerate(variant_questions.items(), start=2):
               ans_text = answers[i] if i < len(answers) else ""
               question_answer_list.append((q_text, ans_text))
       else:
           # If variant not determined, append remaining answers with generic labels
           for i, ans_text in enumerate(answers[2:], start=3):
               question_answer_list.append((f"Question{i}", ans_text))
   else:
       # Input fields directly contain question keys and answers
       pf_val = input_data.get("purchase_for")
       gender_val = input_data.get("gender")
       variant_key = get_variant_key(pf_val, gender_val)
       if pf_val is not None:
           q_text = answer_map.get("purchase_for", "Who are you purchasing for?")
           question_answer_list.append((q_text, pf_val))
       if gender_val is not None:
           q_text = answer_map.get("gender", "Gender")
           question_answer_list.append((q_text, gender_val))
       if variant_key and variant_key in variant_map_all:
           for q_key, q_text in variant_map_all[variant_key].items():
               ans_val = input_data.get(q_key, "")
               question_answer_list.append((q_text, ans_val))
       else:
           # Include any other known question fields present in input
           for key, ans_val in input_data.items():
               if key in ["purchase_for", "gender"]:
                   continue
               if key in answer_map:
                   question_answer_list.append((answer_map[key], ans_val))
               else:
                   for v_map in variant_map_all.values():
                       if key in v_map:
                           question_answer_list.append((v_map[key], ans_val))
                           break
   # Construct XML structure
   root = ET.Element("Request")
   ET.SubElement(root, "request_id").text = metadata.get("request_id", "")
   ET.SubElement(root, "full_name").text = metadata.get("full_name", "")
   ET.SubElement(root, "email").text = metadata.get("email", "")
   ET.SubElement(root, "phone_number").text = metadata.get("phone_number", "")
   ET.SubElement(root, "birth_date").text = metadata.get("birth_date", "")
   for q_text, ans_text in question_answer_list:
       qa_elem = ET.SubElement(root, "questionAnswers")
       ET.SubElement(qa_elem, "question").text = str(q_text) if q_text is not None else ""
       ET.SubElement(qa_elem, "answer").text = str(ans_text) if ans_text is not None else ""
   # Convert to string with XML declaration
   xml_bytes = ET.tostring(root, encoding="utf-8", xml_declaration=True)
   xml_str = xml_bytes.decode("utf-8").strip()
   if not xml_str.startswith("<?xml"):
       xml_str = '<?xml version="1.0" encoding="utf-8"?>' + xml_str
   # Post XML to backend /createRequest endpoint
   backend_url = "https://backend.example.com/createRequest"  # Replace with actual backend URL
   try:
       async with httpx.AsyncClient() as client:
           resp = await client.post(backend_url, content=xml_str, headers={"Content-Type": "application/xml"})
   except Exception as e:
       raise HTTPException(status_code=502, detail=f"Backend request failed: {e}")
   # Return backend status and response body
   return Response(content=resp.content, status_code=resp.status_code, media_type=resp.headers.get("Content-Type") or "text/plain")
