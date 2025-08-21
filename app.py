from fastapi import FastAPI, HTTPException, Request

from fastapi.responses import Response

import httpx

import json

from xml.etree.ElementTree import Element, SubElement, tostring

app = FastAPI()

# Load mapping configuration (maps UI keys to output fields and question text)

with open('mapping.config.json') as f:

    config = json.load(f)

@app.post("/adapter")

async def adapter(request: Request):

    # Parse the JSON body from the UI

    try:

        data = await request.json()

    except Exception as e:

        raise HTTPException(status_code=400, detail=f"Invalid JSON: {e}")

    # Extract and map basic fields

    full_name = None

    email = None

    phone_number = None

    birth_date = None

    request_id = None

    # Use field_map from config to map UI fields to our variables

    field_map = config.get("field_map", {})

    for out_field, ui_field in field_map.items():

        if ui_field in data:

            val = data.get(ui_field)

            if out_field in ["full_name", "name"] and full_name is None:

                full_name = val

            elif out_field == "email":

                email = val

            elif out_field in ["phone", "phone_number"]:

                phone_number = val

            elif out_field == "birth_date":

                birth_date = val

    # Fallbacks if not set by field_map

    if full_name is None:

        if 'full_name' in data:

            full_name = data['full_name']

        elif 'name' in data:

            full_name = data['name']

    if email is None and 'email' in data:

        email = data['email']

    if phone_number is None:

        for key in ['phone_number', 'phone', 'phoneNumber']:

            if key in data:

                phone_number = data[key]

                break

    if birth_date is None:

        for key in ['birth_date', 'birthDate', 'date']:

            if key in data:

                birth_date = data[key]

                break

    # Map UI result key to request_id using configured candidates

    for key in config.get("result_key_field_candidates", []):

        if key in data:

            request_id = data[key]

            break

    # Check for missing required fields

    missing = [fld for fld in ["full_name","email","phone_number","birth_date","request_id"] if eval(fld) is None]

    if missing:

        raise HTTPException(status_code=400,

            detail=f"Missing required field(s): {', '.join(missing)}")

    # Prepare questions/answers lists

    questions_out = []

    answers_out = []

    # Case 1: UI provides a list under 'answers'

    if 'answers' in data and isinstance(data['answers'], list):

        lst = data['answers']

        if lst and isinstance(lst[0], dict):

            # It's an array of {question, answer} objects

            for qa in lst:

                q_key = qa.get("question") or qa.get("question_key") or qa.get("questionId")

                ans = qa.get("answer")

                if q_key is None or ans is None:

                    raise HTTPException(status_code=400,

                                        detail="Each question-answer object must have 'question' and 'answer'.")

                # Map question key to text if available

                questions_out.append(config.get("answer_key_to_question", {}).get(q_key, q_key))

                answers_out.append(str(ans))

        else:

            # It's an array of answer strings only

            answers_out = [str(x) for x in lst]

    # Case 2: UI provides a list under 'questions' (array of objects)

    elif 'questions' in data and isinstance(data['questions'], list):

        for qa in data['questions']:

            if not isinstance(qa, dict):

                continue

            q_key = qa.get("question") or qa.get("question_key") or qa.get("questionId")

            ans = qa.get("answer")

            if q_key is None or ans is None:

                raise HTTPException(status_code=400,

                                    detail="Each question-answer object must have 'question' and 'answer'.")

            questions_out.append(config.get("answer_key_to_question", {}).get(q_key, q_key))

            answers_out.append(str(ans))

    else:

        # Neither format found

        raise HTTPException(status_code=400,

                            detail="Request must include 'answers' (list of strings) or 'questions' (list of objects).")

    # If only answers were provided (no question texts), generate questions from config

    if answers_out and not questions_out:

        q_map = config.get("answer_key_to_question", {})

        if not q_map:

            raise HTTPException(status_code=400, detail="No question mapping configuration available.")

        keys = list(q_map.keys())

        for i, ans in enumerate(answers_out):

            if i < len(keys):

                questions_out.append(q_map[keys[i]])

            else:

                questions_out.append("")

    # Join questions and answers into semicolon-separated strings

    questions_str = "; ".join(questions_out)

    answers_str = "; ".join(answers_out)

    # Build XML payload

    root_name = config.get("xml_root", "request")

    root = Element(root_name)

    # Create child tags for each required field

    for tag, text in {

            "full_name": full_name,

            "email": email,

            "phone_number": phone_number,

            "birth_date": birth_date,

            "request_id": request_id,

            "questions": questions_str,

            "answers": answers_str

        }.items():

        child = SubElement(root, tag)

        child.text = text or ""

    # Convert XML tree to bytes

    xml_bytes = tostring(root, encoding='utf-8', xml_declaration=True)

    # POST to backend (Content-Type: application/xml)

    backend_url = "http://backend-service/createRequest"

    headers = {"Content-Type": "application/xml"}

    try:

        async with httpx.AsyncClient() as client:

            resp = await client.post(backend_url, content=xml_bytes, headers=headers)

    except Exception as e:

        raise HTTPException(status_code=500, detail=f"Backend request failed: {e}")

    # Return backend response directly to UI (status code and content-type preserved)

    return Response(content=resp.content, status_code=resp.status_code, media_type=resp.headers.get("Content-Type", "text/plain"))
 
