import os, json, uuid

from fastapi import FastAPI, Request, HTTPException, Query

from fastapi.responses import JSONResponse, Response

app = FastAPI()

@app.post("/ingest")

async def ingest(request: Request, preview: bool = Query(False), echo: bool = Query(False)):

    # 1. Load mapping configuration dynamically from file

    config_path = os.path.join(os.path.dirname(__file__) if "__file__" in globals() else ".", "mapping.config.json")

    try:

        with open(config_path, "r") as cfg:

            config = json.load(cfg)

        print(f"Loaded mapping config from {config_path}")

    except Exception as e:

        # Configuration file load failure

        print(f"Error loading config: {e}")

        raise HTTPException(status_code=500, detail=f"Configuration load error: {e}")

    # 2. Normalize UI-facing keys to backend-expected field names

    # Update field_map for any keys not covered (e.g., CamelCase to snake_case)

    field_map = config.get("field_map", {})

    field_map["phone"] = "phone_number"        # e.g. "phone" -> "phone_number"

    field_map["phone_number"] = "phone_number" # e.g. "phone_number" -> "phone_number"

    field_map["phoneNumber"] = "phone_number"  # e.g. "phoneNumber" -> "phone_number"

    field_map["birthDate"] = "birth_date"      # e.g. "birthDate" -> "birth_date"

    config["field_map"] = field_map

    # Adjust default values keys if needed (match normalized field names)

    defaults = config.get("defaults", {})

    if "phoneNumber" in defaults:

        defaults["phone_number"] = defaults.get("phoneNumber", "")

        defaults.pop("phoneNumber", None)

    if "birthDate" in defaults:

        defaults["birth_date"] = defaults.get("birthDate", "")

        defaults.pop("birthDate", None)

    config["defaults"] = defaults

    # Parse JSON body from the incoming request

    try:

        body = await request.json()

    except Exception:

        raise HTTPException(status_code=400, detail="Invalid JSON body")

    print("Received request body:", body)

    normalized_data = {}

    processed_keys = set()

    # Map each UI key to the backend key using field_map

    for alias, canonical in config.get("field_map", {}).items():

        if alias in body:

            normalized_data[canonical] = body[alias]

            processed_keys.add(alias)

            processed_keys.add(canonical)

            if alias != canonical:

                print(f"Normalized field '{alias}' to '{canonical}': {body[alias]}")

            else:

                print(f"Kept field '{alias}' as '{canonical}': {body[alias]}")

    # Copy over any remaining expected fields (already in correct form) that weren't processed

    for field in config.get("defaults", {}):

        if field in body and field not in processed_keys:

            normalized_data[field] = body[field]

            processed_keys.add(field)

            print(f"Preserved field '{field}': {body[field]}")

    # 3. Determine request_id (use provided one or generate a new UUID)

    request_id = None

    # Check known candidate fields for an existing request identifier

    for key in config.get("result_key_field_candidates", []):

        if key in body:

            request_id = body[key]

            print(f"Using request id from '{key}': {request_id}")

            break

    # Also check common variations (camelCase or snake_case)

    if request_id is None:

        if "requestId" in body:

            request_id = body["requestId"]

            print(f"Using request id from 'requestId': {request_id}")

        elif "request_id" in body:

            request_id = body["request_id"]

            print(f"Using request id from 'request_id': {request_id}")

    # Generate a new request_id if none was provided

    if request_id is None:

        request_id = str(uuid.uuid4())

        print(f"No request id provided; generated new UUID: {request_id}")

    # 4. Map question text to each answer using mapping.config and apply logic for mapping sheets

    if "answers" not in body or not isinstance(body["answers"], list):

        raise HTTPException(status_code=400, detail="Request must include an 'answers' list")

    answers_input = body["answers"]

    questions_list = []

    answers_list = []

    answer_map = {}  # to store answers by their key (if keys provided)

    # Retrieve the mapping of answer keys to question text from config

    key_to_question = config.get("answer_key_to_question", {})

    # Iterate through answers list and build questions_list and answers_list

    if answers_input and isinstance(answers_input[0], dict):

        # Answers are provided as a list of objects (with question identifiers)

        for ans_obj in answers_input:

            q_key = None

            ans_val = None

            # Identify the question key

            if "questionId" in ans_obj:

                q_key = ans_obj["questionId"]

            elif "key" in ans_obj:

                q_key = ans_obj["key"]

            # Identify the answer value

            if "answer" in ans_obj:

                ans_val = ans_obj["answer"]

            elif "value" in ans_obj:

                ans_val = ans_obj["value"]

            # Handle format like {"Some Question?": "Answer"} if no explicit key field

            if ans_val is None and q_key is None and len(ans_obj) == 1:

                q_key, ans_val = next(iter(ans_obj.items()))

            # Determine the question text

            if q_key:

                answer_map[q_key] = ans_val

                question_text = key_to_question.get(q_key)

                if not question_text:

                    # If no mapping found for this key, use the key itself as fallback

                    question_text = q_key

                    print(f"Warning: No question text mapping for key '{q_key}', using key as text.")

            else:

                # If no key but question text is provided directly in object

                question_text = ans_obj.get("question")

            if question_text is None:

                # Unrecognized answer format, skip this entry

                print(f"Warning: Unrecognized answer format: {ans_obj}")

                continue

            questions_list.append(question_text)

            answers_list.append(ans_val)

            print(f"Mapped '{question_text}' -> '{ans_val}'")

    else:

        # Answers are provided as a simple list of values (assumed in fixed order)

        for idx, ans_val in enumerate(answers_input):

            # Get the corresponding question key by index

            q_key = list(key_to_question.keys())[idx] if idx < len(key_to_question) else None

            question_text = key_to_question.get(q_key) if q_key else None

            if q_key:

                answer_map[q_key] = ans_val

            if question_text is None:

                # If we don't have a mapping (index out of range or missing key), use generic placeholder

                question_text = f"Q{idx+1}"

                print(f"Warning: No mapping for answer at position {idx}, using '{question_text}' as question text")

            else:

                print(f"Matched answer {idx}: '{question_text}' -> '{ans_val}'")

            questions_list.append(question_text)

            answers_list.append(ans_val)

    # Determine which mapping sheet to use (for internal logic/debugging)

    mapping_sheet = None

    who_value = answer_map.get("who") or answer_map.get("purchase")

    if who_value is None and answers_list:

        # If answers provided without keys, use first answer as "who" (purchase-for)

        who_value = answers_list[0]

    if isinstance(who_value, str):

        who_lower = who_value.strip().lower()

        if who_lower == "self":

            mapping_sheet = "Self"

        elif who_lower in ["others", "other"]:

            # For "others", determine gender to pick male/female mapping

            gender_val = answer_map.get("gender")

            if gender_val is None and len(answers_list) > 1:

                gender_val = answers_list[1]

            if gender_val:

                gender_lower = str(gender_val).strip().lower()

                if gender_lower in ["female", "f"]:

                    mapping_sheet = "Others - Female"

                elif gender_lower in ["male", "m"]:

                    mapping_sheet = "Others - Male"

                else:

                    mapping_sheet = "Others"

            else:

                mapping_sheet = "Others"

    print(f"Selected mapping sheet: {mapping_sheet}")

    # 5. Build the transformed payload with questions, answers, request_id, and user profile fields

    # Ensure any missing user profile fields are filled with default values

    for field, default_val in config.get("defaults", {}).items():

        if field not in normalized_data:

            normalized_data[field] = default_val

            if default_val:

                print(f"Field '{field}' missing in input, using default: {default_val}")

            else:

                print(f"Field '{field}' missing in input, defaulting to empty string")

    # Construct payload structure expected by backend

    payload = {

        "request_id": request_id,

        "questions": questions_list,

        "answers": answers_list

    }

    # Add normalized profile fields to payload

    for field in config.get("defaults", {}):

        if field in normalized_data:

            payload[field] = normalized_data[field]

    print("Transformed payload ready:", payload)

    # 6. Forward the normalized request to the backend (unless in preview mode)

    if preview:

        # 7. If preview=true, return the transformed payload without forwarding

        return JSONResponse(content=payload, status_code=200)

    backend_url = os.getenv("BACKEND_POST_URL")

    if not backend_url:

        raise HTTPException(status_code=500, detail="BACKEND_POST_URL is not configured")

    # Send the request to the actual backend URL

    try:

        import httpx

        async with httpx.AsyncClient() as client:

            backend_resp = await client.post(backend_url, json=payload)

    except Exception as exc:

        # Networking or connection error

        print(f"Error forwarding to backend: {exc}")

        raise HTTPException(status_code=502, detail=f"Failed to forward request: {exc}")

    # 8. If echo=true, include backend response status and body in the output

    if echo:

        try:

            backend_body = backend_resp.json()

        except ValueError:

            backend_body = backend_resp.text  # fallback to text if not JSON

        result = {

            "backend_status": backend_resp.status_code,

            "backend_response": backend_body

        }

        print("Backend response (echo mode):", result)

        return JSONResponse(content=result, status_code=200)

    # 6/9. Otherwise, return the backend's response directly (status code and content)

    return Response(content=backend_resp.content,

                    status_code=backend_resp.status_code,

                    media_type=backend_resp.headers.get("content-type", "application/json"))
 
