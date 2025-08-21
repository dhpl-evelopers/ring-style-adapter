from fastapi import FastAPI, HTTPException, Response
import requests
import xml.sax.saxutils as saxutils

app = FastAPI()

# Configure backend base URL (adjust as needed for your environment)
BACKEND_BASE_URL = "http://your-backend-service"  # e.g., http://localhost:8001 or actual host

def build_request_xml(data: dict) -> str:
    """
    Helper function to construct the XML payload string from the input JSON data.
    Expects 'questions' and 'answers' lists and metadata fields in the data.
    """
    # Ensure required fields are present
    required_fields = ["request_id", "name", "phoneNumber", "birth_date", "email", "questions", "answers"]
    for field in required_fields:
        if field not in data:
            raise KeyError(field)
    questions = data["questions"]
    answers = data["answers"]
    # Validate types and lengths
    if not isinstance(questions, list) or not isinstance(answers, list):
        raise ValueError("The 'questions' and 'answers' fields must be lists.")
    if len(questions) != len(answers):
        raise ValueError("The number of questions and answers must match.")
    # Build list of question/answer dicts
    qa_list = []
    for q, a in zip(questions, answers):
        qa_list.append({
            "question": str(q),
            "selectedOption": {"value": str(a)}
        })
    # Convert list to string (this will produce a string with Python dict syntax using single quotes)
    qa_list_str = str(qa_list)
    # Escape special XML characters in the content
    qa_list_str_escaped = saxutils.escape(qa_list_str)
    # Construct the XML payload with a root element and all required sub-elements
    xml_payload = (
        "<applicationRequest>"
        f"<request_id>{saxutils.escape(str(data['request_id']))}</request_id>"
        f"<name>{saxutils.escape(str(data['name']))}</name>"
        f"<phoneNumber>{saxutils.escape(str(data['phoneNumber']))}</phoneNumber>"
        f"<birth_date>{saxutils.escape(str(data['birth_date']))}</birth_date>"
        f"<email>{saxutils.escape(str(data['email']))}</email>"
        f"<questionAnswers>{qa_list_str_escaped}</questionAnswers>"
        "</applicationRequest>"
    )
    return xml_payload

@app.post("/createRequest")
async def create_request(data: dict):
    """
    Accepts a JSON payload with questions and answers, converts it to the XML format expected by the backend,
    and forwards the request to the backend's /createRequest endpoint.
    """
    # Transform input JSON to XML payload
    try:
        xml_payload = build_request_xml(data)
    except KeyError as e:
        # Missing required field
        raise HTTPException(status_code=422, detail=f"Missing field in request: {e.args[0]}")
    except ValueError as e:
        # Input format/validation error
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        # Unexpected error during transformation
        raise HTTPException(status_code=500, detail="Failed to generate XML payload for request.")

    # Call the backend /createRequest endpoint with the XML payload
    backend_url = f"{BACKEND_BASE_URL}/createRequest"
    try:
        backend_response = requests.post(
            backend_url,
            data=xml_payload,
            headers={"Content-Type": "application/xml"}
        )
    except Exception as e:
        # Handle connection errors or other request issues
        raise HTTPException(status_code=502, detail=f"Error connecting to backend: {e}")

    # If the backend returns an error status, forward that as an HTTP exception
    if backend_response.status_code != 200:
        raise HTTPException(status_code=backend_response.status_code, detail=backend_response.text)

    # On success, determine how to return the response to the client
    content_type = backend_response.headers.get("Content-Type", "")
    # If backend responded with JSON, return it directly as JSON
    if "application/json" in content_type:
        try:
            return backend_response.json()
        except ValueError:
            # If JSON parsing fails, return raw text
            return Response(content=backend_response.text, media_type="application/json")
    # If backend returned XML, forward it as XML in the response
    if "xml" in content_type:
        return Response(content=backend_response.content, media_type=content_type)
    # For any other content types, return raw text
    return backend_response.text
