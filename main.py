# main.py
import logging
import uuid
from typing import Dict, Any, List
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import xml.etree.ElementTree as ET

app = FastAPI()
STORE: Dict[str, Dict[str, Any]] = {}

def _text(root, *xpaths) -> str:
    for xp in xpaths:
        node = root.find(xp)
        if node is not None and node.text is not None:
            val = node.text.strip()
            if val:
                return val
    return ""

@app.get("/health")
async def health():
    return {"status": "ok", "service": "ring-style-backend"}

@app.post("/createRequest")
async def createRequest(request: Request):
    xml_bytes = await request.body()
    xml_string = xml_bytes.decode("utf-8", errors="ignore")
    logging.info("xml_string :  %s", xml_string)

    try:
        root = ET.fromstring(xml_string)
    except Exception as e:
        logging.exception("Invalid XML")
        return JSONResponse(status_code=400, content={"error": "invalid_xml", "details": str(e)})

    try:
        name         = _text(root, ".//full_name", ".//FullName")
        email        = _text(root, ".//email", ".//Email")
        phone_number = _text(root, ".//phone_number", ".//PhoneNumber", ".//contact", ".//Contact")
        request_id   = _text(root, ".//request_id", ".//RequestId", ".//RequestID")
        result_key   = _text(root, ".//result_key", ".//ResultKey")
        birth_date   = _text(root, ".//date_of_birth", ".//DateOfBirth", ".//dob", ".//DOB")

        qa_parent = root.find(".//question_answers") or root.find(".//QuestionAnswers")
        qas: List[Dict[str, str]] = []
        if qa_parent is not None:
            for qa in list(qa_parent.findall(".//qa")) + list(qa_parent.findall(".//QA")):
                q = _text(qa, ".//question", ".//Question")
                a = _text(qa, ".//answer", ".//Answer")
                if q or a:
                    qas.append({"question": q, "answer": a})

    except Exception as e:
        logging.exception("XML mapping error")
        return JSONResponse(status_code=422, content={"error": "mapping_error", "details": str(e)})

    missing = [k for k, v in [("full_name", name), ("email", email), ("request_id", request_id)] if not v]
    if missing:
        return JSONResponse(status_code=422, content={"error": "missing_fields", "missing": missing})

    response_id = str(uuid.uuid4())
    STORE[response_id] = {
        "status": "done",
        "request_id": request_id,
        "result_key": result_key,
        "full_name": name,
        "email": email,
        "phone_number": phone_number,
        "date_of_birth": birth_date,
        "qas": qas,
        "recommended_styles": ["Band Ring Style", "Couple Ring Style"]
    }

    return {"response_id": response_id, "status": "accepted"}

@app.get("/fetchResponse")
async def fetchResponse(response_id: str = "", responseId: str = ""):
    rid = response_id or responseId
    item = STORE.get(rid)
    if not item:
        return JSONResponse(status_code=404, content({"error": "response_id_not_found"}))
    return item
