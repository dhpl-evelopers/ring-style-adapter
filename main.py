# main.py (FastAPI)
import logging, uuid, xml.etree.ElementTree as ET
from typing import Dict, Any, List
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

app = FastAPI()
STORE: Dict[str, Dict[str, Any]] = {}

def _text(root, *xpaths: str) -> str:
    for xp in xpaths:
        node = root.find(xp)
        if node is not None and node.text:
            v = node.text.strip()
            if v:
                return v
    return ""

@app.post("/createRequest")
async def createRequest(request: Request):
    xml = (await request.body()).decode("utf-8", errors="ignore")
    logging.info("xml_string :  %s", xml)

    # initialize for safe logging
    name = email = phone_number = request_id = birth_date = ""
    qas: List[Dict[str, str]] = []

    try:
        root = ET.fromstring(xml)
        name         = _text(root, ".//full_name", ".//FullName")
        email        = _text(root, ".//email", ".//Email")
        phone_number = _text(root, ".//phone_number", ".//PhoneNumber")
        request_id   = _text(root, ".//request_id", ".//RequestId", ".//RequestID")
        birth_date   = _text(root, ".//date_of_birth", ".//DateOfBirth", ".//dob", ".//DOB")

        parent = root.find(".//question_answers") or root.find(".//QuestionAnswers")
        if parent is not None:
            for qa in list(parent.findall(".//qa")) + list(parent.findall(".//QA")):
                q = _text(qa, ".//question", ".//Question")
                a = _text(qa, ".//answer", ".//Answer")
                if q or a:
                    qas.append({"question": q, "answer": a})

    except Exception as e:
        logging.exception("XML mapping error name=%r email=%r phone=%r req=%r birth=%r",
                          name, email, phone_number, request_id, birth_date)
        return JSONResponse(status_code=422, content={"error": "mapping_error", "details": str(e)})

    missing = [k for k, v in [("full_name", name), ("email", email), ("request_id", request_id)] if not v]
    if missing:
        return JSONResponse(status_code=422, content={"error": "missing_fields", "missing": missing})

    rid = str(uuid.uuid4())
    STORE[rid] = {
        "status": "done",
        "request_id": request_id,
        "result_key": _text(root, ".//result_key", ".//ResultKey"),
        "full_name": name,
        "email": email,
        "phone_number": phone_number,
        "date_of_birth": birth_date,
        "qas": qas,
        "recommended_styles": ["Band Ring Style", "Couple Ring Style"],
    }
    return {"response_id": rid, "status": "accepted"}

@app.get("/fetchResponse")
async def fetchResponse(response_id: str = "", responseId: str = ""):
    rid = response_id or responseId
    item = STORE.get(rid)
    if not item:
        return JSONResponse(status_code=404, content={"error": "response_id_not_found"})
    return item

@app.get("/health")
async def health():
    return {"status": "ok"}
