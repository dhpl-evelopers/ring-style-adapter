from fastapi import FastAPI, HTTPException, Body

import xml.etree.ElementTree as ET

import httpx

import logging

import json

import io

import os

# Configure logging to show INFO-level messages

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)

app = FastAPI()

# Load mapping configuration

with open('mapping.config.json') as f:

    mapping = json.load(f)

xml_root = mapping.get("xml_root", "Request")

field_map = mapping.get("field_map", {})

answer_map = mapping.get("answer_key_to_question", {})

result_keys = mapping.get("result_key_field_candidates", []) + ["request_id"]

@app.post("/ingest")

async def ingest(payload: dict = Body(...)):

    logger.info("Received payload: %s", payload)

    try:

        # Extract metadata fields with mapping or defaults

        data = {}

        # Required fields and their JSON aliases

        required_fields = {

            "full_name": ["full_name", "name"],

            "email": ["email"],

            "phone_number": ["phone_number", "phone"],

            "birth_date": ["birth_date", "date"]

        }

        for key, aliases in required_fields.items():

            found = None

            for alias in aliases:

                if alias in payload:

                    found = payload[alias]

                    xml_tag = field_map.get(alias, field_map.get(key, key))

                    data[xml_tag] = found

                    break

            if found is None:

                # Field missing

                raise HTTPException(status_code=400, detail=f"Missing required field: {key}")

        # Determine request_id or alias

        request_id = None

        for rk in result_keys:

            if rk in payload:

                request_id = payload[rk]

                break

        if not request_id:

            raise HTTPException(status_code=400, detail="Missing request_id/result_key field")

        data["RequestID"] = request_id

        # Build XML document

        root = ET.Element(xml_root)

        # Add metadata elements

        for tag, value in data.items():

            child = ET.SubElement(root, tag)

            child.text = str(value)

        logger.info("Added metadata elements to XML")

        # Handle answers/questions data

        # Priority 1: combined list of {"question": ..., "answer": ...}

        qa_list = None

        for val in payload.values():

            if isinstance(val, list) and val and isinstance(val[0], dict):

                if 'question' in val[0] and 'answer' in val[0]:

                    qa_list = val

                    break

        if qa_list:

            for item in qa_list:

                q_text = item.get("question", "")

                a_text = item.get("answer", "")

                ans_elem = ET.SubElement(root, "Answer")

                qt = ET.SubElement(ans_elem, "Question")

                qt.text = str(q_text)

                rt = ET.SubElement(ans_elem, "Response")

                rt.text = str(a_text)

            logger.info("Processed combined question-answer list")

        # Priority 2: separate "questions" and "answers" lists

        elif "questions" in payload and "answers" in payload:

            q_list = payload["questions"]

            a_list = payload["answers"]

            for q_item, a_item in zip(q_list, a_list):

                if isinstance(q_item, dict) and "question" in q_item:

                    q_text = q_item["question"]

                else:

                    q_text = q_item if isinstance(q_item, str) else ""

                if isinstance(a_item, dict) and "answer" in a_item:

                    a_text = a_item["answer"]

                else:

                    a_text = a_item if isinstance(a_item, str) else ""

                ans_elem = ET.SubElement(root, "Answer")

                qt = ET.SubElement(ans_elem, "Question")

                qt.text = str(q_text)

                rt = ET.SubElement(ans_elem, "Response")

                rt.text = str(a_text)

            logger.info("Processed separate questions and answers")

        # Priority 3: list under "answers" (either primitives or keyed objects)

        elif "answers" in payload:

            answers = payload["answers"]

            if isinstance(answers, list):

                for item in answers:

                    if isinstance(item, dict):

                        # keyed answers, e.g. {"who": "Alice", ...}

                        for key, val in item.items():

                            ans_elem = ET.SubElement(root, "Answer")

                            qt = ET.SubElement(ans_elem, "Question")

                            qt.text = answer_map.get(key, key)

                            rt = ET.SubElement(ans_elem, "Response")

                            rt.text = str(val)

                    else:

                        # simple answer string

                        ans_elem = ET.SubElement(root, "Answer")

                        ans_elem.text = str(item)

                logger.info("Processed answers list")

        else:

            logger.info("No answer data found in payload")

        # Write XML to bytes with declaration

        tree = ET.ElementTree(root)

        xml_bytes_io = io.BytesIO()

        tree.write(xml_bytes_io, encoding='utf-8', xml_declaration=True)

        xml_data = xml_bytes_io.getvalue()

        logger.info("Constructed XML: %s", xml_data.decode())

    except HTTPException as he:

        logger.error("Input validation error: %s", he.detail)

        raise he

    except Exception as exc:

        logger.exception("Unexpected error during XML construction")

        raise HTTPException(status_code=500, detail=str(exc))

    # Send XML to backend

    backend_url = os.getenv("BACKEND_URL", "http://localhost:9000/createRequest")

    headers = {"Content-Type": "application/xml"}

    try:

        async with httpx.AsyncClient() as client:

            response = await client.post(backend_url, data=xml_data, headers=headers)

        logger.info("Backend responded with status %s", response.status_code)

        if response.status_code >= 400:

            raise HTTPException(status_code=502, detail=f"Backend error: {response.status_code}")

    except httpx.RequestError as re:

        logger.error("Failed to send request to backend: %s", str(re))

        raise HTTPException(status_code=502, detail="Backend request failed")

    except HTTPException as he:

        raise he

    return {"status": "success", "backend_status": response.status_code}
Redirecting...
 
