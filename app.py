from fastapi import FastAPI, Request, HTTPException

from fastapi.responses import JSONResponse, PlainTextResponse

import json

import xml.etree.ElementTree as ET

app = FastAPI()

def json_to_backend_xml(payload: dict) -> str:

    # expected JSON from client

    request_id   = payload.get("request_id") or payload.get("sessionId")

    full_name    = payload.get("full_name") or payload.get("customer", {}).get("name")

    email        = payload.get("email")     or payload.get("customer", {}).get("email")

    phone        = payload.get("phone_number") or payload.get("customer", {}).get("mobile")

    birth_date   = payload.get("birth_date")

    qas          = payload.get("questionAnswers") or payload.get("answers") or []

    root = ET.Element("root")

    # if your backend expects <request> wrapper, create it here instead

    # request = ET.SubElement(root, "request")

    ET.SubElement(root, "request_id").text   = str(request_id or "")

    ET.SubElement(root, "full_name").text    = str(full_name or "")

    ET.SubElement(root, "email").text        = str(email or "")

    ET.SubElement(root, "phone_number").text = str(phone or "")

    ET.SubElement(root, "birth_date").text   = str(birth_date or "")

    qa_parent = ET.SubElement(root, "questionAnswers")

    # accept both [{question:"", answer:""}] and [{id:"", value:""}]

    for qa in qas:

        q_el = ET.SubElement(qa_parent, "question")

        ET.SubElement(q_el, "text").text   = str(qa.get("question") or qa.get("key") or qa.get("id") or "")

        ET.SubElement(q_el, "answer").text = str(qa.get("answer")  or qa.get("value") or "")

    return ET.tostring(root, encoding="utf-8", method="xml").decode("utf-8")

def xml_to_payload(xml_string: str) -> dict:

    root = ET.fromstring(xml_string)

    payload = {

        "request_id":   (root.findtext("request_id") or "").strip(),

        "full_name":    (root.findtext("full_name") or "").strip(),

        "email":        (root.findtext("email") or "").strip(),

        "phone_number": (root.findtext("phone_number") or "").strip(),

        "birth_date":   (root.findtext("birth_date") or "").strip(),

        "questionAnswers": []

    }

    qa_parent = root.find("questionAnswers")

    if qa_parent is not None:

        for q in qa_parent.findall("question"):

            payload["questionAnswers"].append({

                "question": (q.findtext("text") or "").strip(),

                "answer":   (q.findtext("answer") or "").strip()

            })

    return payload

@app.post("/adapter")

async def adapter(request: Request):

    ctype = request.headers.get("content-type","").lower()

    try:

        if "application/json" in ctype:

            payload = await request.json()

        elif "application/xml" in ctype or "text/xml" in ctype or ctype == "":

            # also support swagger “no body” fallback if needed

            body = await request.body()

            if not body:

                raise HTTPException(status_code=415, detail="Empty body. Send JSON or XML.")

            payload = xml_to_payload(body.decode("utf-8"))

        else:

            raise HTTPException(status_code=415, detail="Unsupported Content-Type. Use application/json or application/xml.")

        # map JSON -> backend XML

        backend_xml = json_to_backend_xml(payload)

        # forward to backend

        import httpx, os

        backend_url = os.getenv("BACKEND_POST_URL")

        headers = {"Content-Type": "application/xml", "Accept": "application/xml"}

        async with httpx.AsyncClient(timeout=30) as client:

            resp = await client.post(backend_url, data=backend_xml.encode("utf-8"), headers=headers)

        # mirror or translate response

        # if backend returns XML, pass it through; or convert to JSON if you prefer

        return PlainTextResponse(resp.text, status_code=resp.status_code, media_type=resp.headers.get("content-type","text/xml"))

    except HTTPException:

        raise

    except Exception as e:

        # never reference .text of None

        return JSONResponse(status_code=500, content={"error":"adapter_failed", "detail": str(e)})
 
