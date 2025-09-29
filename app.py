from flask import Flask, request, jsonify
import xml.etree.ElementTree as ET
import requests
import os

app = Flask(__name__)

BACKEND_URL = os.getenv("BACKEND_URL")  # Your backend endpoint

@app.route("/adapter", methods=["POST"])
def adapter():
    try:
        data = request.get_json()
        
        # 1️⃣ Convert JSON to XML with <questionAnswers>
        root = ET.Element("Request")
        for key, value in data.items():
            if key != "questionAnswers":
                ET.SubElement(root, key).text = str(value)
        
        qna_root = ET.SubElement(root, "QuestionAnswers")
        for qna in data.get("questionAnswers", []):
            qa_elem = ET.SubElement(qna_root, "QA")
            ET.SubElement(qa_elem, "Question").text = qna["question"]
            ET.SubElement(qa_elem, "Answer").text = qna["answer"]

        xml_payload = ET.tostring(root, encoding="utf-8")
        
        # 2️⃣ Send XML payload to backend
        headers = {"Content-Type": "application/xml"}
        backend_resp = requests.post(BACKEND_URL, data=xml_payload, headers=headers)

        return jsonify({
            "status": backend_resp.status_code,
            "backend": backend_resp.json() if backend_resp.status_code == 200 else backend_resp.text
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
