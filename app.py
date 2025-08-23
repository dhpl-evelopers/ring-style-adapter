import os

import json

import time

import logging

from typing import Dict, Any, List, Tuple, Optional

import requests

from flask import Flask, request, jsonify

from lxml import etree

# -----------------------------------------------------------------------------

# Logging

# -----------------------------------------------------------------------------

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(

    level=getattr(logging, LOG_LEVEL, logging.INFO),

    format="%(asctime)s | %(levelname)s | %(message)s",

)

log = logging.getLogger("adapter")

# -----------------------------------------------------------------------------

# Env

# -----------------------------------------------------------------------------

BACKEND_BASE_URL   = os.getenv("BACKEND_BASE_URL")                        # e.g. https://projectaiapi-<...>.azurewebsites.net

CREATE_PATH        = os.getenv("BACKEND_CREATE_PATH", "/createRequest")   # POST by default

FETCH_PATH         = os.getenv("BACKEND_FETCH_PATH", "/fetchResponse")    # GET by default (configurable)

FETCH_METHOD       = os.getenv("BACKEND_FETCH_METHOD", "GET").upper()     # GET or POST

TIMEOUT_SEC        = int(os.getenv("BACKEND_TIMEOUT_SEC", "25"))

MAPPING_PATH       = os.getenv("MAPPING_PATH", "/home/site/wwwroot/mapping.config.json")

ALLOW_UNKNOWN      = os.getenv("ALLOW_UNKNOWN_QUESTIONS", "false").lower() == "true"

FETCH_RETRIES      = int(os.getenv("FETCH_RETRIES", "6"))

FETCH_SLEEP_SEC    = float(os.getenv("FETCH_SLEEP_SEC", "2.0"))

# Extra headers if your backend needs them (optional)

EXTRA_HEADERS_JSON = os.getenv("BACKEND_EXTRA_HEADERS_JSON", "").strip()

EXTRA_HEADERS = {}

if EXTRA_HEADERS_JSON:

    try:

        EXTRA_HEADERS = json.loads(EXTRA_HEADERS_JSON)

    except Exception:

        log.warning("BACKEND_EXTRA_HEADERS_JSON is not valid JSON; ignoring.")

# -----------------------------------------------------------------------------

# App

# -----------------------------------------------------------------------------

app = Flask(__name__)

# -----------------------------------------------------------------------------

# Mapping loader

# -----------------------------------------------------------------------------

def load_mapping(path: str) -> Dict[str, Any]:

    log.info(f"Loading mapping from {path}")

    with open(path, "r", encoding="utf-8") as f:

        m = json.load(f)

    # Quick sanity

    if "questions" not in m:

        raise RuntimeError("mapping.json missing 'questions' root")

    return m

try:

    MAPPING = load_mapping(MAPPING_PATH)

    MUST_HAVE = set(MAPPING.get("must_have_questions_keys", []))

    log.info(f"Mapping loaded. must_have_questions_keys={sorted(MUST_HAVE)}")

except Exception as e:

    log.exception("Failed to load mapping at startup")

    raise

# -----------------------------------------------------------------------------

# Utilities

# -----------------------------------------------------------------------------

def _sanitize_for_log(s: str, maxlen: int = 1500) -> str:

    if not isinstance(s, str):

        s = str(s)

    return (s[:maxlen] + " …(trunc)") if len(s) > maxlen else s

def _labels_index(mapping: Dict[str, Any]) -> Dict[str, str]:

    """

    Build a lowercase label -> canonical key index for quick text matching.

    """

    idx = {}

    for key, meta in mapping.get("questions", {}).items():

        for lbl in meta.get("labels", []):

            idx[lbl.strip().lower()] = key

    return idx

LABELS_INDEX = _labels_index(MAPPING)

def _find_key_by_label(question_text: str) -> Optional[str]:

    if not question_text:

        return None

    return LABELS_INDEX.get(question_text.strip().lower())

def normalize_answers(ui_payload: Dict[str, Any]) -> Tuple[List[Dict[str, str]], List[str]]:

    """

    Accepts flexible UI payload, returns a normalized list of {'key', 'question', 'answer'}.

    Also returns list of missing keys (if required).

    """

    qas_in = ui_payload.get("questionAnswers", []) or []

    normalized = []

    seen_keys = set()

    for item in qas_in:

        # The UI can send: {"id": "Q1", "question": "Who are you purchasing for?", "answer": "Self"}

        q_text = item.get("question")

        q_id   = item.get("id")

        answer = item.get("answer", "")

        # Prefer canonical key by label text; fallback to id if your UI still sends "Q1" etc

        key = _find_key_by_label(q_text)

        if not key and isinstance(q_id, str):

            # If your mapping uses keys like q1_purchasing_for and your UI sends "Q1", map here if you want:

            qid_lower = q_id.strip().lower()

            if qid_lower in MAPPING["questions"]:

                key = qid_lower  # only if your mapping actually defined "q1", etc.

        if not key:

            # Keep unknown if allowed

            if ALLOW_UNKNOWN:

                normalized.append({"key": "__unknown__", "question": q_text or q_id or "", "answer": answer})

                continue

            else:

                # skip unknown question but log it

                log.warning(f"Unknown question: {_sanitize_for_log(q_text or q_id)} (set ALLOW_UNKNOWN_QUESTIONS=true to pass through)")

                continue

        if key in seen_keys:

            # Take the first; ignore subsequent duplicates (or you can overwrite)

            log.info(f"Duplicate question '{key}' ignored; first one kept")

            continue

        seen_keys.add(key)

        normalized.append({"key": key, "question": q_text or key, "answer": answer})

    # Validate required keys

    missing = []

    if MUST_HAVE and not ALLOW_UNKNOWN:

        present = {x["key"] for x in normalized if x["key"] != "__unknown__"}

        missing = sorted(list(MUST_HAVE - present))

    return normalized, missing

def json_to_xml(ui_payload: Dict[str, Any], normalized_qas: List[Dict[str, str]]) -> str:

    """

    Build XML in the exact shape your backend log expects.

    """

    root = etree.Element("Request")

    # top-level fields

    etree.SubElement(root, "FullName").text    = ui_payload.get("full_name", ui_payload.get("name", ""))

    etree.SubElement(root, "Email").text       = ui_payload.get("email", "")

    etree.SubElement(root, "PhoneNumber").text = ui_payload.get("phone_number", "")

    etree.SubElement(root, "BirthDate").text   = ui_payload.get("birth_date", "")

    etree.SubElement(root, "RequestId").text   = ui_payload.get("request_id", "")

    etree.SubElement(root, "ResultKey").text   = ui_payload.get("result_key", "")

    qa_root = etree.SubElement(root, "QuestionAnswers")

    for qa in normalized_qas:

        q_el = etree.SubElement(qa_root, "QA")

        etree.SubElement(q_el, "Question").text = qa["question"]

        etree.SubElement(q_el, "Answer").text   = qa["answer"]

    return etree.tostring(root, pretty_print=True, encoding="utf-8").decode("utf-8")

def call_backend_create(xml_body: str) -> requests.Response:

    url = f"{BACKEND_BASE_URL.rstrip('/')}{CREATE_PATH}"

    headers = {"Content-Type": "application/xml"}

    headers.update(EXTRA_HEADERS)

    log.info(f"POST {url}")

    log.debug(f"XML payload:\n{_sanitize_for_log(xml_body, 4000)}")

    return requests.post(url, data=xml_body.encode("utf-8"), headers=headers, timeout=TIMEOUT_SEC)

def call_backend_fetch(request_id: str) -> requests.Response:

    url = f"{BACKEND_BASE_URL.rstrip('/')}{FETCH_PATH}"

    headers = {"Accept": "*/*"}

    headers.update(EXTRA_HEADERS)

    if FETCH_METHOD == "GET":

        url = f"{url}?response_id={request_id}"

        log.info(f"GET {url}")

        return requests.get(url, headers=headers, timeout=TIMEOUT_SEC)

    else:

        body = {"response_id": request_id}

        log.info(f"POST {url} (fetch)")

        log.debug(f"Fetch body: {body}")

        return requests.post(url, json=body, headers=headers, timeout=TIMEOUT_SEC)

# -----------------------------------------------------------------------------

# Health

# -----------------------------------------------------------------------------

@app.get("/health")

def health():

    return {"status": "ok"}

# -----------------------------------------------------------------------------

# Main adapter endpoint

# -----------------------------------------------------------------------------

@app.post("/adapter")

def adapter():

    if not BACKEND_BASE_URL:

        return jsonify({"error": "BACKEND_BASE_URL not configured"}), 500

    try:

        payload = request.get_json(force=True, silent=False)

    except Exception:

        log.exception("Failed to parse JSON")

        return jsonify({"error": "Invalid JSON"}), 400

    log.info("Incoming UI JSON")

    log.debug(_sanitize_for_log(json.dumps(payload, ensure_ascii=False), 4000))

    # Normalize flexible UI

    normalized, missing = normalize_answers(payload)

    if missing and not ALLOW_UNKNOWN:

        log.warning(f"Missing required keys: {missing}")

        return jsonify({"error": "Mandatory questions missing", "missing_keys": missing}), 400

    # Build XML exactly as backend expects

    xml_body = json_to_xml(payload, normalized)

    # send create

    try:

        create_resp = call_backend_create(xml_body)

    except Exception as e:

        log.exception("Error calling backend create")

        return jsonify({"backend_error": str(e)}), 502

    log.info(f"Backend create status={create_resp.status_code}")

    log.debug(f"Backend create body:\n{_sanitize_for_log(create_resp.text, 4000)}")

    if create_resp.status_code not in (200, 201, 202):

        return jsonify({

            "error": "Backend create failed",

            "status": create_resp.status_code,

            "body": create_resp.text

        }), 502

    # Poll fetch (optional; remove if your backend returns everything on create)

    req_id = payload.get("request_id", "")

    fetch_text = ""

    if FETCH_PATH:

        for i in range(FETCH_RETRIES):

            try:

                fetch_resp = call_backend_fetch(req_id)

                log.info(f"Fetch try {i+1}/{FETCH_RETRIES} status={fetch_resp.status_code}")

                if fetch_resp.status_code == 200 and fetch_resp.text:

                    fetch_text = fetch_resp.text

                    break

            except Exception:

                log.exception("Error during backend fetch")

                # continue polling

            time.sleep(FETCH_SLEEP_SEC)

    # Return a clear JSON

    return jsonify({

        "status": "ok",

        "request_id": req_id,

        "normalized": [

            {"key": qa["key"], "question": qa["question"], "answer_text": qa["answer"]}

            for qa in normalized

        ],

        "backend_create_status": create_resp.status_code,

        "backend_fetch": fetch_text

    }), 200

# -----------------------------------------------------------------------------

# Azure entrypoint when run directly

# -----------------------------------------------------------------------------

if __name__ == "__main__":

    # For local run; in Azure use gunicorn

    app.run(host="0.0.0.0", port=8000)
 
