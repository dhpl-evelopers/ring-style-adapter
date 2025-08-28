import os
import json
import time
import uuid
import logging
from typing import Dict, Any, List, Tuple, Optional

from flask import Flask, request, jsonify, g
from werkzeug.middleware.proxy_fix import ProxyFix
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from xml.etree.ElementTree import Element, SubElement, tostring

# NEW: database imports
import psycopg2
from psycopg2.extras import execute_values

# ==================== Config ====================

def _getenv(name: str, default: Optional[str] = None, required: bool = False) -> str:
    v = os.getenv(name, default)
    if required and not (v and str(v).strip()):
        raise RuntimeError(f"Missing required env var: {name}")
    return (v or "").strip()

BACKEND_BASE_URL    = _getenv("BACKEND_BASE_URL", required=True).rstrip("/")
CREATE_PATH         = _getenv("BACKEND_CREATE_PATH", "/createRequest")
FETCH_PATH          = _getenv("BACKEND_FETCH_PATH", "/fetchResponse")
BACKEND_TIMEOUT_S   = int(_getenv("BACKEND_TIMEOUT_SEC", "25"))
MAPPING_PATH        = _getenv("MAPPING_PATH", "/home/site/wwwroot/mapping.config.json")
API_KEY_REQUIRED    = _getenv("API_KEY_REQUIRED", "false").lower() == "true"
MAX_CONTENT_LENGTH  = int(_getenv("MAX_CONTENT_LENGTH", str(512 * 1024)))
LOG_LEVEL           = _getenv("LOG_LEVEL", "INFO").upper()
LOG_XML_ALWAYS      = _getenv("LOG_XML_ALWAYS", "false").lower() == "true"

# switches
REQUIRE_ONLY_USER_FIELDS = _getenv("REQUIRE_ONLY_USER_FIELDS", "true").lower() == "true"
POSITIONAL_QA_ENABLED    = _getenv("POSITIONAL_QA_ENABLED", "true").lower() == "true"
REQUIRE_RESULT_KEY       = _getenv("REQUIRE_RESULT_KEY", "true").lower() == "true"
RESPONSE_MODE            = _getenv("RESPONSE_MODE", "full").lower()   # "full" or "minimal"

# ==================== App ====================

app = Flask(__name__)
app.config["JSON_SORT_KEYS"] = False
app.config["MAX_CONTENT_LENGTH"] = MAX_CONTENT_LENGTH
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_port=1, x_prefix=1)

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("adapter")

# ==================== HTTP Session ====================

def _session() -> requests.Session:
    s = requests.Session()
    retries = Retry(total=3, backoff_factor=0.4,
                    status_forcelist=(429, 500, 502, 503, 504),
                    allowed_methods=frozenset(["GET","POST"]),
                    raise_on_status=False)
    ad = HTTPAdapter(max_retries=retries, pool_connections=20, pool_maxsize=50)
    s.mount("http://", ad)
    s.mount("https://", ad)
    return s

HTTP = _session()

# ==================== Database ====================

def _get_db_conn():
    return psycopg2.connect(
        host="projectai-pict-postgresql.postgres.database.azure.com",
        dbname="projectai-ringsandi-pict",
        user="postgres",
        password="Apollo11",
        port=5432,
        sslmode="require",
    )

def _store_request_and_qna(user: Dict[str, Any], qas: List[Dict[str, Any]]) -> None:
    try:
        conn = _get_db_conn()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO requests (request_id, name, email, phonenumber, birth_date, timestamp)
            VALUES (%s,%s,%s,%s,%s,NOW())
            ON CONFLICT (request_id) DO NOTHING
        """, (
            user.get("result_key",""),
            user.get("full_name",""),
            user.get("email",""),
            user.get("phone_number",""),
            user.get("birth_date","")
        ))

        if qas:
            qna_rows = [
                (str(uuid.uuid4()), user.get("result_key",""),
                 qa.get("question_text",""), qa.get("answer_text",""), idx)
                for idx, qa in enumerate(qas)
            ]
            execute_values(cur, """
                INSERT INTO requests_qna (qna_id, request_id, question, answer, index)
                VALUES %s
            """, qna_rows)

        conn.commit()
        cur.close(); conn.close()
        logger.info("DB saved request=%s qna_count=%d", user.get("result_key",""), len(qas))
    except Exception as e:
        logger.exception("DB insert failed: %s", e)

# ==================== Mapping / Validation (shortened for clarity) ====================
# Keep your existing Mapping, _load_mapping, _extract_question_and_answer, _validate etc.
# ---------------------- (not repeated here, unchanged from your last version) ----------------------

# ==================== Backend Call ====================
# Keep your _xml_superset, _get_retry_after, _extract_response_id, _call_backend etc.

# ==================== Routes ====================

@app.post("/adapter")
def adapter():
    if API_KEY_REQUIRED:
        err = _require_api_key(request.headers)
        if err:
            return _json_error(401,"unauthorized",err)
    if MAPPING is None:
        return _json_error(503,"not_ready","Mapping not loaded")

    try:
        payload = request.get_json(force=True, silent=False)
        if not isinstance(payload, dict):
            return _json_error(400,"bad_request","Body must be a JSON object")
    except Exception as e:
        return _json_error(400,"bad_json",f"Invalid JSON: {e}")

    try:
        user,qas = _validate(payload,MAPPING)
    except ValueError as ve:
        try:
            return jsonify({"status":"error","error":json.loads(str(ve)),"correlation_id":g.cid}),400
        except Exception:
            return _json_error(400,"validation_error",str(ve))

    # Save into DB
    _store_request_and_qna(user,qas)

    # normalize_only shortcut
    if str(payload.get("normalize_only","")).lower() in ("1","true","yes"):
        return jsonify({
            "status":"ok",
            "request_id":user["request_id"],
            "result_key":user["result_key"],
            "normalized":[{"key":qa["key"],"question":qa["question_text"],"answer":qa["answer_text"]} for qa in qas],
            "correlation_id":g.cid
        })

    # Build XML + call backend
    xml_body=_xml_superset(user,qas)
    try:
        backend_result=_call_backend(xml_body,g.cid,user)
    except Exception as e:
        logger.exception("Backend call failed cid=%s", g.cid)
        body={"details":str(e)}
        return _json_error(502,"backend_error","Upstream backend call failed",body)

    # -------- Response shaping --------
    req_mode = (request.headers.get("x-response-mode") or request.args.get("mode") or RESPONSE_MODE).lower()

    if req_mode=="minimal":
        be=backend_result or {}
        payload=None
        if isinstance(be.get("backend_final"),dict):
            payload=be["backend_final"].get("data",be["backend_final"])
        if payload is None and isinstance(be.get("backend_create"),dict):
            payload=be["backend_create"]
        if payload is None:
            payload=be.get("backend_raw")

        out={"status":"ok","result_key":user.get("result_key","")}
        if user.get("request_id"): out["request_id"]=user["request_id"]
        if payload is not None: out["data"]=payload
        return jsonify(out)

    # default full
    result_payload={
        "status":"ok",
        "request_id":user["request_id"],
        "result_key":user["result_key"],
        "normalized":[{"key":qa["key"],"question":qa["question_text"],"answer":qa["answer_text"]} for qa in qas],
        "backend":backend_result,
        "correlation_id":g.cid
    }
    return jsonify(result_payload)

# ==================== Main ====================

if __name__=="__main__":
    app.run(host="0.0.0.0",port=int(os.getenv("PORT","8000")))
