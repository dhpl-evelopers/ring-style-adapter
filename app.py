# app.py

# v2.1 — Ring Style Adapter (FastAPI)

import os

import json

import uuid

import logging

from typing import Any, Dict, Optional, Tuple

import httpx

from fastapi import FastAPI, Request, HTTPException

from fastapi.responses import JSONResponse, PlainTextResponse, Response

from fastapi.middleware.cors import CORSMiddleware

try:

    import xmltodict

    from dicttoxml import dicttoxml

except Exception:  # pragma: no cover

    xmltodict = None

    dicttoxml = None


# -----------------------------

# Logging

# -----------------------------

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(

    level=LOG_LEVEL,

    format="%(asctime)s [%(levelname)s] %(message)s",

)

log = logging.getLogger("adapter")


# -----------------------------

# Environment / Config

# -----------------------------

app = FastAPI(title="Ring Style Adapter", version="2.1.0")

# Allow CORS if you need it (safe defaults)

app.add_middleware(

    CORSMiddleware,

    allow_origins=os.getenv("CORS_ALLOW_ORIGINS", "*").split(","),

    allow_credentials=True,

    allow_methods=["*"],

    allow_headers=["*"],

)

# Mandatory/optional env vars

BACKEND_POST_URL = os.getenv("BACKEND_POST_URL", "").strip()  # your backend endpoint

API_KEY_REQUIRED = os.getenv("API_KEY_REQUIRED", "false").lower() == "true"

API_KEY_HEADER = os.getenv("API_KEY_HEADER", "x-api-key")

API_KEY_VALUE = os.getenv("API_KEY", "")

REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "30"))

# --- mapping.config.json absolute path (critical fix) ---

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

CFG_PATH = os.path.join(BASE_DIR, "mapping.config.json")

# --------------------------------------------------------


# -----------------------------

# Helpers

# -----------------------------

def _read_mapping() -> Dict[str, Any]:

    """

    Load mapping.config.json if present.

    Expected shape (example):

    {

      "in_to_backend": { "ring.fieldA": "backend.fieldOne", "ring.user.id": "backend.customerId" },

      "backend_to_out": { "backend.orderId": "out.order_id" },

      "static_backend": { "source": "ring-adapter" }  # always added to backend payload

    }

    """

    if not os.path.exists(CFG_PATH):

        log.warning("mapping.config.json not found at %s; using pass-through.", CFG_PATH)

        return {

            "in_to_backend": {},

            "backend_to_out": {},

            "static_backend": {}

        }

    with open(CFG_PATH, "r", encoding="utf-8") as f:

        try:

            cfg = json.load(f)

        except json.JSONDecodeError as e:

            log.error("Invalid JSON in mapping.config.json: %s", e)

            raise

    # Normalize keys

    return {

        "in_to_backend": cfg.get("in_to_backend", {}),

        "backend_to_out": cfg.get("backend_to_out", {}),

        "static_backend": cfg.get("static_backend", {}),

    }


def _get_by_path(data: Dict[str, Any], path: str) -> Any:

    cur = data

    for part in path.split("."):

        if not isinstance(cur, dict) or part not in cur:

            return None

        cur = cur[part]

    return cur


def _set_by_path(target: Dict[str, Any], path: str, value: Any) -> None:

    cur = target

    parts = path.split(".")

    for p in parts[:-1]:

        if p not in cur or not isinstance(cur[p], dict):

            cur[p] = {}

        cur = cur[p]

    cur[parts[-1]] = value


def map_payload(

    payload: Dict[str, Any],

    mapping: Dict[str, str],

    static_items: Optional[Dict[str, Any]] = None

) -> Dict[str, Any]:

    """

    Build a new dict by mapping dotted paths from source->dest.

    """

    out: Dict[str, Any] = {}

    for src, dst in mapping.items():

        val = _get_by_path(payload, src)

        if val is not None:

            _set_by_path(out, dst, val)

    if static_items:

        for k, v in static_items.items():

            _set_by_path(out, k, v)

    return out


def ensure_json_body(content_type: str, body_bytes: bytes) -> Tuple[Dict[str, Any], str]:

    """

    Accept JSON or XML. Return python dict + detected_format ("json"|"xml").

    """

    ct = (content_type or "").lower()

    raw = body_bytes.decode("utf-8").strip() if body_bytes else ""

    # Prefer JSON if content-type says JSON or if it looks like JSON

    if "application/json" in ct or (raw.startswith("{") or raw.startswith("[")):

        try:

            return json.loads(raw) if raw else {}, "json"

        except json.JSONDecodeError as e:

            raise HTTPException(status_code=400, detail=f"Invalid JSON: {e}") from e

    # Fallback: XML (if allowed)

    if ("application/xml" in ct or raw.startswith("<")) and xmltodict is not None:

        try:

            return xmltodict.parse(raw) if raw else {}, "xml"

        except Exception as e:  # xml parsing

            raise HTTPException(status_code=400, detail=f"Invalid XML: {e}") from e

    # If xmltodict unavailable

    if "xml" in ct and xmltodict is None:

        raise HTTPException(status_code=415, detail="XML support not installed on server.")

    # Nothing recognized

    raise HTTPException(status_code=415, detail="Unsupported Content-Type. Use application/json or application/xml.")


def to_xml(data: Dict[str, Any]) -> bytes:

    if dicttoxml is None:

        raise HTTPException(status_code=500, detail="XML response requested but XML library not installed.")

    # dicttoxml returns bytes already

    return dicttoxml(data, custom_root="response", attr_type=False)


# -----------------------------

# Startup: load mapping once

# -----------------------------

MAPPING = _read_mapping()

log.info("Adapter ready. Mapping loaded from %s", CFG_PATH)


# -----------------------------

# Routes

# -----------------------------

@app.get("/", response_class=PlainTextResponse)

async def root() -> str:

    return "Ring Style Adapter OK"


@app.get("/ping", response_class=JSONResponse)

async def ping() -> Dict[str, Any]:

    return {"status": "ok", "version": app.version, "request_id": str(uuid.uuid4())}


@app.post("/adapter")

async def adapter(request: Request) -> Response:

    """

    Entry point for Ring/Phygital webhook.

    1) Accept JSON or XML.

    2) Map to backend schema using mapping.config.json.

    3) POST to BACKEND_POST_URL with optional API key header.

    4) Map backend response back to outward schema (if configured).

    5) Mirror client Accept header (json default).

    """

    if not BACKEND_POST_URL:

        raise HTTPException(status_code=500, detail="BACKEND_POST_URL is not configured.")

    # 1) Parse inbound body

    body_bytes = await request.body()

    src_payload, src_format = ensure_json_body(request.headers.get("content-type", ""), body_bytes)

    log.debug("Inbound format=%s payload=%s", src_format, src_payload)

    # If XML was posted, xmltodict returns a nested dict with a single root. That’s fine.

    # 2) Map to backend schema

    to_backend = map_payload(

        src_payload,

        MAPPING.get("in_to_backend", {}),

        MAPPING.get("static_backend", {}),

    )

    # If no mapping, default to pass-through

    backend_payload = to_backend or src_payload

    # 3) Call backend

    headers = {"content-type": "application/json"}

    if API_KEY_REQUIRED:

        if not API_KEY_VALUE:

            raise HTTPException(status_code=500, detail="API key required but API_KEY is not set.")

        headers[API_KEY_HEADER] = API_KEY_VALUE

    request_id = str(uuid.uuid4())

    headers["x-request-id"] = request_id

    try:

        async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT, verify=True) as client:

            resp = await client.post(BACKEND_POST_URL, json=backend_payload, headers=headers)

    except httpx.TimeoutException:

        log.exception("Backend timeout (%ss)", REQUEST_TIMEOUT)

        raise HTTPException(status_code=504, detail="Backend timeout.")

    except httpx.HTTPError as e:

        log.exception("Backend HTTP error")

        raise HTTPException(status_code=502, detail=f"Backend error: {e!s}") from e

    # 4) Map backend response

    try:

        backend_json: Dict[str, Any] = resp.json()

    except ValueError:

        # Not JSON? return raw text as-is

        backend_text = resp.text

        # Echo text (don’t attempt mapping)

        return PlainTextResponse(

            content=backend_text,

            status_code=resp.status_code,

            headers={"x-request-id": request_id},

        )

    mapped_out = map_payload(backend_json, MAPPING.get("backend_to_out", {}))

    outward = mapped_out or backend_json

    # 5) Negotiate response type: default JSON; use XML if client explicitly prefers it

    accept = (request.headers.get("accept") or "").lower()

    if "application/xml" in accept:

        xml_bytes = to_xml(outward)

        return Response(content=xml_bytes, media_type="application/xml", status_code=resp.status_code, headers={"x-request-id": request_id})

    return JSONResponse(content=outward, status_code=resp.status_code, headers={"x-request-id": request_id})


# -----------------------------

# Local dev (optional)

# -----------------------------

if __name__ == "__main__":

    import uvicorn

    uvicorn.run(

        "app:app",

        host=os.getenv("HOST", "0.0.0.0"),

        port=int(os.getenv("PORT", "8000")),

        log_level=LOG_LEVEL.lower(),

        reload=bool(os.getenv("RELOAD", "0") == "1"),

    )
 
