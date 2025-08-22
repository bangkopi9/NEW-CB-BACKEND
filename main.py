# -*- coding: utf-8 -*-
"""
Planville Chatbot Backend (FastAPI)
- Chat endpoints (non-stream, stream, SSE) dengan optional RAG/scraper
- Intent/keyword gate + simple rate limit
- Funnel qualification: /funnel/validate
- Lead to CRM (HTTP) + fallback queue: /lead, /queue/flush
- Event tracking sink: /track
- Healthcheck: /health
"""

import os
import json
import time
import logging
from typing import Dict, List, Optional, Deque, Any
from collections import defaultdict, deque
from datetime import datetime, timezone

import requests
from fastapi import FastAPI, HTTPException, Request, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field, EmailStr

# -----------------------------------------------------------------------------
# Optional dependencies (RAG/scraper/OpenAI) - safe import
# -----------------------------------------------------------------------------
try:
    from rag_engine import query_index  # type: ignore
except Exception:
    query_index = None

try:
    from scraper import get_scraped_context  # type: ignore
except Exception:
    get_scraped_context = None

try:
    from openai import OpenAI  # type: ignore
    _OPENAI = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
except Exception:
    _OPENAI = None

# -----------------------------------------------------------------------------
# Build metadata (optional)
# -----------------------------------------------------------------------------
APP_VERSION = os.getenv("APP_VERSION", "dev")
COMMIT_SHA = os.getenv("COMMIT_SHA", "")
BUILD_TIME_ISO = os.getenv("BUILD_TIME", datetime.now(timezone.utc).isoformat())

# -----------------------------------------------------------------------------
# Paths / storage
# -----------------------------------------------------------------------------
DATA_DIR = os.path.join(os.getcwd(), "data")
LEADS_PATH = os.path.join(DATA_DIR, "leads.jsonl")
QUEUE_PATH = os.path.join(DATA_DIR, "queue.jsonl")
EVENTS_PATH = os.path.join(DATA_DIR, "events.jsonl")
os.makedirs(DATA_DIR, exist_ok=True)

# CRM env (boleh kosong)
CRM_URL = (os.getenv("CRM_URL") or "").strip()
CRM_TOKEN = (os.getenv("CRM_TOKEN") or "").strip()

# -----------------------------------------------------------------------------
# App + CORS
# -----------------------------------------------------------------------------
app = FastAPI(title="Planville Chatbot Backend", version=APP_VERSION)

app.add_middleware(
    CORSMiddleware,
    # NOTE: di production batasi ke domain vercel dan planville
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------------------------------------------------------
# Logging & intent helpers
# -----------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(message)s")
intent_logger = logging.getLogger("intent")

VALID_KEYWORDS = [
    # German
    "photovoltaik", "pv", "solaranlage", "dach", "wärmepumpe", "klimaanlage",
    "angebot", "kosten", "preise", "förderung", "termin", "beratung",
    "installation", "montage", "wartung", "service", "garantie",
    # English
    "photovoltaics", "solar", "roof", "heat pump", "air conditioner", "ac",
    "quote", "cost", "price", "subsidy", "appointment", "consultation",
    "install", "maintenance", "warranty"
]

def _match_intent(text: str) -> bool:
    if not text:
        return False
    t = text.lower()
    return any(k in t for k in VALID_KEYWORDS)

# simple in-memory rate limiter
REQUEST_BUCKETS: Dict[str, Deque[float]] = defaultdict(deque)
def _allow_request(bucket: str, limit: int, window_sec: int) -> bool:
    now = time.time()
    q = REQUEST_BUCKETS[bucket]
    while q and (now - q[0]) > window_sec:
        q.popleft()
    if len(q) >= limit:
        return False
    q.append(now)
    return True

# intent analytics
INTENT_LOG_PATH = os.getenv("INTENT_LOG_PATH")  # optional sink file
def log_intent_analytics(text: str, kw_hit: bool, sem_score: float, source: str):
    rec = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "source": source,
        "kw": bool(kw_hit),
        "sem_score": float(sem_score or 0.0),
        "text": (text or "")[:512],
    }
    try:
        intent_logger.info(json.dumps(rec, ensure_ascii=False))
    except Exception:
        pass
    if INTENT_LOG_PATH:
        try:
            with open(INTENT_LOG_PATH, "a", encoding="utf-8") as f:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        except Exception:
            pass

# dummy semantic score (you can wire your own embedder here)
def _semantic_score(text: str) -> float:
    if not text:
        return 0.0
    # placeholder: lightweight heuristic
    t = text.lower()
    hits = sum(1 for k in ("pv", "dach", "roof", "wärmepumpe", "heat pump", "planville") if k in t)
    return min(1.0, 0.2 * hits)

# -----------------------------------------------------------------------------
# Chat request/response schemas
# -----------------------------------------------------------------------------
class ChatRequest(BaseModel):
    message: str
    lang: str = "de"

# prompt builder (ASCII safe)
def _build_prompt(user_message: str, context_text: str, lang: str, intent_ok: bool) -> str:
    cta = "Weitere Fragen? Kontakt: https://planville.de/kontakt" if lang == "de" else \
          "More questions? Contact: https://planville.de/kontakt"
    style = "Antworte präzise, professionell und freundlich." if lang == "de" else \
            "Answer concisely, professionally, and helpfully."
    scope = (
        "Thema: Photovoltaik, Dachsanierung, Wärmepumpe, Klimaanlage. Antworte auf Basis des CONTEXT unten. "
        "Wenn CONTEXT nicht ausreicht, antworte kurz (1–2 Saetze) und fuege am Ende den CTA hinzu."
        if lang == "de"
        else "Topics: Photovoltaics, roofing, heat pumps, air conditioning. Answer based on CONTEXT below. "
             "If CONTEXT is insufficient, reply briefly (1–2 sentences) and append the CTA."
    )
    soft_gate = (
        "Falls die Nutzerfrage klar ausserhalb der Themen ist, antworte sehr kurz (1–2 Saetze) und fuege den CTA hinzu."
        if lang == "de"
        else "If the user question is clearly off-topic, answer very briefly (1–2 sentences) and append the CTA."
    )
    prompt = f"""{style}
{scope}
{soft_gate}

CONTEXT:
{context_text or ''}

USER:
{user_message}

ASSISTANT (append CTA if needed):
"""
    return prompt

# -----------------------------------------------------------------------------
# Chat endpoints
# -----------------------------------------------------------------------------
@app.post("/chat")
async def chat(req: ChatRequest):
    if not _allow_request("chat", 20, 60):
        raise HTTPException(status_code=429, detail="Too Many Requests")

    lang = req.lang or "de"
    intent_kw = _match_intent(req.message)
    sem_score = _semantic_score(req.message)
    intent_ok = bool(intent_kw or (sem_score >= 0.62))
    log_intent_analytics(req.message, intent_kw, sem_score, "chat")

    # Build context from RAG / scraper if available
    context_text = ""
    try:
        if query_index:
            try:
                ctx = query_index(req.message, k=4)  # type: ignore
                if isinstance(ctx, list):
                    context_text = "\n".join(ctx)
                elif ctx:
                    context_text = str(ctx)
            except Exception:
                pass
        if not context_text and get_scraped_context:
            try:
                sc = get_scraped_context(req.message)  # type: ignore
                if sc:
                    context_text = sc
            except Exception:
                pass
    except Exception:
        context_text = ""

    prompt = _build_prompt(req.message, context_text, lang, intent_ok)

    # If no OpenAI client, return deterministic fallback
    if _OPENAI is None:
        return {"reply": "Temporarily running in fallback mode. Please contact us: https://planville.de/kontakt"}

    try:
        resp = _OPENAI.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.4,
        )
        reply_text = (resp.choices[0].message.content or "").strip()
        if not reply_text:
            reply_text = "Entschuldigung, ich habe gerade keine passende Information. Kontakt: https://planville.de/kontakt"
        return {"reply": reply_text}
    except Exception:
        msg = "Es ist ein Fehler aufgetreten. Bitte versuchen Sie es spaeter erneut. Kontakt: https://planville.de/kontakt" \
              if lang == "de" else \
              "Something went wrong. Please try again later. Contact: https://planville.de/kontakt"
        return {"reply": msg}

@app.post("/chat/stream")
async def chat_stream(req: ChatRequest):
    if not _allow_request("chat_stream", 60, 60):
        raise HTTPException(status_code=429, detail="Too Many Requests")

    lang = req.lang or "de"
    intent_kw = _match_intent(req.message)
    sem_score = _semantic_score(req.message)
    intent_ok = bool(intent_kw or (sem_score >= 0.62))
    log_intent_analytics(req.message, intent_kw, sem_score, "chat_stream")

    # context
    context_text = ""
    try:
        if query_index:
            try:
                ctx = query_index(req.message, k=4)  # type: ignore
                context_text = "\n".join(ctx) if isinstance(ctx, list) else str(ctx or "")
            except Exception:
                pass
        if not context_text and get_scraped_context:
            try:
                sc = get_scraped_context(req.message)  # type: ignore
                context_text = sc or ""
            except Exception:
                pass
    except Exception:
        context_text = ""

    prompt = _build_prompt(req.message, context_text, lang, intent_ok)

    def token_stream():
        if _OPENAI is None:
            yield "Fallback mode. Please contact: https://planville.de/kontakt"
            return
        try:
            stream = _OPENAI.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3,
                stream=True,
            )
            for chunk in stream:
                try:
                    delta = chunk.choices[0].delta.get("content")
                except Exception:
                    delta = None
                if delta:
                    yield delta
        except Exception:
            msg = "Ups, da ist etwas schiefgelaufen. Bitte versuchen Sie es erneut. Kontakt: https://planville.de/kontakt" \
                  if lang == "de" else \
                  "Oops, something went wrong. Please try again. Contact: https://planville.de/kontakt"
            yield msg

    headers = {"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
    return StreamingResponse(token_stream(), media_type="text/plain; charset=utf-8", headers=headers)

@app.get("/chat/sse")
async def chat_sse(message: str = Query(...), lang: str = Query("de")):
    if not _allow_request("chat_sse", 60, 60):
        raise HTTPException(status_code=429, detail="Too Many Requests")

    intent_kw = _match_intent(message)
    sem_score = _semantic_score(message)
    intent_ok = bool(intent_kw or (sem_score >= 0.62))
    log_intent_analytics(message, intent_kw, sem_score, "chat_sse")

    context_text = ""
    try:
        if query_index:
            try:
                ctx = query_index(message, k=4)  # type: ignore
                context_text = "\n".join(ctx) if isinstance(ctx, list) else str(ctx or "")
            except Exception:
                pass
        if not context_text and get_scraped_context:
            try:
                sc = get_scraped_context(message)  # type: ignore
                context_text = sc or ""
            except Exception:
                pass
    except Exception:
        context_text = ""

    prompt = _build_prompt(message, context_text, lang or "de", intent_ok)

    def event_stream():
        if _OPENAI is None:
            yield "data: Fallback mode. Contact: https://planville.de/kontakt\n\n"
            yield "event: done\ndata: [DONE]\n\n"
            return
        try:
            stream = _OPENAI.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3,
                stream=True,
            )
            for chunk in stream:
                try:
                    delta = chunk.choices[0].delta.get("content")
                except Exception:
                    delta = None
                if delta:
                    yield "data: " + delta.replace("\n", "\\n") + "\n\n"
            yield "event: done\ndata: [DONE]\n\n"
        except Exception:
            msg = "Ups, da ist etwas schiefgelaufen. Bitte versuchen Sie es erneut. Kontakt: https://planville.de/kontakt" \
                  if (lang or "de") == "de" else \
                  "Oops, something went wrong. Please try again. Contact: https://planville.de/kontakt"
            yield "data: " + msg + "\n\n"

    headers = {"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
    return StreamingResponse(event_stream(), media_type="text/event-stream; charset=utf-8", headers=headers)

# -----------------------------------------------------------------------------
# Funnel qualification (/funnel/validate)
# -----------------------------------------------------------------------------
@app.post("/funnel/validate")
async def funnel_validate(data: Dict[str, Any]):
    product = (data or {}).get("product", "").lower()
    owner = bool((data or {}).get("owner", True))
    occupant = bool((data or {}).get("occupant", True))
    area = float((data or {}).get("area_sqm", 0) or 0)
    units = int((data or {}).get("units", 0) or 0)
    living_area = float((data or {}).get("living_area", 0) or 0)

    qualified = True
    reason = None

    if product in ("pv", "photovoltaik", "dach", "roof", "waermepumpe", "wärmepumpe", "heatpump"):
        if not owner:
            qualified = False
            reason = "kein_eigentumer"
        if product != "mieterstrom" and not occupant:
            qualified = False
            reason = reason or "nicht_bewohnt"

    if product in ("pv", "photovoltaik") and area and area < 10:
        qualified = False
        reason = "dachflaeche_zu_klein"

    if product in ("waermepumpe", "wärmepumpe", "heatpump") and living_area and living_area < 30:
        qualified = False
        reason = "wohnflaeche_zu_klein"

    if product in ("mieterstrom", "gewerbe", "tenant") and units and units < 3:
        qualified = False
        reason = "einheiten_zu_wenig"

    return {"qualified": qualified, "reason": reason}

# -----------------------------------------------------------------------------
# Lead schema + CRM push with queue
# -----------------------------------------------------------------------------
class Contact(BaseModel):
    firstName: str = ""
    lastName: Optional[str] = None
    email: EmailStr
    phone: Optional[str] = None

class Address(BaseModel):
    street: Optional[str] = None
    zip: Optional[str] = None
    city: Optional[str] = None

class Property(BaseModel):
    type: Optional[str] = None
    subType: Optional[str] = None
    area_sqm: Optional[float] = None
    roofType: Optional[str] = None
    roofOrientation: Optional[str] = None
    material: Optional[str] = None

class Energy(BaseModel):
    consumption_kwh: Optional[float] = None
    heatingType: Optional[str] = None
    heatDemand: Optional[float] = None

class Consent(BaseModel):
    accepted: bool = True
    timestamp: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    version: str = "v1.0"

class LeadPayload(BaseModel):
    leadSource: str = "chatbot"
    product: str
    qualified: bool = True
    disqualifyReason: Optional[str] = None
    contact: Contact
    address: Optional[Address] = None
    property: Optional[Property] = None
    energy: Optional[Energy] = None
    timeline: Optional[str] = None
    addons: Optional[List[str]] = None
    consent: Consent = Field(default_factory=Consent)
    utm: Optional[Dict[str, Any]] = None
    leadScore: Optional[int] = 0
    chatSummary: Optional[str] = ""

def calc_leadscore(payload: LeadPayload) -> int:
    score = 0
    try:
        if payload.timeline in ("0-3", "0–3", "0_3"):
            score += 30
        if payload.product.lower() in ("pv", "photovoltaik"):
            if payload.addons and any((a or "").lower() == "speicher" for a in payload.addons):
                score += 20
        if payload.energy and payload.energy.consumption_kwh and payload.energy.consumption_kwh >= 3500:
            score += 15
        if payload.property and payload.property.area_sqm and payload.property.area_sqm >= 60:
            score += 10
    except Exception:
        pass
    return score

def persist_jsonl(path: str, obj: Dict[str, Any]) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")

def push_to_crm(obj: Dict[str, Any]) -> (bool, str):
    if not CRM_URL:
        return False, "CRM_URL not set"
    headers = {"Content-Type": "application/json"}
    if CRM_TOKEN:
        headers["Authorization"] = f"Bearer {CRM_TOKEN}"
    try:
        r = requests.post(CRM_URL, json=obj, headers=headers, timeout=15)
        r.raise_for_status()
        return True, r.text
    except Exception as e:
        return False, str(e)

@app.post("/lead")
async def create_lead(lead: LeadPayload):
    if not lead.leadScore or lead.leadScore == 0:
        lead.leadScore = calc_leadscore(lead)

    persist_jsonl(LEADS_PATH, lead.dict())

    sent, detail = push_to_crm(lead.dict())
    queued = False
    if not sent:
        persist_jsonl(QUEUE_PATH, {"ts": datetime.now(timezone.utc).isoformat(), "payload": lead.dict(), "detail": detail})
        queued = True

    return {"ok": True, "leadScore": lead.leadScore, "sent": sent, "queued": queued, "detail": detail}

@app.post("/queue/flush")
def queue_flush():
    if not CRM_URL:
        raise HTTPException(status_code=400, detail="CRM_URL not set")
    if not os.path.exists(QUEUE_PATH):
        return {"flushed": 0}

    flushed = 0
    with open(QUEUE_PATH, "r", encoding="utf-8") as f:
        lines = f.read().splitlines()
    open(QUEUE_PATH, "w", encoding="utf-8").close()

    for line in lines:
        try:
            payload = json.loads(line).get("payload") if line.strip().startswith("{") else None
            if not payload:
                continue
            ok, _ = push_to_crm(payload)
            if ok:
                flushed += 1
            else:
                persist_jsonl(QUEUE_PATH, {"ts": datetime.now(timezone.utc).isoformat(), "payload": payload})
        except Exception:
            pass
    return {"flushed": flushed}

# -----------------------------------------------------------------------------
# Tracking & health
# -----------------------------------------------------------------------------
class TrackEvent(BaseModel):
    event: str
    props: Optional[Dict[str, Any]] = None
    ts: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

@app.post("/track")
async def track(ev: TrackEvent):
    persist_jsonl(EVENTS_PATH, ev.dict())
    return {"ok": True}

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.get("/version")
async def version():
    return {"version": APP_VERSION, "commit": COMMIT_SHA, "build_time": BUILD_TIME_ISO}
