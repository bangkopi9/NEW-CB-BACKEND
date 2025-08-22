# -*- coding: utf-8 -*-
"""
Planville Chatbot Backend (FastAPI) — Hybrid GPT + optional RAG
- /chat (non-stream) + /chat/stream + /chat/sse
- Intent/keyword gate + simple rate limit
- Funnel validate: /funnel/validate
- Lead sink + CRM queue: /lead, /queue/flush
- Event tracking: /track
- Health/version: /health, /healthz, /version
- Simple config sink for FE form: /save-config
"""

import os, json, time, logging, requests
from typing import Dict, List, Optional, Deque, Any
from collections import defaultdict, deque
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, Request, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel, Field, EmailStr

# -----------------------------------------------------------------------------
# Optional RAG/scraper/OpenAI (safe import)
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
# Build metadata
# -----------------------------------------------------------------------------
APP_VERSION = os.getenv("APP_VERSION", "dev")
COMMIT_SHA = os.getenv("COMMIT_SHA", "")
BUILD_TIME_ISO = os.getenv("BUILD_TIME", datetime.now(timezone.utc).isoformat())

# -----------------------------------------------------------------------------
# Paths / storage
# -----------------------------------------------------------------------------
DATA_DIR = os.path.join(os.getcwd(), "data")
os.makedirs(DATA_DIR, exist_ok=True)
LEADS_PATH  = os.path.join(DATA_DIR, "leads.jsonl")
QUEUE_PATH  = os.path.join(DATA_DIR, "queue.jsonl")
EVENTS_PATH = os.path.join(DATA_DIR, "events.jsonl")
CONFIGS_PATH = os.path.join(DATA_DIR, "configs.jsonl")

CRM_URL   = (os.getenv("CRM_URL") or "").strip()
CRM_TOKEN = (os.getenv("CRM_TOKEN") or "").strip()

CONTACT_LINK = "https://planville.de/kontakt"

# -----------------------------------------------------------------------------
# App + CORS
# -----------------------------------------------------------------------------
app = FastAPI(title="Planville Chatbot Backend", version=APP_VERSION)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],           # batasi di prod ke domain FE kamu
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------------------------------------------------------
# Logging, intent, rate-limit
# -----------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(message)s")
intent_logger = logging.getLogger("intent")

VALID_KEYWORDS = [
    # German
    "photovoltaik","pv","solaranlage","dach","wärmepumpe","klimaanlage",
    "angebot","kosten","preise","förderung","termin","beratung",
    "installation","montage","wartung","service","garantie",
    # English
    "photovoltaics","solar","roof","heat pump","air conditioner","ac",
    "quote","cost","price","subsidy","appointment","consultation",
    "install","maintenance","warranty"
]

def _match_intent(text: str) -> bool:
    if not text: return False
    t = text.lower()
    return any(k in t for k in VALID_KEYWORDS)

REQUEST_BUCKETS: Dict[str, Deque[float]] = defaultdict(deque)
def _allow_request(bucket: str, limit: int, window_sec: int) -> bool:
    now = time.time()
    q = REQUEST_BUCKETS[bucket]
    while q and (now - q[0]) > window_sec:
        q.popleft()
    if len(q) >= limit: return False
    q.append(now)
    return True

INTENT_LOG_PATH = os.getenv("INTENT_LOG_PATH")
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

def _semantic_score(text: str) -> float:
    if not text: return 0.0
    t = text.lower()
    hits = sum(1 for k in ("pv","dach","roof","wärmepumpe","heat pump","planville") if k in t)
    return min(1.0, 0.2 * hits)

def persist_jsonl(path: str, obj: Dict[str, Any]) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")

# -----------------------------------------------------------------------------
# Schemas
# -----------------------------------------------------------------------------
class ChatMessage(BaseModel):
    role: str
    content: str

class ChatRequest(BaseModel):
    # support keduanya: single message atau riwayat
    message: Optional[str] = None
    messages: Optional[List[ChatMessage]] = None
    lang: str = "de"
    mode: str = "hybrid"  # "hybrid" | "rag_only" | "free"

# Lead & lainnya (tetap)
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

class TrackEvent(BaseModel):
    event: str
    props: Optional[Dict[str, Any]] = None
    ts: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

# -----------------------------------------------------------------------------
# Prompt builders
# -----------------------------------------------------------------------------
def _sys_prompt(lang: str, mode: str, context_text: str) -> str:
    de = (lang or "de") == "de"
    base = (
        "Du bist der Planville Chat-Assistent.\n"
        "- Antworte kurz, präzise, freundlich.\n"
        "- Themen: Photovoltaik, Dachsanierung, Wärmepumpe, Klimaanlage.\n"
        "- Keine verbindlichen Preise/Fristen; formuliere vorsichtig (\"typischerweise/abhängig von…\").\n"
        f"- Kontaktmöglichkeit: {CONTACT_LINK}\n"
    ) if de else (
        "You are the Planville chat assistant.\n"
        "- Reply briefly, precisely, and helpfully.\n"
        "- Topics: photovoltaics, roofing, heat pumps, air conditioning.\n"
        "- No binding prices/deadlines; use cautious phrasing (\"typically/depends on …\").\n"
        f"- Contact: {CONTACT_LINK}\n"
    )
    ctx = f"\nKONTEXT:\n{context_text}\n" if context_text else ""
    policy = {
        "hybrid": ("- Nutze den KONTEXT, wenn vorhanden. Fehlt Kontext: antworte kurz mit Branchenwissen "
                   "und biete Kontakt an.\n"),
        "rag_only": ("- Antworte NUR basierend auf KONTEXT. Wenn KONTEXT unzureichend ist: antworte sehr kurz, "
                     "biete Kontakt an und mache keine Spekulationen.\n"),
        "free": ("- Du darfst allgemeines Wissen nutzen, aber bleibe konservativ und verweise auf Kontakt.\n")
    }
    return base + policy.get(mode, policy["hybrid"]) + ctx

def _build_context(question: str) -> str:
    # RAG / scraper context (opsional)
    context_text = ""
    try:
        if query_index:
            try:
                ctx = query_index(question, k=4)  # type: ignore
                if isinstance(ctx, list):
                    context_text = "\n".join([str(x) for x in ctx])
                elif ctx:
                    context_text = str(ctx)
            except Exception:
                pass
        if not context_text and get_scraped_context:
            try:
                sc = get_scraped_context(question)  # type: ignore
                if sc:
                    context_text = str(sc)
            except Exception:
                pass
    except Exception:
        context_text = ""
    return context_text

# -----------------------------------------------------------------------------
# CHAT (non-stream)
# -----------------------------------------------------------------------------
@app.post("/chat")
async def chat(req: ChatRequest):
    if not _allow_request("chat", 20, 60):
        raise HTTPException(status_code=429, detail="Too Many Requests")

    lang = req.lang or "de"
    mode = (req.mode or "hybrid").lower()

    # Ambil teks user terakhir (untuk intent/RAG)
    if req.messages and len(req.messages) > 0:
        user_text = next((m.content for m in reversed(req.messages) if m.role == "user"), req.messages[-1].content)
    else:
        user_text = req.message or ""

    intent_kw  = _match_intent(user_text)
    sem_score  = _semantic_score(user_text)
    intent_ok  = bool(intent_kw or (sem_score >= 0.62))
    log_intent_analytics(user_text, intent_kw, sem_score, "chat")

    # Siapkan konteks
    context_text = _build_context(user_text)

    # Gate untuk rag_only: tanpa konteks -> fallback
    if mode == "rag_only" and not context_text:
        msg = (f'Ich bin mir nicht sicher. Bitte 📞 <a href="{CONTACT_LINK}" target="_blank">kontaktieren Sie unser Team hier</a>.'
              if (lang or "de")=="de" else
               f'I’m not sure. Please 📞 <a href="{CONTACT_LINK}" target="_blank">contact our team here</a>.')
        return JSONResponse({"reply": msg, "fallback": True})

    if _OPENAI is None:
        # Fallback deterministik
        msg = (f'Temporär im Fallback-Modus. 📞 <a href="{CONTACT_LINK}" target="_blank">Kontakt</a>.'
               if (lang or "de")=="de" else
               f'Temporarily in fallback mode. 📞 <a href="{CONTACT_LINK}" target="_blank">Contact</a>.')
        return JSONResponse({"reply": msg, "fallback": True})

    # Compose messages untuk OpenAI
    sys_msg = {"role": "system", "content": _sys_prompt(lang, mode, context_text)}
    if req.messages and len(req.messages) > 0:
        oa_messages = [sys_msg] + [m.dict() for m in req.messages]
    else:
        oa_messages = [sys_msg, {"role": "user", "content": user_text}]

    try:
        resp = _OPENAI.chat.completions.create(
            model="gpt-4o-mini",
            messages=oa_messages,
            temperature=0.2 if mode != "free" else 0.4,
        )
        reply_text = (resp.choices[0].message.content or "").strip()
        if not reply_text:
            reply_text = (f'Entschuldigung, mir fehlt hierzu gesicherte Information. '
                          f'📞 <a href="{CONTACT_LINK}" target="_blank">Kontakt</a>.'
                          if (lang or "de")=="de" else
                          f'Sorry, I don’t have verified info on that. '
                          f'📞 <a href="{CONTACT_LINK}" target="_blank">Contact</a>.')
        # Pastikan ada CTA (jika belum disebutkan)
        if "planville.de/kontakt" not in reply_text:
            reply_text += ("\n\n📞 <a href=\"%s\" target=\"_blank\">Kontaktieren Sie unser Team</a>."
                           if (lang or "de")=="de"
                           else "\n\n📞 <a href=\"%s\" target=\"_blank\">Contact our team</a>.") % CONTACT_LINK
        return JSONResponse({"reply": reply_text})
    except Exception as e:
        msg = ("Es ist ein Fehler aufgetreten. Bitte später erneut versuchen. "
               f'📞 <a href="{CONTACT_LINK}" target="_blank">Kontakt</a>.'
               if (lang or "de")=="de" else
               "Something went wrong. Please try again later. "
               f'📞 <a href="{CONTACT_LINK}" target="_blank">Contact</a>.')
        logging.error(f"CHAT_ERR: {e}")
        return JSONResponse({"reply": msg}, status_code=200)

# Alias untuk kompatibilitas
app.add_api_route("/api/chat", chat, methods=["POST"])

# -----------------------------------------------------------------------------
# STREAM & SSE (tetap single-message; bisa kamu perluas jika perlu)
# -----------------------------------------------------------------------------
@app.post("/chat/stream")
async def chat_stream(req: ChatRequest):
    if not _allow_request("chat_stream", 60, 60):
        raise HTTPException(status_code=429, detail="Too Many Requests")

    lang = req.lang or "de"
    user_text = (req.message or
                 (next((m.content for m in reversed(req.messages or []) if m.role=="user"), "") if req.messages else ""))
    context_text = _build_context(user_text)
    sys = _sys_prompt(lang, req.mode or "hybrid", context_text)
    def token_stream():
        if _OPENAI is None:
            yield f"Fallback mode. Contact: {CONTACT_LINK}"
            return
        try:
            stream = _OPENAI.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role":"system","content":sys},{"role":"user","content":user_text}],
                temperature=0.3,
                stream=True,
            )
            for chunk in stream:
                try:
                    delta = chunk.choices[0].delta.get("content")
                except Exception:
                    delta = None
                if delta: yield delta
        except Exception as e:
            logging.error(f"STREAM_ERR: {e}")
            yield ("Ups, da ist etwas schiefgelaufen. Bitte erneut versuchen. "
                   f"Kontakt: {CONTACT_LINK}" if (lang or "de")=="de" else
                   f"Oops, something went wrong. Contact: {CONTACT_LINK}")
    headers = {"Cache-Control":"no-cache","X-Accel-Buffering":"no"}
    return StreamingResponse(token_stream(), media_type="text/plain; charset=utf-8", headers=headers)

@app.get("/chat/sse")
async def chat_sse(message: str = Query(...), lang: str = Query("de")):
    if not _allow_request("chat_sse", 60, 60):
        raise HTTPException(status_code=429, detail="Too Many Requests")
    context_text = _build_context(message)
    sys = _sys_prompt(lang, "hybrid", context_text)
    def event_stream():
        if _OPENAI is None:
            yield "data: Fallback mode. Contact: %s\n\n" % CONTACT_LINK
            yield "event: done\ndata: [DONE]\n\n"
            return
        try:
            stream = _OPENAI.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role":"system","content":sys},{"role":"user","content":message}],
                temperature=0.3,
                stream=True,
            )
            for chunk in stream:
                try:
                    delta = chunk.choices[0].delta.get("content")
                except Exception:
                    delta = None
                if delta:
                    yield "data: " + delta.replace("\n","\\n") + "\n\n"
            yield "event: done\ndata: [DONE]\n\n"
        except Exception as e:
            logging.error(f"SSE_ERR: {e}")
            msg = ("Ups, da ist etwas schiefgelaufen. Bitte erneut versuchen."
                   if (lang or "de")=="de" else "Oops, something went wrong.")
            yield "data: " + msg + "\n\n"
    headers = {"Cache-Control":"no-cache","X-Accel-Buffering":"no"}
    return StreamingResponse(event_stream(), media_type="text/event-stream; charset=utf-8", headers=headers)

# -----------------------------------------------------------------------------
# Funnel qualification
# -----------------------------------------------------------------------------
@app.post("/funnel/validate")
async def funnel_validate(data: Dict[str, Any]):
    product = (data or {}).get("product","").lower()
    owner = bool((data or {}).get("owner", True))
    occupant = bool((data or {}).get("occupant", True))
    area = float((data or {}).get("area_sqm", 0) or 0)
    units = int((data or {}).get("units", 0) or 0)
    living_area = float((data or {}).get("living_area", 0) or 0)

    qualified = True
    reason = None

    if product in ("pv","photovoltaik","dach","roof","waermepumpe","wärmepumpe","heatpump"):
        if not owner: qualified = False; reason = "kein_eigentumer"
        if product != "mieterstrom" and not occupant:
            qualified = False; reason = reason or "nicht_bewohnt"

    if product in ("pv","photovoltaik") and area and area < 10:
        qualified = False; reason = "dachflaeche_zu_klein"

    if product in ("waermepumpe","wärmepumpe","heatpump") and living_area and living_area < 30:
        qualified = False; reason = "wohnflaeche_zu_klein"

    if product in ("mieterstrom","gewerbe","tenant") and units and units < 3:
        qualified = False; reason = "einheiten_zu_wenig"

    return {"qualified": qualified, "reason": reason}

# -----------------------------------------------------------------------------
# Lead + CRM queue
# -----------------------------------------------------------------------------
def calc_leadscore(payload: 'LeadPayload') -> int:
    score = 0
    try:
        if payload.timeline in ("0-3","0–3","0_3"): score += 30
        if payload.product.lower() in ("pv","photovoltaik"):
            if payload.addons and any((a or "").lower()=="speicher" for a in payload.addons):
                score += 20
        if payload.energy and payload.energy.consumption_kwh and payload.energy.consumption_kwh >= 3500:
            score += 15
        if payload.property and payload.property.area_sqm and payload.property.area_sqm >= 60:
            score += 10
    except Exception:
        pass
    return score

def push_to_crm(obj: Dict[str, Any]) -> (bool, str):
    if not CRM_URL:
        return False, "CRM_URL not set"
    headers = {"Content-Type":"application/json"}
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
        persist_jsonl(QUEUE_PATH, {"ts": datetime.now(timezone.utc).isoformat(),
                                   "payload": lead.dict(), "detail": detail})
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
            if not payload: continue
            ok, _ = push_to_crm(payload)
            if ok: flushed += 1
            else: persist_jsonl(QUEUE_PATH, {"ts": datetime.now(timezone.utc).isoformat(), "payload": payload})
        except Exception:
            pass
    return {"flushed": flushed}

# -----------------------------------------------------------------------------
# Tracking & misc
# -----------------------------------------------------------------------------
@app.post("/track")
async def track(ev: TrackEvent):
    persist_jsonl(EVENTS_PATH, ev.dict())
    return {"ok": True}

# Alias untuk kompatibilitas
app.add_api_route("/api/track", track, methods=["POST"])

@app.post("/save-config")
async def save_config(data: Dict[str, Any]):
    # Sink sederhana untuk form FE (non-CRM)
    doc = {"ts": datetime.now(timezone.utc).isoformat(), "payload": data}
    persist_jsonl(CONFIGS_PATH, doc)
    return {"ok": True}

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.get("/healthz")
async def healthz():
    return {"status": "ok"}

@app.get("/version")
async def version():
    return {"version": APP_VERSION, "commit": COMMIT_SHA, "build_time": BUILD_TIME_ISO}
