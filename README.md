# 🚀 Planville Chatbot Backend (FastAPI + RAG)

This is the backend API server for the Planville Chatbot using FastAPI + Sentence Transformers + FAISS.

## 🔧 Setup

```bash
python -m venv venv
source venv/bin/activate  # or venv\Scripts\activate on Windows
pip install -r requirements.txt
cp .env.example .env
```

## ▶️ Run the server

```bash
uvicorn main:app --reload
```

## 📁 Structure
- `main.py` → FastAPI server
- `rag_engine.py` → FAISS vector search logic
- `scraper.py` → Real-time fallback scraper
- `auto_rebuild_index.py` → Auto-scrape & reindex tool
- `data/` → Index files and scraped context