#!/bin/bash
echo "[🧠] Menjalankan Planville Chatbot Backend..."

# Aktifkan virtual environment
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Jalankan server dengan uvicorn
echo "[🚀] Menjalankan FastAPI server di http://localhost:8000 ..."
uvicorn main:app --reload