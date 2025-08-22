@echo off
echo [🧠] Menjalankan Planville Chatbot Backend...

REM Aktifkan virtual environment
call venv\Scripts\activate

REM Install dependencies
pip install -r requirements.txt

REM Jalankan server dengan uvicorn
echo [🚀] Menjalankan FastAPI server di http://localhost:8000 ...
uvicorn main:app --reload

pause