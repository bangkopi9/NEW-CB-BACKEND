@echo off
title Planville Chatbot - Lokaler Start
echo 🚀 Starte Planville Backend...

:: In das aktuelle Verzeichnis wechseln
cd /d %~dp0

:: Virtuelle Umgebung aktivieren
call venv\Scripts\activate

:: FastAPI-Server mit Uvicorn starten
uvicorn main:app --reload

:: Terminal offen halten nach Absturz
pause
