# main_api.py
from fastapi import FastAPI
from api.notifier_api import router as notifier_router

# Bestehender Kommentar bleibt: (keine Änderung)
app = FastAPI()
app.include_router(notifier_router, prefix="/notifier")
