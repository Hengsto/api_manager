# main_api.py
from fastapi import FastAPI
from .notifier_api import router as notifier_router


app = FastAPI()
app.include_router(notifier_router, prefix="/notifier")
