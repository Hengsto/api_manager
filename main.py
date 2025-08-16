# main_api.py
from fastapi import FastAPI
from api.notifier_api import router as notifier_router

# Bestehender Kommentar bleibt: (keine Änderung)
app = FastAPI(title="Notifier API", version="1.0.0")
app.include_router(notifier_router, prefix="/notifier")

# -- Debug/Health (optional, hilft beim Startcheck) --
@app.get("/notifier/health")
def health():
    return {"status": "ok"}

@app.on_event("startup")
def _log_routes():
    # viele Prints sind gewünscht – hier bewusst laut
    print("[DEBUG] Notifier API gestartet. Routen:")
    for r in app.router.routes:
        try:
            print(f"[DEBUG]  {getattr(r, 'methods', ['GET'])} {r.path}")
        except Exception:
            pass
