# Dockerfile
# Minimaler, aber sauberer Python-Build für deinen Registry-Host

FROM python:3.12-slim

# Keine .pyc und ungepuffertes Logging
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Sicherstellen, dass /app existiert und Arbeitsverzeichnis setzen
WORKDIR /app

# Systempakete nur bei Bedarf (hier sehr schlank)
# Wenn du später z.B. libpq, gcc etc. brauchst, hier ergänzen.
RUN apt-get update && apt-get install -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# Dependencies installieren
# -> requirements.txt MUSS im Projektroot liegen
COPY requirements.txt /app/

RUN pip install --no-cache-dir -r requirements.txt

# Restlichen Code ins Image kopieren
COPY . /app

# Standard-ENV für Container:
# - MAIN_IP = 0.0.0.0, damit FastAPI im Container erreichbar ist
ENV MAIN_IP=0.0.0.0

# Optional: default Port im Container (kann per ENV überschrieben werden)
ENV REGISTRY_PORT=8098

# Debug-Ausgaben im Container sinnvoller
ENV PYTHONUNBUFFERED=1

# Startkommando:
# Wir starten dein main_registry.py, das intern uvicorn.run(...) macht.
CMD ["python", "main_registry.py"]
