# Dockerfile
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Minimal runtime packages (curl optional für healthchecks)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
  && rm -rf /var/lib/apt/lists/*

# Dependencies zuerst für besseres Layer-Caching
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# App-Code
COPY . /app

# Defaults (können via compose überschrieben werden)
ENV MAIN_IP=0.0.0.0 \
    PORT=8098

# Sauberer als "python main.py": direkt uvicorn starten
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8098", "--log-level", "info"]
