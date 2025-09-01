FROM python:3.13.7-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    APP_PORT=8000

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl build-essential && \
    rm -rf /var/lib/apt/lists/*

RUN echo "fastapi==0.115.0" > requirements.txt && \
    echo "uvicorn[standard]==0.30.6" >> requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

# Dev: mount your code; but bootstrap a tiny app if none present yet
RUN mkdir -p payments && \
    [ -f payments/main.py ] || cat > payments/main.py <<'PY'
from fastapi import FastAPI
app = FastAPI()
@app.get("/healthz")
def healthz(): return {"status": "ok"}
PY

EXPOSE ${APP_PORT}
CMD ["bash","-lc","uvicorn payments.main:app --host 0.0.0.0 --port ${APP_PORT} --reload --no-access-log"]
