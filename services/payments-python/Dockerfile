FROM python:3.13.7-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    APP_PORT=8000

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl build-essential && \
    rm -rf /var/lib/apt/lists/*

COPY services/payments-python/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Generate protobuf files with correct paths
WORKDIR /app/services/payments-python
RUN python -m grpc_tools.protoc \
    -I /app/proto \
    --python_out=payments/pb \
    --grpc_python_out=payments/pb \
    /app/proto/payments.proto

EXPOSE ${APP_PORT}
CMD ["bash","-lc","uvicorn payments.main:app --host 0.0.0.0 --port ${APP_PORT} --reload --no-access-log"]
