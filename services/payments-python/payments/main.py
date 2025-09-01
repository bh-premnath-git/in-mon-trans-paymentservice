from fastapi import FastAPI
from .adapters import payment_client

app = FastAPI()

@app.get("/healthz")
async def healthz():
    return {"status": "ok"}

@app.get("/payments")
async def payments():
    message = await payment_client.ping("ping")
    return {"message": message}
