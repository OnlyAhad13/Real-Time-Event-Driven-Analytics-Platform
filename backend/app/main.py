from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.db.session import db
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

app = FastAPI(title="Analytics Dashboard API", version="1.0.0")

# CORS (Allow Frontend to connect)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup():
    await db.connect()

@app.on_event("shutdown")
async def shutdown():
    await db.disconnect()

@app.get("/")
async def root():
    return {"message": "Analytics Platform API is running. Go to /docs for Swagger UI."}

from app.api import kpi, live

app.include_router(kpi.router, prefix="/kpi", tags=["KPIs"])
app.include_router(live.router, prefix="/live", tags=["Real-Time"])
