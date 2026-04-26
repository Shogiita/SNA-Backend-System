import os
from contextlib import asynccontextmanager

os.environ["HTTP_PROXY"] = ""
os.environ["HTTPS_PROXY"] = ""
os.environ["NO_PROXY"] = "*"

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.background import BackgroundScheduler
from app.controllers.sna_controller import sync_instagram_to_neo4j
from app.controllers.report_controller import get_live_analytics_summary

from app import config

print("check koneksi neo4j:", config.NEO4J_URI)

from app.routers import (
    auth_router,
    instagram_router,
    sna_router,
    report_router,
    neo4j_router,
    integration_router
)

scheduler = BackgroundScheduler()
@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler.add_job(sync_instagram_to_neo4j, 'interval', hours=1, args=[False])
    scheduler.start()

    scheduler.add_job(get_live_analytics_summary, 'interval', minutes=2)

    print("🚀 APScheduler berjalan: Auto-update IG-Neo4j setiap 1 jam.")
    print("🚀 APScheduler berjalan: Auto-hit GA4 Live Analytics setiap 2 menit.")
    
    yield 
    scheduler.shutdown()
    print("🛑 APScheduler dihentikan.")


app = FastAPI(
    title="SNA Backend System API",
    description="API terstruktur dengan Router dan Controller.",
    version="0.5.0",
    lifespan=lifespan  # Mendaftarkan fungsi lifespan ke aplikasi
)

app.include_router(auth_router.router)
app.include_router(instagram_router.router)
app.include_router(sna_router.router)
app.include_router(report_router.router)
app.include_router(neo4j_router.router) 
app.include_router(integration_router.router)

@app.get("/")
def read_root():
    return {"message": "SNA Backend System is running with CORS Enabled for Flutter!"}

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_credentials=True,
    allow_methods=["*"],  
    allow_headers=["*"],  
    expose_headers=["X-Process-Time"], 
)