from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.background import BackgroundScheduler
from app.controllers.sna_controller import sync_instagram_to_neo4j
from app.controllers.report_controller import get_live_analytics_summary

from app.routers import (
    csv_graph_router, 
    user_router, 
    post_router, 
    graph_router, 
    ss_graph_router, 
    ml_router,
    auth_router,
    instagram_router,
    sna_router,
    report_router,
    neo4j_router,
    integration_router
)

app = FastAPI(
    title="SNA Backend System API",
    description="API terstruktur dengan Router dan Controller.",
    version="0.5.0", 
)

scheduler = BackgroundScheduler()

app.include_router(csv_graph_router.router)
app.include_router(user_router.router)
app.include_router(post_router.router)
app.include_router(graph_router.router)
app.include_router(ss_graph_router.router)
app.include_router(ml_router.router)
app.include_router(auth_router.router)
app.include_router(instagram_router.router)
app.include_router(sna_router.router)
app.include_router(report_router.router)
app.include_router(neo4j_router.router) 
app.include_router(integration_router.router)

@app.get("/")
def read_root():
    return {"message": "SNA Backend System is running with CORS Enabled for Flutter!"}

@app.on_event("startup")
def startup_event():
    scheduler.add_job(sync_instagram_to_neo4j, 'interval', hours=1, args=[False])
    scheduler.start()

    scheduler.add_job(get_live_analytics_summary, 'interval', minutes=2)

    print("🚀 APScheduler berjalan: Auto-update IG-Neo4j setiap 1 jam.")
    print("🚀 APScheduler berjalan: Auto-hit GA4 Live Analytics setiap 2 menit.")

@app.on_event("shutdown")
def shutdown_event():
    scheduler.shutdown()
    print("🛑 APScheduler dihentikan.")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_credentials=True,
    allow_methods=["*"],  
    allow_headers=["*"],  
    expose_headers=["X-Process-Time"], 
)
