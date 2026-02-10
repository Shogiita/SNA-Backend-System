from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
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
    report_router 
)

app = FastAPI(
    title="SNA Backend System API",
    description="API terstruktur dengan Router dan Controller.",
    version="0.5.0", 
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_credentials=True,
    allow_methods=["*"],  
    allow_headers=["*"],  
)

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

@app.get("/")
def read_root():
    return {"message": "SNA Backend System is running with CORS Enabled!"}