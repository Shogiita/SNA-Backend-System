from fastapi import FastAPI
from app.routers import csv_graph_router, user_router, post_router, graph_router, ss_graph_router, ml_router

app = FastAPI(
    title="SNA Backend System API",
    description="API terstruktur dengan Router dan Controller.",
    version="0.4.0", 
)

app.include_router(user_router.router)
app.include_router(post_router.router) 
app.include_router(graph_router.router)
app.include_router(ss_graph_router.router)
app.include_router(csv_graph_router.router)
app.include_router(ml_router.router)

@app.get("/")
def read_root():
    return {"message": "Selamat datang di API yang sudah direstrukturisasi!"}