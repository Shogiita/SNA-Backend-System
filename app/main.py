from fastapi import FastAPI
from app.routers import (
    csv_graph_router, 
    user_router, 
    post_router, 
    graph_router, 
    ss_graph_router, 
    ml_router,
    auth_router,
    instagram_router, # Pastikan ini ada (dari perbaikan sebelumnya)
    sna_router        # <--- TAMBAHKAN INI (1)
)

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
app.include_router(auth_router.router)
app.include_router(instagram_router.router) # Pastikan ini ada
app.include_router(sna_router.router)       # <--- TAMBAHKAN INI (2)

@app.get("/")
def read_root():
    return {"message": "Selamat datang di API yang sudah direstrukturisasi!"}