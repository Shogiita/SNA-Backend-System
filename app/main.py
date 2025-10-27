from fastapi import FastAPI
from app.routers import user_router, post_router

app = FastAPI(
    title="SNA Backend System API",
    description="API terstruktur dengan Router dan Controller.",
    version="0.4.0", 
)

app.include_router(user_router.router)
app.include_router(post_router.router) 

@app.get("/")
def read_root():
    return {"message": "Selamat datang di API yang sudah direstrukturisasi!"}