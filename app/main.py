from fastapi import FastAPI
from app.routers import user_router 

app = FastAPI(
    title="SNA Backend System API",
    description="API terstruktur dengan Router dan Controller.",
    version="0.3.0",
)

app.include_router(user_router.router)

@app.get("/")
def read_root():
    return {"message": "Selamat datang di API yang sudah direstrukturisasi!"}