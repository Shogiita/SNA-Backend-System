from fastapi import APIRouter
from app.controllers import sna_controller
from typing import Literal

router = APIRouter(
    prefix="/sna",
    tags=["SNA Instagram (Advanced)"]
)

@router.get("/ingest")
def run_ingestion_endpoint():
    """
    Mengambil data dari Instagram (Post & Comments) dan menyimpannya ke Cache JSON.
    WARNING: Proses ini memakan waktu tergantung jumlah post.
    """
    return sna_controller.run_ingestion_process()

@router.get("/dataset")
def get_sna_dataset_endpoint():
    """
    Menghasilkan dataset lengkap (Flat List) berisi:
    ID Post, ID User, Caption, Gambar, ID Reacted (Target), Comment, ID Comment.
    Cocok untuk diexport ke CSV/Excel.
    """
    return sna_controller.get_dataset_flat()

@router.get("/firestore/analyze")
async def analyze_firestore_endpoint():
    """
    Melakukan Social Network Analysis (SNA) pada data real-time Firestore.
    Sumber Data: Koleksi 'user', 'kawanss', 'kawanssLikes', 'kawanssComments'.
    
    Output:
    - Metrik Centrality (Degree, Betweenness, Closeness, Eigenvector)
    - Deteksi Komunitas (Leiden Algorithm)
    - Struktur Graf (Nodes & Edges) untuk visualisasi Frontend
    """
    return await sna_controller.analyze_firestore_network()