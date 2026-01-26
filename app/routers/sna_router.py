from fastapi import APIRouter
from app.controllers import sna_controller
from typing import Literal
from fastapi import APIRouter, HTTPException
from app.controllers.sna_graph_controller import SnaGraphController

router = APIRouter()
controller = SnaGraphController()

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

@router.get("/visualize/{metric}")
def visualize_graph_endpoint(metric: Literal['degree', 'betweenness', 'closeness', 'eigenvector']):
    """
    Membuat file HTML visualisasi SNA berdasarkan data yang sudah di-ingest.
    """
    return sna_controller.generate_sna_html(metric_type=metric)

@router.post("/graph/interaction/comment")
async def record_comment(username: str, post_id: str, sentiment: str = "neutral"):
    success = controller.add_comment_interaction(username, post_id, sentiment)
    if not success:
        raise HTTPException(status_code=500, detail="Gagal menyimpan ke Graph")
    return {"message": "Interaksi berhasil direkam di Neo4j"}

@router.get("/graph/analysis/top-users")
async def get_top_users():
    data = controller.get_top_active_users()
    return {"data": data}