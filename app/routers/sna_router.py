from fastapi import APIRouter, Query
from app.controllers import sna_controller

router = APIRouter(
    prefix="/sna",
    tags=["SNA (Advanced - Neo4j)"]
)

@router.get("/ingest")
def run_ingestion_endpoint():
    return sna_controller.run_ingestion_process()

@router.get("/dataset")
def get_sna_dataset_endpoint():
    return sna_controller.get_dataset_flat()

@router.get("/neo4j/analyze")
async def analyze_neo4j_endpoint(
    mode: int = Query(1, description="1 untuk 1-Mode (User-User), 2 untuk 2-Mode (User-Post)")
):
    """ 
    Mengembalikan JSON data mentah jaringan sosial dari database Neo4j.
    Cocok untuk diproses lebih lanjut oleh Mobile App jika tidak ingin menggunakan WebView.
    """
    return await sna_controller.analyze_neo4j_network(mode=mode)

@router.get("/neo4j/visualize")
async def visualize_neo4j_endpoint(
    mode: int = Query(1, description="1 untuk 1-Mode (User-User), 2 untuk 2-Mode (User-Post)")
):
    """
    Mengembalikan tampilan HTML Interaktif (Pyvis):
    - Menggunakan data dari Neo4j.
    - Menampilkan Komunitas/Clustering (Warna-warni).
    - Menampilkan Weighted Graph (Garis tebal-tipis sesuai jumlah interaksi/bobot).
    """
    return await sna_controller.visualize_neo4j_network(mode=mode)