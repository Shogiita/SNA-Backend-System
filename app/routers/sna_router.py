from fastapi import APIRouter, Query, BackgroundTasks
from app.controllers import sna_controller

router = APIRouter(
    prefix="/sna",
    tags=["SNA (Advanced - Neo4j)"]
)

@router.get("/metrics")
def get_sna_dashboard_metrics(
    start_date: str = Query(None, description="Format tanggal start (e.g. 2024-01-01)"),
    end_date: str = Query(None, description="Format tanggal end (e.g. 2024-12-31)")
):
    return sna_controller.get_instagram_metrics(start_date=start_date, end_date=end_date)
    
@router.get("/ingest")
async def run_ingestion_endpoint(background_tasks: BackgroundTasks):
    """
    Endpoint ini akan MENGEMBALIKAN RESPONSE DALAM HITUNGAN DETIK.
    Proses penarikan data Instagram akan dikerjakan di background.
    """
    background_tasks.add_task(sna_controller.background_ingestion_task)
    
    return {
        "status": "success",
        "message": "Proses sinkronisasi data Instagram Suara Surabaya sedang berjalan di latar belakang. Silakan cek /sna/dataset beberapa menit lagi."
    }

@router.get("/dataset")
def get_sna_dataset_endpoint():
    return sna_controller.get_dataset_flat()

@router.get("/neo4j/analyze")
async def analyze_neo4j_endpoint(
    mode: int = Query(1, description="1 untuk 1-Mode (User-User), 2 untuk 2-Mode (User-Post)"),
    limit: int = Query(1000, description="Limit nodes agar browser tidak freeze")
):
    return await sna_controller.analyze_neo4j_network(mode=mode, limit=limit)

@router.get("/neo4j/visualize")
async def visualize_neo4j_endpoint(
    mode: int = Query(1, description="1 untuk 1-Mode (User-User), 2 untuk 2-Mode (User-Post)"),
    limit: int = Query(1000, description="Limit nodes agar browser tidak freeze")
):
    return await sna_controller.visualize_neo4j_network(mode=mode, limit=limit)

@router.post("/instagram/sync-neo4j")
async def manual_sync_ig_neo4j_endpoint(
    background_tasks: BackgroundTasks,
    initial_sync: bool = Query(False, description="Set True jika ingin menarik data 1 Tahun. False untuk update data baru saja.")
):
    """
    Endpoint manual untuk menarik data Instagram terbaru dan memasukannya ke Neo4j.
    """
    background_tasks.add_task(sna_controller.sync_instagram_to_neo4j, initial_sync)
    
    msg = "Proses penarikan 2 Bulan data" if initial_sync else "Proses update data terbaru"
    return {
        "status": "success",
        "message": f"{msg} Instagram ke Neo4j sedang berjalan di latar belakang."
    }