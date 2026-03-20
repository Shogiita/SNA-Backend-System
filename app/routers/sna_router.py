from fastapi import APIRouter, Query, BackgroundTasks
from app.controllers import sna_controller

router = APIRouter(
    prefix="/sna",
    tags=["SNA (Advanced - Neo4j)"]
)

@router.get("/metrics")
def get_sna_dashboard_metrics(background_tasks: BackgroundTasks):
    return sna_controller.get_instagram_metrics(background_tasks)
    
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
    mode: int = Query(1, description="1 untuk 1-Mode (User-User), 2 untuk 2-Mode (User-Post)")
):
    return await sna_controller.analyze_neo4j_network(mode=mode)

@router.get("/neo4j/visualize")
async def visualize_neo4j_endpoint(
    mode: int = Query(1, description="1 untuk 1-Mode (User-User), 2 untuk 2-Mode (User-Post)")
):
    return await sna_controller.visualize_neo4j_network(mode=mode)