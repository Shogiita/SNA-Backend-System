from fastapi import APIRouter
from app.controllers import sna_controller
from typing import Literal

router = APIRouter(
    prefix="/sna",
    tags=["SNA Instagram (Advanced)"]
)

@router.get("/ingest")
def run_ingestion_endpoint():
    return sna_controller.run_ingestion_process()

@router.get("/visualize/{metric}")
def visualize_graph_endpoint(metric: Literal['degree', 'betweenness', 'closeness', 'eigenvector']):
    return sna_controller.generate_sna_html(metric_type=metric)