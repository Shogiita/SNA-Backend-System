from fastapi import APIRouter
from app.controllers import graph_controller

router = APIRouter(
    prefix="/graph",
    tags=["Graph"]
)

@router.get("/generate")
async def generate_graph_endpoint():
    """
    Endpoint mandiri untuk membuat graf, menghitung betweenness centrality,
    dan mendeteksi komunitas dengan algoritma Leiden.
    """
    return await graph_controller.create_social_graph()