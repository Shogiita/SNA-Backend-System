from fastapi import APIRouter
from app.controllers import graph_controller

router = APIRouter(
    prefix="/graph",
    tags=["Graph"]
)

@router.get("/generate")
async def generate_graph_endpoint():
    """
    Endpoint mandiri untuk membuat graf (format JSON), menghitung betweenness centrality,
    dan mendeteksi komunitas dengan algoritma Leiden.
    """
    return await graph_controller.create_social_graph()

@router.get("/generate/pajek")
async def generate_pajek_graph_endpoint():
    """
    Endpoint untuk membuat graf dari data dummy dalam format teks Pajek (.net).
    """
    return await graph_controller.create_social_graph_pajek()
