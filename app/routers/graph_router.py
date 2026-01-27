from fastapi import APIRouter
from app.controllers import graph_controller

router = APIRouter(
    prefix="/graph",
    tags=["Graph"]
)

@router.get("/generate")
async def generate_graph_endpoint():
    return await graph_controller.create_social_graph()

@router.get("/generate/pajek")
async def generate_pajek_graph_endpoint():
    return await graph_controller.create_social_graph_pajek()
