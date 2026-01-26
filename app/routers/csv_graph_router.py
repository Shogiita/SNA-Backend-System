from fastapi import APIRouter
from app.controllers import csv_graph_controller
from typing import Literal

router = APIRouter(
    prefix="/csvgraph",
    tags=["CSV Graph"]
)

@router.get("/generate")
async def generate_graph_from_csv_endpoint(format: Literal['json', 'pajek'] = 'json'):
    return await csv_graph_controller.create_graph_from_csv(output_format=format)