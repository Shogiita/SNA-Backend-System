from fastapi import APIRouter
from app.controllers import csv_graph_controller
from typing import Literal

router = APIRouter(
    prefix="/csvgraph",
    tags=["CSV Graph"]
)

@router.get("/generate")
async def generate_graph_from_csv_endpoint(format: Literal['json', 'pajek'] = 'json'):
    """
    Endpoint untuk membuat graf dari twitter_dataset.csv.
    
    - **format=json** (default): Mengembalikan data graf dalam format JSON.
    - **format=pajek**: Mengembalikan data graf dalam format teks Pajek (.net).
    """
    return await csv_graph_controller.create_graph_from_csv(output_format=format)