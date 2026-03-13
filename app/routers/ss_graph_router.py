from fastapi import APIRouter, Query
from pydantic import BaseModel
from typing import Optional
from app.controllers import neo4j_graph_controller

router = APIRouter(
    tags=["SS Graph (Neo4j)"]
)

class GraphLimitRequest(BaseModel):
    limit: Optional[int] = 1000
    mode: Optional[int] = 1 # Tambahan parameter: 1 = 1-Mode (User-User), 2 = 2-Mode (User-Post)

@router.post("/snagraph")
async def create_ss_graph_endpoint(req: GraphLimitRequest):
    """
    Menerima request POST dengan body JSON:
    {
      "limit": 1000,
      "mode": 1
    }
    """
    return await neo4j_graph_controller.create_graph_from_neo4j(limit=req.limit, mode=req.mode)

@router.get("/snagraph/visualize")
async def visualize_ss_graph_endpoint(
    limit: int = Query(1000), 
    mode: int = Query(1, description="1: User-User, 2: User-Post")
):
    """
    Endpoint Visualisasi HTML (Bisa dibuka langsung di Browser / WebView Flutter).
    Contoh: http://127.0.0.1:8000/snagraph/visualize?mode=1&limit=500
    """
    return await neo4j_graph_controller.visualize_graph_from_neo4j(limit=limit, mode=mode)