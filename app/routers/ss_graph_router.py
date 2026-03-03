from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional
from app.controllers import neo4j_graph_controller

router = APIRouter(
    tags=["SS Graph (Neo4j)"]
)

class GraphLimitRequest(BaseModel):
    limit: Optional[int] = 1000

@router.post("/snagraph")
async def create_ss_graph_endpoint(req: GraphLimitRequest):
    return await neo4j_graph_controller.create_graph_from_neo4j(limit=req.limit)