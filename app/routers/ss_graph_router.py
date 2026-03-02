from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional
from app.controllers import firestore_graph_controller

router = APIRouter(
    tags=["SS Graph (Firestore)"]
)

class GraphLimitRequest(BaseModel):
    user_limit: Optional[int] = 100  
    post_limit: Optional[int] = 500  

@router.post("/snagraph")
async def create_ss_graph_endpoint(req: GraphLimitRequest):
    """
    Endpoint untuk membuat graf SNA terhubung dengan batas user dan post.
    """
    return await firestore_graph_controller.create_graph_from_firestore(
        user_limit=req.user_limit,
        post_limit=req.post_limit
    )

@router.get("/ssgraph/pajek")
async def create_ss_graph_pajek_endpoint():
    """
    Endpoint untuk membuat graf dari koleksi 'users' dan 'kawanss' di Firestore
    dalam format Pajek (.net).
    """
    return await firestore_graph_controller.create_graph_from_firestore_pajek()