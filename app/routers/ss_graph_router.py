from fastapi import APIRouter
from app.controllers import firestore_graph_controller

router = APIRouter(
    tags=["SS Graph (Firestore)"]
)

@router.get("/ssgraph")
async def create_ss_graph_endpoint():
    """
    Endpoint untuk membuat graf dari koleksi 'users' dan 'kawanss' di Firestore.
    """
    return await firestore_graph_controller.create_graph_from_firestore()

@router.get("/ssgraph/pajek")
async def create_ss_graph_pajek_endpoint():
    """
    Endpoint untuk membuat graf dari koleksi 'users' dan 'kawanss' di Firestore
    dalam format Pajek (.net).
    """
    return await firestore_graph_controller.create_graph_from_firestore_pajek()