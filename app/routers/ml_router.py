from fastapi import APIRouter
from app.controllers import ml_controller

router = APIRouter(
    prefix="/ml",
    tags=["Machine Learning"]
)

@router.get("/train")
async def train_model_endpoint():
    return await ml_controller.train_model_from_csv()