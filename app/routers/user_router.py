from fastapi import APIRouter
from app.controllers import user_controller

router = APIRouter(
    prefix="/users",
    tags=["Users"]
)

@router.post("/")
async def create_user_endpoint(user_data: dict):
    return await user_controller.create_new_user(user_data)

@router.get("/all")
async def get_all_users_endpoint():
    return await user_controller.get_all_users_from_db()

@router.get("/test")
async def test_endpoint():
    return await user_controller.get_test_message()

@router.get("/{user_id}")
async def get_user_endpoint(user_id: str):
    return await user_controller.get_user_by_id(user_id)