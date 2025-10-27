# app/routers/user_router.py

from fastapi import APIRouter
from app.controllers import user_controller 

router = APIRouter(
    prefix="/users", 
    tags=["Users"]
)

@router.post("/")
def create_user_endpoint(user_data: dict):
    return user_controller.create_new_user(user_data)

@router.get("/all")
def get_all_users_endpoint():
    return user_controller.get_all_users_from_db()

@router.get("/{user_id}")
def get_user_endpoint(user_id: str):
    return user_controller.get_user_by_id(user_id)

@router.get("/test")
def test_endpoint():
    return user_controller.get_test_message()