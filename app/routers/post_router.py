from fastapi import APIRouter
from app.controllers import post_controller

router = APIRouter(
    prefix="/posts",
    tags=["Posts"]
)

@router.get("/all")
async def get_all_posts_endpoint():
    return await post_controller.get_all_posts_from_db()