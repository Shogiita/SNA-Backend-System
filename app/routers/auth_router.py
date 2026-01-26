from fastapi import APIRouter
from app.controllers import auth_controller

router = APIRouter(
    prefix="/auth",
    tags=["Authentication"]
)

@router.get("/refresh-token")
async def refresh_token_endpoint():
    return await auth_controller.refresh_instagram_token()