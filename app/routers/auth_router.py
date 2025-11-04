from fastapi import APIRouter
from app.controllers import auth_controller

router = APIRouter(
    prefix="/auth",
    tags=["Authentication"]
)

@router.get("/refresh-token")
async def refresh_token_endpoint():
    """
    Endpoint untuk memicu refresh Instagram Long-Lived Access Token.
    """
    return await auth_controller.refresh_instagram_token()