from fastapi import APIRouter, Body
from pydantic import BaseModel, Field
from app.controllers import instagram_controller

router = APIRouter(
    prefix="/instagram",
    tags=["Instagram"]
)

class MediaRequest(BaseModel):
    limit: int = Field(
        default=25, 
        ge=1, 
        le=10000, 
        description="Jumlah postingan yang ingin diambil (Maksimal 10000)"
    )

@router.get("/profile")
async def get_profile_endpoint():
    return await instagram_controller.get_user_profile()


@router.post("/media")
async def get_media_endpoint(request: MediaRequest):
    limit_value = request.limit
    
    return await instagram_controller.get_user_media(limit=limit_value)

@router.get("/debug-token")
async def debug_token_endpoint():
    return await instagram_controller.debug_token()