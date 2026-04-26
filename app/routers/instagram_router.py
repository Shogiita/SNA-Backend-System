from fastapi import APIRouter, Query
from typing import Dict, Any
from app.controllers import instagram_controller

router = APIRouter(
    prefix="/instagram",
    tags=["Instagram"]
)

#done

#dipake
@router.get("/profile", response_model=Dict[str, Any])
async def get_profile_endpoint():
    """Mengambil data profil Instagram Business/Creator."""
    return await instagram_controller.get_user_profile()

@router.get("/media", response_model=Dict[str, Any])
async def get_media_endpoint(
    limit: int = Query(
        default=10, 
        ge=1, 
        le=100, 
        description="Jumlah postingan yang ingin diambil (antara 1 dan 100). Default adalah 10."
    )
):
    """Mengambil daftar media/postingan dari Instagram."""
    return await instagram_controller.get_user_media(limit=limit)

@router.get("/debug-token", response_model=Dict[str, Any])
async def debug_token_endpoint():
    """Endpoint khusus untuk memverifikasi access token. JANGAN gunakan di production."""
    return await instagram_controller.debug_token()