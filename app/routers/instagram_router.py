from fastapi import APIRouter, Query
from app.controllers import instagram_controller

router = APIRouter(
    prefix="/instagram",
    tags=["Instagram"]
)

@router.get("/profile")
async def get_profile_endpoint():
    """
    Endpoint untuk mengambil data profil dasar dari akun Instagram
    yang terhubung (suarasurabayamedia).
    """
    return await instagram_controller.get_user_profile()

@router.get("/media")
async def get_media_endpoint(
    limit: int = Query(25, ge=1, le=100, description="Jumlah postingan yang ingin diambil (antara 1 dan 100)")
):
    """
    Endpoint untuk mengambil postingan terbaru dari akun Instagram
    yang terhubung (suarasurabayamedia).
    """
    return await instagram_controller.get_user_media(limit=limit)

@router.get("/debug-token")
async def debug_token_endpoint():
    """
    Endpoint debugging untuk mengecek token yang dimuat.
    HANYA UNTUK PENGEMBANGAN.
    """
    return await instagram_controller.debug_token()
