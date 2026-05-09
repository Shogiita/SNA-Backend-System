from typing import Dict, Any

from fastapi import APIRouter, Query

from app.controllers import instagram_controller

router = APIRouter(
    prefix="/instagram",
    tags=["Instagram"],
)


@router.get("/profile", response_model=Dict[str, Any])
async def get_profile_endpoint():
    return await instagram_controller.get_user_profile()


@router.get("/media", response_model=Dict[str, Any])
async def get_media_endpoint(
    limit: int = Query(
        default=10,
        ge=1,
        le=100,
        description="Jumlah postingan yang ingin diambil. Minimal 1 dan maksimal 100."
    )
):
    return await instagram_controller.get_user_media(limit=limit)


@router.get("/media/{media_id}/comments", response_model=Dict[str, Any])
async def get_media_comments_endpoint(
    media_id: str,
    limit: int = Query(
        default=50,
        ge=1,
        le=100,
        description="Jumlah komentar yang ingin diambil. Minimal 1 dan maksimal 100."
    )
):
    return await instagram_controller.get_media_comments(
        media_id=media_id,
        limit=limit,
    )


# @router.get("/debug-token", response_model=Dict[str, Any])
# async def debug_token_endpoint():
#     return await instagram_controller.debug_token()