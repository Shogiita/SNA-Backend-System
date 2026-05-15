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

# @router.get("/debug-token", response_model=Dict[str, Any])
# async def debug_token_endpoint():
#     return await instagram_controller.debug_token()