import httpx
from fastapi import HTTPException, status
from typing import Dict, Any, Optional

from app import config


async def _check_config() -> None:
    if not config.IG_BUSINESS_ACCOUNT_ID:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="IG_BUSINESS_ACCOUNT_ID belum diatur di server."
        )

    if (
        not config.IG_ACCESS_TOKEN
        or config.IG_ACCESS_TOKEN == "MASUKKAN_ACCESS_TOKEN_ANDA_YANG_SUDAH_DIGENERATE"
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="IG_ACCESS_TOKEN belum di-generate atau belum diatur di server."
        )


async def _make_ig_api_request(
    endpoint: str,
    params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    await _check_config()

    if params is None:
        params = {}

    clean_endpoint = endpoint.lstrip("/")
    base_url = f"{config.GRAPH_API_URL}/{clean_endpoint}"
    all_params = {
        "access_token": config.IG_ACCESS_TOKEN,
        **params,
    }

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(base_url, params=all_params)
            response.raise_for_status()
            return response.json()

    except httpx.HTTPStatusError as error:
        try:
            error_data = error.response.json()
            error_message = error_data.get("error", {}).get("message", "Unknown error")
        except Exception:
            error_message = error.response.text

        raise HTTPException(
            status_code=error.response.status_code,
            detail=f"Error dari Instagram API: {error_message}"
        )

    except httpx.RequestError as error:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Gagal menghubungi Instagram API: {str(error)}"
        )

    except Exception as error:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Terjadi kesalahan internal: {str(error)}"
        )


async def get_user_profile() -> Dict[str, Any]:
    endpoint = f"{config.IG_BUSINESS_ACCOUNT_ID}"
    params = {
        "fields": (
            "id,username,name,biography,followers_count,"
            "follows_count,media_count,profile_picture_url"
        )
    }

    return await _make_ig_api_request(endpoint, params)


async def get_user_media(limit: int = 10) -> Dict[str, Any]:
    safe_limit = max(1, min(int(limit), 100))

    endpoint = f"{config.IG_BUSINESS_ACCOUNT_ID}/media"
    params = {
        "limit": safe_limit,
        "fields": (
            "id,caption,media_type,media_product_type,media_url,"
            "permalink,thumbnail_url,timestamp,like_count,comments_count"
        )
    }

    return await _make_ig_api_request(endpoint, params)


async def get_media_comments(media_id: str, limit: int = 50) -> Dict[str, Any]:
    if not media_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="media_id wajib diisi."
        )

    safe_limit = max(1, min(int(limit), 100))

    endpoint = f"{media_id}/comments"
    params = {
        "limit": safe_limit,
        "fields": (
            "id,text,timestamp,username,like_count,"
            "replies{id,text,timestamp,username,like_count}"
        )
    }

    return await _make_ig_api_request(endpoint, params)


# async def debug_token() -> Dict[str, Any]:
#     token = config.IG_ACCESS_TOKEN

#     if not token or token == "MASUKKAN_ACCESS_TOKEN_ANDA_YANG_SUDAH_DIGENERATE":
#         return {
#             "status": "error",
#             "message": "Token tidak ditemukan atau masih placeholder.",
#             "is_configured": False,
#         }

#     return {
#         "status": "success",
#         "message": "Token terbaca oleh server.",
#         "is_configured": True,
#         "token_length": len(token),
#         "first_5_chars": token[:5],
#         "last_5_chars": token[-5:],
#     }