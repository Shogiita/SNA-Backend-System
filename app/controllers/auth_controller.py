import httpx
from fastapi import HTTPException
from app import config 
INSTAGRAM_REFRESH_URL = "https://graph.instagram.com"

async def refresh_instagram_token():
    """
    Logika untuk me-refresh Instagram Long-Lived Access Token
    menggunakan variabel dari config.py.
    """
    
    current_token = config.IG_ACCESS_TOKEN 
    
    if not current_token or current_token.startswith("MASUKKAN"):
        raise HTTPException(
            status_code=500, 
            detail="INSTAGRAM_ACCESS_TOKEN tidak diatur atau masih placeholder di file .env"
        )

    refresh_url = f"{INSTAGRAM_REFRESH_URL}/refresh_access_token"
    
    params = {
        "grant_type": "ig_refresh_token",
        "access_token": current_token
    }

    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(refresh_url, params=params)

        if response.status_code == 200:
            data = response.json()
            return {
                "message": "Token berhasil di-refresh",
                "new_access_token": data.get("access_token"),
                "expires_in_seconds": data.get("expires_in"),
                "token_type": data.get("token_type")
            }
        else:
            raise HTTPException(
                status_code=response.status_code,
                detail=f"Gagal me-refresh token: {response.json()}"
            )
            
    except httpx.RequestError as e:
        raise HTTPException(
            status_code=500, 
            detail=f"Terjadi kesalahan saat menghubungi API Instagram: {str(e)}"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))