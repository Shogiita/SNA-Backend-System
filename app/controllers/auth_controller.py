import httpx
import os
import re
import time
from fastapi import HTTPException
from app import config

FB_GRAPH_URL = config.GRAPH_API_URL 

def update_env_file(key, new_value):
    """Update file .env secara otomatis."""
    env_path = ".env"
    if not os.path.exists(env_path): return
        
    with open(env_path, "r") as f:
        lines = f.readlines()
    
    found = False
    with open(env_path, "w") as f:
        for line in lines:
            if line.startswith(f"{key}="):
                f.write(f"{key}={new_value}\n")
                found = True
            else:
                f.write(line)
        if not found:
            f.write(f"\n{key}={new_value}")

async def refresh_instagram_token():
    """
    Menukar Long-Lived Token lama dengan yang baru.
    Instagram Long-lived tokens bisa di-refresh setiap 60 hari.
    """
    current_token = config.IG_ACCESS_TOKEN
    client_id = config.IG_APP_ID
    client_secret = config.IG_APP_SECRET

    if not current_token:
        raise HTTPException(status_code=400, detail="Token tidak ditemukan di konfigurasi.")

    # Endpoint untuk menukar Long-Lived Token (Refresh)
    # Docs: GET /oauth/access_token?grant_type=fb_exchange_token...
    refresh_url = f"{FB_GRAPH_URL}/oauth/access_token"
    params = {
        "grant_type": "fb_exchange_token",
        "client_id": client_id,
        "client_secret": client_secret,
        "fb_exchange_token": current_token
    }

    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(refresh_url, params=params)
            data = response.json()
            
            if response.status_code != 200:
                # Jika token sudah mati total dan tidak bisa di-refresh
                error_msg = data.get("error", {}).get("message", "Unknown error")
                raise HTTPException(
                    status_code=response.status_code, 
                    detail=f"Gagal refresh token: {error_msg}. Anda mungkin perlu login ulang secara manual."
                )

            new_token = data.get("access_token")
            expires_in = data.get("expires_in") # Dalam detik

            if new_token:
                # 1. Update di Memory Aplikasi
                config.IG_ACCESS_TOKEN = new_token
                
                # 2. Update di File .env untuk permanensi
                update_env_file("INSTAGRAM_ACCESS_TOKEN", new_token)
                
                return {
                    "status": "success",
                    "message": "Instagram Access Token berhasil diperbarui.",
                    "expires_in_days": round(expires_in / 86400, 2) if expires_in else "60",
                    "preview": f"{new_token[:15]}..."
                }

        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")

    raise HTTPException(status_code=400, detail="Gagal memperbarui token.")