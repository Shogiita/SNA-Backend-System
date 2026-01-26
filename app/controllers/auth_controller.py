import httpx
import os
import re
import time
from fastapi import HTTPException
from app import config

# Gunakan Graph API Facebook, bukan Basic Display
FB_GRAPH_URL = config.GRAPH_API_URL 

def update_env_file(key, new_value):
    """Update file .env secara otomatis."""
    env_path = ".env"
    if not os.path.exists(env_path):
        return
        
    with open(env_path, "r") as f:
        content = f.read()
    
    pattern = f"^{key}=.*"
    replacement = f"{key}={new_value}"
    
    if re.search(pattern, content, flags=re.MULTILINE):
        new_content = re.sub(pattern, replacement, content, flags=re.MULTILINE)
    else:
        new_content = content + f"\n{replacement}"
        
    with open(env_path, "w") as f:
        f.write(new_content)

async def check_token_status(current_token):
    """
    Mengecek status token (validitas & expiry) menggunakan endpoint debug_token.
    """
    debug_url = f"{FB_GRAPH_URL}/debug_token"
    # Kita butuh access_token (bisa app token atau user token itu sendiri) untuk debug
    params = {
        "input_token": current_token,
        "access_token": f"{config.IG_APP_ID}|{config.IG_APP_SECRET}" # Menggunakan App Access Token
    }
    
    async with httpx.AsyncClient() as client:
        resp = await client.get(debug_url, params=params)
        data = resp.json()
        
    if "data" not in data:
        raise HTTPException(status_code=400, detail=f"Gagal mengecek token: {data}")
        
    return data["data"]

async def refresh_instagram_token():
    """
    Logika Refresh Token KHUSUS untuk Graph API (EAA...).
    Hanya refresh jika token masih valid.
    """
    current_token = config.IG_ACCESS_TOKEN
    app_id = config.IG_APP_ID
    app_secret = config.IG_APP_SECRET

    if not current_token or not app_id or not app_secret:
        raise HTTPException(status_code=500, detail="Konfigurasi Token/App ID/Secret belum lengkap di .env")

    # 1. Cek Status Token Dulu
    token_info = await check_token_status(current_token)
    
    is_valid = token_info.get("is_valid", False)
    expires_at = token_info.get("expires_at", 0)
    current_time = int(time.time())
    
    # Jika Token SUDAH EXPIRED atau TIDAK VALID
    if not is_valid:
        raise HTTPException(
            status_code=401, 
            detail="Token sudah EXPIRED atau TIDAK VALID. Sistem tidak bisa refresh otomatis. Silakan generate token baru secara manual."
        )

    # Opsi: Tentukan batas waktu refresh (misal: refresh jika sisa waktu < 7 hari)
    # Jika masih lama expired-nya, kita bisa kembalikan info saja tanpa refresh.
    # Namun jika Anda ingin paksa refresh, bagian 'if' ini bisa dilewati.
    days_left = (expires_at - current_time) / 86400
    if days_left > 7:
         return {
            "message": "Token masih aman, tidak perlu refresh.",
            "days_left": f"{days_left:.2f} hari",
            "expires_at": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(expires_at))
        }

    # 2. Lakukan Refresh (Exchange Token)
    # Endpoint untuk menukar token lama dengan yang baru (Long-Lived)
    exchange_url = f"{FB_GRAPH_URL}/oauth/access_token"
    params = {
        "grant_type": "fb_exchange_token",
        "client_id": app_id,
        "client_secret": app_secret,
        "fb_exchange_token": current_token
    }

    async with httpx.AsyncClient() as client:
        response = await client.get(exchange_url, params=params)
        
    if response.status_code == 200:
        data = response.json()
        new_token = data.get("access_token")
        expires_in = data.get("expires_in") # Detik
        
        if new_token:
            # Update Memory & File .env
            config.IG_ACCESS_TOKEN = new_token
            update_env_file("INSTAGRAM_ACCESS_TOKEN", new_token)
            
            return {
                "message": "Token berhasil diperbarui dan disimpan.",
                "expires_in_seconds": expires_in,
                "new_token_preview": new_token[:10] + "..."
            }
    
    # Jika gagal
    error_data = response.json()
    raise HTTPException(
        status_code=response.status_code,
        detail=f"Gagal refresh token Graph API: {error_data}"
    )