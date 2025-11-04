import httpx
from fastapi import HTTPException, status
from app import config # Impor konfigurasi yang baru kita buat

async def _check_config():
    """Helper untuk memvalidasi konfigurasi sebelum membuat panggilan API."""
    if not config.IG_BUSINESS_ACCOUNT_ID:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="INSTAGRAM_BUSINESS_ACCOUNT_ID belum diatur di server."
        )
    if not config.IG_ACCESS_TOKEN or config.IG_ACCESS_TOKEN == "MASUKKAN_ACCESS_TOKEN_ANDA_YANG_SUDAH_DIGENERATE":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="INSTAGRAM_ACCESS_TOKEN belum di-generate atau belum diatur di server. Silakan generate token di Meta Dashboard."
        )

async def _make_ig_api_request(endpoint: str, params: dict = {}):
    """Fungsi helper untuk melakukan panggilan ke Instagram Graph API."""
    await _check_config()
    
    base_url = f"{config.GRAPH_API_URL}/{endpoint}"
    
    # Selalu tambahkan access token ke setiap permintaan
    all_params = {"access_token": config.IG_ACCESS_TOKEN, **params}
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(base_url, params=all_params)
            
            # Melemparkan error jika respons API tidak sukses
            response.raise_for_status() 
            
            return response.json()
            
    except httpx.HTTPStatusError as e:
        # Jika API Facebook/Instagram mengembalikan error
        error_data = e.response.json()
        raise HTTPException(
            status_code=e.response.status_code,
            detail=f"Error dari Instagram API: {error_data.get('error', {}).get('message', 'Unknown error')}"
        )
    except httpx.RequestError as e:
        # Jika ada error jaringan
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Gagal menghubungi Instagram API: {str(e)}"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Terjadi kesalahan internal: {str(e)}"
        )

# --- Logika Endpoint ---

async def get_user_profile():
    """
    Mengambil data profil dasar untuk akun 'suarasurabayamedia'.
    """
    endpoint = f"/{config.IG_BUSINESS_ACCOUNT_ID}"
    
    # Tentukan field (data) apa saja yang ingin Anda ambil
    # Referensi: https://developers.facebook.com/docs/instagram-api/reference/ig-user
    params = {
        "fields": "id,username,name,biography,followers_count,follows_count,media_count,profile_picture_url"
    }
    
    return await _make_ig_api_request(endpoint, params)

async def get_user_media(limit: int = 25):
    """
    Mengambil media (postingan) terbaru dari akun 'suarasurabayamedia'.
    """
    endpoint = f"/{config.IG_BUSINESS_ACCOUNT_ID}/media"
    
    # Tentukan field media yang ingin Anda ambil
    # Referensi: https://developers.facebook.com/docs/instagram-api/reference/ig-media
    params = {
        "fields": "id,caption,media_type,media_url,thumbnail_url,permalink,timestamp,like_count,comments_count,username",
        "limit": limit
    }
    
    return await _make_ig_api_request(endpoint, params)

# --- FUNGSI DEBUG BARU ---
async def debug_token():
    """
    Mengembalikan token yang saat ini dimuat oleh server untuk debugging.
    JANGAN GUNAKAN INI DI PRODUKSI.
    """
    token = config.IG_ACCESS_TOKEN
    
    if not token or token == "MASUKKAN_ACCESS_TOKEN_ANDA_YANG_SUDAH_DIGENERATE":
        return {
            "error": "Token TIDAK DITEMUKAN atau masih placeholder di file .env",
            "loaded_token": token
        }
    
    # Ini akan menunjukkan kepada kita jika ada spasi tersembunyi
    return {
        "message": "Ini adalah token yang dibaca server dari file .env Anda:",
        "token_length": len(token),
        "first_5_chars": token[:5],
        "last_5_chars": token[-5:],
        "loaded_token": token
    }

