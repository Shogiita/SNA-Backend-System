import os
from dotenv import load_dotenv

# Memuat variabel dari file .env di root project
load_dotenv()

# --- Konfigurasi Instagram ---
IG_BUSINESS_ACCOUNT_ID = os.getenv("INSTAGRAM_BUSINESS_ACCOUNT_ID")
IG_ACCESS_TOKEN = os.getenv("INSTAGRAM_ACCESS_TOKEN")
IG_APP_ID = os.getenv("INSTAGRAM_APP_ID")
IG_APP_SECRET = os.getenv("INSTAGRAM_APP_SECRET")
IG_USERNAME = os.getenv("INSTAGRAM_USERNAME")  # <--- INI WAJIB ADA

# --- Konfigurasi API Graph ---
# Sebaiknya tentukan versi API agar tetap stabil
GRAPH_API_VERSION = "v19.0" 
GRAPH_API_URL = f"https://graph.facebook.com/{GRAPH_API_VERSION}"

# --- Validasi ---
# Memastikan token akses sudah diisi dan bukan placeholder
if not IG_ACCESS_TOKEN or IG_ACCESS_TOKEN == "MASUKKAN_ACCESS_TOKEN_ANDA_YANG_SUDAH_DIGENERATE":
    print("PERINGATAN: INSTAGRAM_ACCESS_TOKEN belum diatur di file .env.")
    print("Silakan generate token di Meta Dashboard dan masukkan ke file .env.")