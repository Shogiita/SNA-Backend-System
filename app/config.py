import os
from dotenv import load_dotenv

# Memuat variabel dari file .env di root project
load_dotenv()

# --- Konfigurasi Instagram ---
IG_BUSINESS_ACCOUNT_ID = os.getenv("INSTAGRAM_BUSINESS_ACCOUNT_ID")
IG_ACCESS_TOKEN = os.getenv("INSTAGRAM_ACCESS_TOKEN")
IG_APP_ID = os.getenv("INSTAGRAM_APP_ID")
IG_APP_SECRET = os.getenv("INSTAGRAM_APP_SECRET")
IG_USERNAME = os.getenv("INSTAGRAM_USERNAME")

# --- Konfigurasi API Graph ---
# Sebaiknya tentukan versi API agar tetap stabil
GRAPH_API_VERSION = "v19.0" 
GRAPH_API_URL = f"https://graph.facebook.com/{GRAPH_API_VERSION}"

# --- Validasi Instagram Token ---
if not IG_ACCESS_TOKEN or IG_ACCESS_TOKEN == "MASUKKAN_ACCESS_TOKEN_ANDA_YANG_SUDAH_DIGENERATE":
    print("PERINGATAN: INSTAGRAM_ACCESS_TOKEN belum diatur di file .env.")

# --- Konfigurasi Firebase (DARI ENV) ---
# Membaca Private Key dan mengganti '\\n' string menjadi karakter newline asli
private_key = os.getenv("FIREBASE_PRIVATE_KEY")
if private_key:
    private_key = private_key.replace('\\n', '\n')

FIREBASE_CREDENTIALS = {
    "type": os.getenv("FIREBASE_TYPE"),
    "project_id": os.getenv("FIREBASE_PROJECT_ID"),
    "private_key_id": os.getenv("FIREBASE_PRIVATE_KEY_ID"),
    "private_key": private_key,
    "client_email": os.getenv("FIREBASE_CLIENT_EMAIL"),
    "client_id": os.getenv("FIREBASE_CLIENT_ID"),
    "auth_uri": os.getenv("FIREBASE_AUTH_URI"),
    "token_uri": os.getenv("FIREBASE_TOKEN_URI"),
    "auth_provider_x509_cert_url": os.getenv("FIREBASE_AUTH_PROVIDER_CERT_URL"),
    "client_x509_cert_url": os.getenv("FIREBASE_CLIENT_CERT_URL")
}