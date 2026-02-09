import os
from dotenv import load_dotenv

# Memuat variabel dari file .env di root project
load_dotenv()

# ... (Kode konfigurasi Instagram & Graph API yang sudah ada biarkan saja) ...

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