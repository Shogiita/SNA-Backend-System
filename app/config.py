import os
from dotenv import load_dotenv
from pydantic_settings import BaseSettings

# Memuat variabel dari file .env di root project
load_dotenv()

class Settings(BaseSettings):
    # ==========================================
    # 1. Konfigurasi Firebase
    # ==========================================
    FIREBASE_CRED_PATH: str = os.getenv("FIREBASE_CRED_PATH", "firebase_credentials.json")
    
    # ==========================================
    # 2. Konfigurasi Neo4j (Aura / Docker)
    # ==========================================
    NEO4J_URI: str = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    NEO4J_USER: str = os.getenv("NEO4J_USER", "neo4j")
    NEO4J_PASSWORD: str = os.getenv("NEO4J_PASSWORD", "password")

    # ==========================================
    # 3. Konfigurasi Instagram & Graph API
    # ==========================================
    IG_BUSINESS_ACCOUNT_ID: str = os.getenv("INSTAGRAM_BUSINESS_ACCOUNT_ID", "")
    IG_ACCESS_TOKEN: str = os.getenv("INSTAGRAM_ACCESS_TOKEN", "")
    IG_APP_ID: str = os.getenv("INSTAGRAM_APP_ID", "")
    IG_APP_SECRET: str = os.getenv("INSTAGRAM_APP_SECRET", "")
    IG_USERNAME: str = os.getenv("INSTAGRAM_USERNAME", "") 

    # Versi API Graph
    GRAPH_API_VERSION: str = "v19.0"
    
    # URL Graph API
    GRAPH_API_URL: str = f"https://graph.facebook.com/{GRAPH_API_VERSION}"

    class Config:
        env_file = ".env"
        extra = "ignore"

# Inisialisasi Settings
settings = Settings()

# ==========================================
# 4. BACKWARD COMPATIBILITY (PENTING!)
# ==========================================
# Baris ini wajib ada agar 'auth_controller.py' tidak crash
GRAPH_API_URL = settings.GRAPH_API_URL