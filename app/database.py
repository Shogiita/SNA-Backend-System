import firebase_admin
from firebase_admin import credentials, firestore
from neo4j import GraphDatabase
from app import config

if not config.FIREBASE_CREDENTIALS["private_key"]:
    raise ValueError("Firebase Private Key tidak ditemukan di environment variables!")

cred = credentials.Certificate(config.FIREBASE_CREDENTIALS)

try:
    firebase_admin.get_app()
except ValueError:
    firebase_admin.initialize_app(cred)

db = firestore.client()

# ===============================================================
# PERBAIKAN FATAL: Konfigurasi Anti-Hang & Anti-Deadlock untuk Neo4j Aura
# ===============================================================
neo4j_driver = GraphDatabase.driver(
    config.NEO4J_URI, 
    auth=(config.NEO4J_USER, config.NEO4J_PASSWORD),
    max_connection_lifetime=200,         # Wajib! Buang koneksi sebelum diputus sepihak oleh Cloud (3-4 menit)
    max_connection_pool_size=50,         # Batasi memori pool
    connection_acquisition_timeout=10.0, # JANGAN tunggu selamanya jika macet (maks 10 detik)
    keep_alive=True                      # Deteksi koneksi jaringan yang terputus diam-diam
)

def get_neo4j_session():
    return neo4j_driver.session()