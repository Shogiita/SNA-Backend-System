from neo4j import GraphDatabase
from app.config import settings

class Neo4jConnector:
    def __init__(self):
        self.driver = None
        self.connect()

    def connect(self):
        try:
            self.driver = GraphDatabase.driver(
                settings.NEO4J_URI,
                auth=(settings.NEO4J_USER, settings.NEO4J_PASSWORD)
            )
            # Cek koneksi
            self.driver.verify_connectivity()
            print("✅ Berhasil terhubung ke Neo4j Aura")
        except Exception as e:
            print(f"⚠️ Gagal konek ke Neo4j (Fitur Graph akan nonaktif): {e}")
            self.driver = None

    def close(self):
        if self.driver:
            self.driver.close()

    def get_session(self):
        # Auto-reconnect jika driver belum ada atau terputus
        if not self.driver:
             print("🔄 Mencoba menghubungkan ulang ke Neo4j...")
             self.connect()
        
        if self.driver:
            return self.driver.session()
        else:
            raise Exception("Koneksi Neo4j tidak tersedia.")

neo4j_driver = Neo4jConnector()