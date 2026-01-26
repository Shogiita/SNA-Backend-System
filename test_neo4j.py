from neo4j import GraphDatabase
import os
from dotenv import load_dotenv

load_dotenv()

uri = os.getenv("NEO4J_URI")
user = os.getenv("NEO4J_USER")
password = os.getenv("NEO4J_PASSWORD")

print(f"Menguji koneksi ke: {uri}")
print(f"User: {user}")

try:
    driver = GraphDatabase.driver(uri, auth=(user, password))
    driver.verify_connectivity()
    print("✅ SUKSES! Koneksi berhasil.")
    driver.close()
except Exception as e:
    print(f"❌ GAGAL: {e}")