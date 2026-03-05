import asyncio
import datetime
import json
from fastapi.responses import StreamingResponse
from app.database import db, neo4j_driver

def check_neo4j_connection():
    try:
        neo4j_driver.verify_connectivity()
        return True
    except Exception as e:
        print(f"❌ STATUS: Gagal terhubung ke Neo4j.\nError: {e}")
        return False

def make_serializable(item):
    """Fungsi rekursif untuk memastikan semua data aman di-convert ke JSON"""
    if hasattr(item, 'isoformat'):
        return item.isoformat()
    if isinstance(item, dict):
        return {k: make_serializable(v) for k, v in item.items()}
    if isinstance(item, list):
        return [make_serializable(i) for i in item]
    return item

def prepare_for_neo4j(doc_dict):
    """
    Menyiapkan data dari Firestore agar 100% diterima oleh Neo4j.
    - Menghapus nilai None/Null.
    - Timestamp diubah ke format String ISO.
    - Dict (Map) dan List (Array) diubah menjadi JSON String.
    - Sisanya (String, Int, Float, Bool) dibiarkan murni.
    """
    cleaned = {}
    for k, v in doc_dict.items():
        if v is None:
            continue
            
        if isinstance(v, (dict, list)):
            serializable_val = make_serializable(v)
            cleaned[k] = json.dumps(serializable_val)
        elif hasattr(v, 'isoformat'):
            cleaned[k] = v.isoformat()
        elif isinstance(v, (bool, int, float, str)):
            cleaned[k] = v
        else:
            cleaned[k] = str(v)
            
    return cleaned

async def delete_all_neo4j_data():
    """Endpoint untuk menghapus seluruh isi database Neo4j"""
    if not check_neo4j_connection():
        return {"status": "error", "message": "Gagal terhubung ke Neo4j"}
        
    try:
        with neo4j_driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
        return {"status": "success", "message": "✅ Semua data di Neo4j berhasil dihapus bersih!"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

async def run_migration_streaming():
    if not check_neo4j_connection():
        return StreamingResponse(
            iter(["Gagal terhubung ke Neo4j.\n"]), media_type="text/plain"
        )

    def generate_progress():
        try:
            yield "✅ Koneksi Neo4j terhubung.\nMemulai Full Sync Migrasi Seluruh Tabel...\n"
            
            with neo4j_driver.session() as session:
                
                # Daftar koleksi, label terminal, dan query Cypher
                collections_to_migrate = [
                    {
                        "name": "users",
                        "label": "user",
                        "query": """
                            UNWIND $batch AS data
                            MERGE (n:User {id: data.id})
                            SET n += data
                        """
                    },
                    {
                        "name": "infoss",
                        "label": "infoss",
                        "query": """
                            UNWIND $batch AS data
                            MERGE (n:Infoss {id: data.id})
                            SET n += data
                        """
                    },
                    {
                        "name": "kawanss",
                        "label": "kawanss",
                        "query": """
                            UNWIND $batch AS data
                            MERGE (n:KawanSS {id: data.id})
                            SET n += data
                            WITH n, data
                            MERGE (u:User {id: data.userId})
                            MERGE (u)-[:POSTED]->(n)
                        """
                    },
                    {
                        "name": "kategoriInfoSS",
                        "label": "kategoriInfoSS",
                        "query": """
                            UNWIND $batch AS data
                            MERGE (n:KategoriInfoSS {id: data.id})
                            SET n += data
                        """
                    },
                    {
                        "name": "kategoriKawanSS",
                        "label": "kategoriKawanSS",
                        "query": """
                            UNWIND $batch AS data
                            MERGE (n:KategoriKawanSS {id: data.id})
                            SET n += data
                        """
                    },
                    {
                        "name": "infossLikes",
                        "label": "infossLikes",
                        "query": """
                            UNWIND $batch AS data
                            MERGE (u:User {id: data.userUid})
                            MERGE (i:Infoss {id: data.infossUid})
                            MERGE (u)-[r:LIKES_INFO {id: data.id}]->(i)
                            SET r += data
                        """
                    },
                    {
                        "name": "kawanssLikes",
                        "label": "kawanssLikes",
                        "query": """
                            UNWIND $batch AS data
                            MERGE (u:User {id: data.userUid})
                            MERGE (k:KawanSS {id: data.kawanssUid})
                            MERGE (u)-[r:LIKES_KAWAN {id: data.id}]->(k)
                            SET r += data
                        """
                    },
                    {
                        "name": "infossComments",
                        "label": "infossComments",
                        "query": """
                            UNWIND $batch AS data
                            MERGE (c:InfossComment {id: data.id})
                            SET c += data
                            WITH c, data
                            MERGE (u:User {id: data.userId})
                            MERGE (i:Infoss {id: data.infossUid})
                            MERGE (u)-[:WROTE]->(c)
                            MERGE (c)-[:COMMENTED_ON]->(i)
                        """
                    },
                    {
                        "name": "kawanssComments",
                        "label": "kawanssComments",
                        "query": """
                            UNWIND $batch AS data
                            MERGE (c:KawanssComment {id: data.id})
                            SET c += data
                            WITH c, data
                            MERGE (u:User {id: data.userId})
                            MERGE (k:KawanSS {id: data.kawanssUid})
                            MERGE (u)-[:WROTE]->(c)
                            MERGE (c)-[:COMMENTED_ON]->(k)
                        """
                    }
                ]

                # Looping Otomatis Untuk Semua Koleksi
                for task in collections_to_migrate:
                    col_name = task["name"]
                    label = task["label"]
                    query = task["query"]
                    
                    # 1. Ambil data dari Firestore
                    stream = db.collection(col_name).stream()
                    data_list = []
                    for doc in stream:
                        data = doc.to_dict() or {}
                        data['id'] = doc.id
                        
                        # Fallback keamanan untuk mencegah Error jika ada relasi yang terhapus/hilang dari Firebase
                        if col_name == "kawanss":
                            data['userId'] = data.get('userId', "unknown_user")
                        elif col_name == "infossLikes":
                            data['userUid'] = data.get('userUid', "unknown_user")
                            data['infossUid'] = data.get('infossUid', "unknown_post")
                        elif col_name == "kawanssLikes":
                            data['userUid'] = data.get('userUid', "unknown_user")
                            data['kawanssUid'] = data.get('kawanssUid', "unknown_post")
                        elif col_name == "infossComments":
                            data['userId'] = data.get('userId', "unknown_user")
                            data['infossUid'] = data.get('infossUid', "unknown_post")
                        elif col_name == "kawanssComments":
                            data['userId'] = data.get('userId', "unknown_user")
                            data['kawanssUid'] = data.get('kawanssUid', "unknown_post")
                            
                        data = prepare_for_neo4j(data)
                        data_list.append(data)
                        
                    total = len(data_list)
                    print(f"\n{label}")
                    yield f"Memigrasi {total} {label}...\n"
                    
                    if total == 0:
                        continue
                        
                    # 2. Push ke Neo4j secara bertahap (Batching)
                    count = 0
                    batch_size = 200 # <-- Memproses 200 data sekaligus agar lebih cepat dari 20
                    for i in range(0, total, batch_size):
                        batch = data_list[i:i+batch_size]
                        session.run(query, batch=batch)
                        count += len(batch)
                        print(f"{count}/{total}")

            yield "\n✅ Migrasi Seluruh Koleksi (Node & Relasi) Selesai Sepenuhnya!\n"

        except Exception as e:
            import traceback
            error_trace = traceback.format_exc()
            print(f"\n❌ Error Terhenti:\n{error_trace}")
            yield f"\n❌ Error Terhenti:\n{str(e)}\n"

    return StreamingResponse(generate_progress(), media_type="text/plain")