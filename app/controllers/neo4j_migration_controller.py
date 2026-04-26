import asyncio
import json
from app.database import db, neo4j_driver

#done
def check_neo4j_connection():
    try:
        neo4j_driver.verify_connectivity()
        return True
    except Exception as e:
        print(f"❌ STATUS: Gagal terhubung ke Neo4j.\nError: {e}")
        return False

def make_serializable(item):
    if hasattr(item, 'isoformat'):
        return item.isoformat()
    if isinstance(item, dict):
        return {k: make_serializable(v) for k, v in item.items()}
    if isinstance(item, list):
        return [make_serializable(i) for i in item]
    return item

def prepare_for_neo4j(doc_dict):
    cleaned = {}
    for k, v in doc_dict.items():
        if v is None: continue
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
    if not check_neo4j_connection():
        return {"status": "error", "message": "Gagal terhubung ke Neo4j"}
    try:
        with neo4j_driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
        return {"status": "success", "message": "✅ Semua data di Neo4j berhasil dihapus bersih!"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

async def run_migration_background():
    """Menjalankan migrasi secara background tanpa menggunakan Event Stream (SSE)"""
    if not check_neo4j_connection():
        print("❌ [MIGRATION] Gagal terhubung ke Neo4j. Proses dihentikan.")
        return

    # Daftar koleksi yang dimigrasi dengan Label dan Relasi KHUSUS FIREBASE
    collections_to_migrate = [
        {
            "name": "users", 
            "label": "FirebaseUser", 
            "query": "UNWIND $batch AS data MERGE (n:FirebaseUser {id: data.id}) SET n += data, n.last_updated = datetime()"
        },
        {
            "name": "infoss", 
            "label": "FirebaseInfoss", 
            "query": "UNWIND $batch AS data MERGE (n:FirebaseInfoss {id: data.id}) SET n += data, n.last_updated = datetime()"
        },
        {
            "name": "kawanss", 
            "label": "FirebaseKawanSS", 
            "query": "UNWIND $batch AS data MERGE (n:FirebaseKawanSS {id: data.id}) SET n += data, n.last_updated = datetime() WITH n, data MERGE (u:FirebaseUser {id: data.userId}) MERGE (u)-[r:POSTED_FB]->(n) SET r.last_updated = datetime()"
        },
        {
            "name": "infossLikes", 
            "label": "FirebaseInfossLikes", 
            "query": "UNWIND $batch AS data MERGE (u:FirebaseUser {id: data.userUid}) MERGE (i:FirebaseInfoss {id: data.infossUid}) MERGE (u)-[r:LIKES_INFO_FB {id: data.id}]->(i) SET r += data, r.last_updated = datetime()"
        },
        {
            "name": "kawanssLikes", 
            "label": "FirebaseKawanssLikes", 
            "query": "UNWIND $batch AS data MERGE (u:FirebaseUser {id: data.userUid}) MERGE (k:FirebaseKawanSS {id: data.kawanssUid}) MERGE (u)-[r:LIKES_KAWAN_FB {id: data.id}]->(k) SET r += data, r.last_updated = datetime()"
        },
        {
            "name": "infossComments", 
            "label": "FirebaseInfossComment", 
            "query": "UNWIND $batch AS data MERGE (c:FirebaseInfossComment {id: data.id}) SET c += data, c.last_updated = datetime() WITH c, data MERGE (u:FirebaseUser {id: data.userUid}) MERGE (i:FirebaseInfoss {id: data.infossUid}) MERGE (u)-[r1:WROTE_FB]->(c) SET r1.last_updated = datetime() MERGE (c)-[r2:COMMENTED_ON_FB]->(i) SET r2.last_updated = datetime()"
        },
        {
            "name": "kawanssComments", 
            "label": "FirebaseKawanSSComment", 
            "query": "UNWIND $batch AS data MERGE (c:FirebaseKawanSSComment {id: data.id}) SET c += data, c.last_updated = datetime() WITH c, data MERGE (u:FirebaseUser {id: data.userUid}) MERGE (k:FirebaseKawanSS {id: data.kawanssUid}) MERGE (u)-[r1:WROTE_FB]->(c) SET r1.last_updated = datetime() MERGE (c)-[r2:COMMENTED_ON_FB]->(k) SET r2.last_updated = datetime()"
        }
    ]

    try:
        total_all_docs = 0
        print("\n🚀 [MIGRATION] Menghitung total data di Firebase...")
        for task in collections_to_migrate:
            col_ref = db.collection(task["name"])
            count_query = col_ref.count()
            count_result = count_query.get()
            total_all_docs += count_result[0][0].value

        if total_all_docs == 0:
            print("⚠️ [MIGRATION] Tidak ada data untuk dimigrasi.")
            return

        print(f"📊 [MIGRATION] Total {total_all_docs} dokumen siap dimigrasi.")
        processed_global = 0
        chunk_size = 500

        with neo4j_driver.session() as session:
            for task in collections_to_migrate:
                col_name = task["name"]
                label = task["label"]
                query = task["query"]
                
                print(f"🔄 [MIGRATION] Memproses koleksi: {col_name}...")
                fs_query = db.collection(col_name).order_by("__name__").limit(chunk_size)
                
                while True:
                    docs = list(fs_query.stream())
                    if not docs:
                        break
                        
                    current_batch = []
                    for doc in docs:
                        data = doc.to_dict() or {}
                        data['id'] = doc.id
                        
                        if col_name == "kawanss":
                            data['userId'] = data.get('userId', "unknown_user")
                        elif col_name == "infossLikes" or col_name == "infossComments":
                            data['userUid'] = data.get('userUid', data.get('userId', "unknown_user"))
                            data['infossUid'] = data.get('infossUid', "unknown_post")
                        elif col_name == "kawanssLikes" or col_name == "kawanssComments":
                            data['userUid'] = data.get('userUid', data.get('userId', "unknown_user"))
                            data['kawanssUid'] = data.get('kawanssUid', "unknown_post")

                        data = prepare_for_neo4j(data)
                        current_batch.append(data)
                        processed_global += 1
                        
                    if current_batch:
                        session.run(query, batch=current_batch)
                        progress_percent = int((processed_global / total_all_docs) * 100)
                        print(f"   ⏳ Progress {label}: {progress_percent}% ({processed_global}/{total_all_docs})")
                        await asyncio.sleep(0.01)
                        
                    last_doc = docs[-1]
                    fs_query = db.collection(col_name).order_by("__name__").start_after(last_doc).limit(chunk_size)

        print("✅ [MIGRATION] Seluruh Koleksi Firebase Berhasil Dimigrasi ke Neo4j!\n")

    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        print(f"\n❌ [MIGRATION ERROR] Terhenti karena:\n{error_trace}")