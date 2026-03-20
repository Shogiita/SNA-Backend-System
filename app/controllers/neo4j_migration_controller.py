import asyncio
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


async def run_migration_streaming():
    if not check_neo4j_connection():
        yield f"data: {json.dumps({'status': 'error', 'message': 'Gagal terhubung ke Neo4j'})}\n\n"
        return

    # Daftar koleksi dan query Cypher
    collections_to_migrate = [
        {"name": "users", "label": "user", "query": "UNWIND $batch AS data MERGE (n:User {id: data.id}) SET n += data"},
        {"name": "infoss", "label": "infoss", "query": "UNWIND $batch AS data MERGE (n:Infoss {id: data.id}) SET n += data"},
        {"name": "kawanss", "label": "kawanss", "query": "UNWIND $batch AS data MERGE (n:KawanSS {id: data.id}) SET n += data WITH n, data MERGE (u:User {id: data.userId}) MERGE (u)-[:POSTED]->(n)"},
        {"name": "kategoriInfoSS", "label": "kategoriInfoSS", "query": "UNWIND $batch AS data MERGE (n:KategoriInfoSS {id: data.id}) SET n += data"},
        {"name": "kategoriKawanSS", "label": "kategoriKawanSS", "query": "UNWIND $batch AS data MERGE (n:KategoriKawanSS {id: data.id}) SET n += data"},
        {"name": "infossLikes", "label": "infossLikes", "query": "UNWIND $batch AS data MERGE (u:User {id: data.userUid}) MERGE (i:Infoss {id: data.infossUid}) MERGE (u)-[r:LIKES_INFO {id: data.id}]->(i) SET r += data"},
        {"name": "kawanssLikes", "label": "kawanssLikes", "query": "UNWIND $batch AS data MERGE (u:User {id: data.userUid}) MERGE (k:KawanSS {id: data.kawanssUid}) MERGE (u)-[r:LIKES_KAWAN {id: data.id}]->(k) SET r += data"},
        {"name": "infossComments", "label": "infossComments", "query": "UNWIND $batch AS data MERGE (c:InfossComment {id: data.id}) SET c += data WITH c, data MERGE (u:User {id: data.userId}) MERGE (i:Infoss {id: data.infossUid}) MERGE (u)-[:WROTE]->(c) MERGE (c)-[:COMMENTED_ON]->(i)"},
        {"name": "kawanssComments", "label": "kawanssComments", "query": "UNWIND $batch AS data MERGE (c:KawanssComment {id: data.id}) SET c += data WITH c, data MERGE (u:User {id: data.userId}) MERGE (k:KawanSS {id: data.kawanssUid}) MERGE (u)-[:WROTE]->(c) MERGE (c)-[:COMMENTED_ON]->(k)"}
    ]

    try:
        # Menghitung total seluruh dokumen di semua koleksi untuk Global Progress Bar
        total_all_docs = 0
        yield f"data: {json.dumps({'status': 'info', 'message': 'Menghitung total data di Firebase...'})}\n\n"
        for task in collections_to_migrate:
            # Gunakan count() yang lebih efisien di Firestore jika tersedia, 
            # jika tidak, kita tangkap dari aggregasi.
            col_ref = db.collection(task["name"])
            count_query = col_ref.count()
            count_result = count_query.get()
            total_all_docs += count_result[0][0].value

        if total_all_docs == 0:
            yield f"data: {json.dumps({'status': 'complete', 'progress': 100, 'message': 'Tidak ada data untuk dimigrasi'})}\n\n"
            return

        processed_global = 0
        batch_size = 500  # Memproses 500 data sekaligus (Optimal untuk Neo4j)

        with neo4j_driver.session() as session:
            for task in collections_to_migrate:
                col_name = task["name"]
                label = task["label"]
                query = task["query"]
                
                stream = db.collection(col_name).stream()
                current_batch = []
                
                for doc in stream:
                    data = doc.to_dict() or {}
                    data['id'] = doc.id
                    
                    # Fallback Keamanan (Sesuai kodemu)
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
                    
                    # Jika batch sudah 500, lempar ke Neo4j, lalu kosongkan memori (RAM aman)
                    if len(current_batch) >= batch_size:
                        session.run(query, batch=current_batch)
                        current_batch = [] # Kosongkan memori
                        
                        # Kirim progres ke frontend
                        progress_percent = int((processed_global / total_all_docs) * 100)
                        yield f"data: {json.dumps({'status': 'progress', 'progress': progress_percent, 'message': f'Memigrasi {label}: {processed_global}/{total_all_docs}'})}\n\n"
                        await asyncio.sleep(0.01) # Jeda kecil untuk Event Loop
                
                # Eksekusi sisa batch yang kurang dari 500
                if current_batch:
                    session.run(query, batch=current_batch)
                    progress_percent = int((processed_global / total_all_docs) * 100)
                    yield f"data: {json.dumps({'status': 'progress', 'progress': progress_percent, 'message': f'Memigrasi {label}: {processed_global}/{total_all_docs}'})}\n\n"
                    await asyncio.sleep(0.01)

        # SELESAI
        yield f"data: {json.dumps({'status': 'complete', 'progress': 100, 'message': '✅ Migrasi Seluruh Koleksi Selesai Sepenuhnya!'})}\n\n"

    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        print(f"\n❌ Error Terhenti:\n{error_trace}")
        yield f"data: {json.dumps({'status': 'error', 'message': str(e)})}\n\n"