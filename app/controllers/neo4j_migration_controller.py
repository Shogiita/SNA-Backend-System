import asyncio
from fastapi.responses import StreamingResponse
from app.database import db, neo4j_driver

async def run_migration_streaming():
    def generate_progress():
        try:
            # 1. Ambil semua ID dari Firestore (Operasi ringan menggunakan select)
            users_ids = [d.id for d in db.collection('users').select([]).stream()]
            infoss_ids = [d.id for d in db.collection('infoss').select([]).stream()]
            kawanss_ids = [d.id for d in db.collection('kawanss').select([]).stream()]

            total_data = len(users_ids) + len(infoss_ids) + len(kawanss_ids)
            current_count = 0

            yield f"Total data di Firebase: {total_data}\n"

            with neo4j_driver.session() as session:
                # --- FUNGSI HELPER PENGECEKAN ---
                def node_exists(label, node_id):
                    query = f"MATCH (n:{label} {{id: $id}}) RETURN n.id"
                    result = session.run(query, id=node_id)
                    return result.single() is not None

                # --- MIGRASI USERS ---
                for uid in users_ids:
                    if not node_exists("User", uid):
                        doc = db.collection('users').document(uid).get().to_dict()
                        if doc:
                            session.run("""
                                MERGE (u:User {id: $id})
                                SET u.nama = $nama, u.username = $username, u.email = $email
                            """, id=uid, nama=doc.get('nama'), username=doc.get('username'), email=doc.get('email'))
                    
                    current_count += 1
                    yield f"{current_count}/{total_data}\n"

                # --- MIGRASI INFOSS ---
                for iid in infoss_ids:
                    if not node_exists("Infoss", iid):
                        doc = db.collection('infoss').document(iid).get().to_dict()
                        if doc:
                            session.run("""
                                MERGE (i:Infoss {id: $id})
                                SET i.judul = $judul, i.kategori = $kategori
                            """, id=iid, judul=doc.get('judul'), kategori=doc.get('kategori'))
                    
                    current_count += 1
                    yield f"{current_count}/{total_data}\n"

                # --- MIGRASI KAWANSS ---
                for kid in kawanss_ids:
                    if not node_exists("KawanSS", kid):
                        doc = db.collection('kawanss').document(kid).get().to_dict()
                        if doc:
                            session.run("""
                                MERGE (k:KawanSS {id: $id})
                                SET k.title = $title, k.deskripsi = $deskripsi
                                WITH k
                                MATCH (u:User {id: $userId})
                                MERGE (u)-[:POSTED]->(k)
                            """, id=kid, title=doc.get('title'), deskripsi=doc.get('deskripsi'), userId=doc.get('userId'))
                    
                    current_count += 1
                    yield f"{current_count}/{total_data}\n"

            yield f"Migrasi Selesai: {current_count}/{total_data}\n"

        except Exception as e:
            yield f"Error Terhenti: {str(e)}\n"

    return StreamingResponse(generate_progress(), media_type="text/plain")