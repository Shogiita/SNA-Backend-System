import asyncio
import datetime
from fastapi.responses import StreamingResponse
from app.database import db, neo4j_driver

def check_neo4j_connection():
    """Fungsi untuk mengecek status koneksi ke Neo4j"""
    try:
        neo4j_driver.verify_connectivity()
        print("\n" + "="*50)
        print("✅ STATUS: Neo4j Berhasil Terhubung!")
        print("="*50 + "\n")
        return True
    except Exception as e:
        print("\n" + "="*50)
        print(f"❌ STATUS: Gagal terhubung ke Neo4j.\nError: {e}")
        print("="*50 + "\n")
        return False

def sanitize_firestore_data(data):
    """
    Helper rekursif untuk mengonversi DatetimeWithNanoseconds 
    atau tipe data tidak standar lainnya menjadi string agar didukung Neo4j.
    """
    if isinstance(data, datetime.datetime):
        return data.isoformat()
    elif isinstance(data, dict):
        return {k: sanitize_firestore_data(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [sanitize_firestore_data(v) for v in data]
    return data

async def run_migration_streaming():
    # 1. Cek Koneksi sebelum mulai migrasi
    if not check_neo4j_connection():
        return StreamingResponse(
            iter(["Gagal terhubung ke Neo4j. Silakan cek log terminal server.\n"]), 
            media_type="text/plain"
        )

    def generate_progress():
        try:
            yield "✅ Koneksi Neo4j terhubung.\nMemulai Full Sync Migrasi (Insert, Update, Delete)...\n"
            
            with neo4j_driver.session() as session:
                
                # ==========================================
                # 1. MIGRASI USERS (Nodes)
                # ==========================================
                users_stream = db.collection('users').stream()
                users_data = []
                active_user_ids = []
                
                for doc in users_stream:
                    data = doc.to_dict() or {}
                    data['id'] = doc.id
                    data = sanitize_firestore_data(data)
                    users_data.append(data)
                    active_user_ids.append(doc.id)
                    
                total_users = len(users_data)
                print("users")
                yield f"Memigrasi & Update {total_users} Users...\n"
                
                count = 0
                for i in range(0, total_users, 200):
                    batch = users_data[i:i+200]
                    session.run("""
                        UNWIND $batch AS user
                        MERGE (u:User {id: user.id})
                        SET u.nama = user.nama, u.username = user.username, u.email = user.email
                    """, batch=batch)
                    count += len(batch)
                    print(f"{count}/{total_users}")

                yield "Membersihkan data Users yang terhapus di Firebase...\n"
                session.run("""
                    MATCH (u:User)
                    WHERE NOT u.id IN $active_ids
                    DETACH DELETE u
                """, active_ids=active_user_ids)


                # ==========================================
                # 2. MIGRASI INFOSS (Nodes)
                # ==========================================
                infoss_stream = db.collection('infoss').stream()
                infoss_data = []
                active_infoss_ids = []
                
                for doc in infoss_stream:
                    data = doc.to_dict() or {}
                    data['id'] = doc.id
                    data = sanitize_firestore_data(data)
                    infoss_data.append(data)
                    active_infoss_ids.append(doc.id)
                    
                total_infoss = len(infoss_data)
                print("\ninfoss")
                yield f"Memigrasi & Update {total_infoss} Infoss...\n"
                
                count = 0
                for i in range(0, total_infoss, 200):
                    batch = infoss_data[i:i+200]
                    session.run("""
                        UNWIND $batch AS info
                        MERGE (i:Infoss {id: info.id})
                        SET i.judul = info.judul, i.kategori = info.kategori
                    """, batch=batch)
                    count += len(batch)
                    print(f"{count}/{total_infoss}")

                yield "Membersihkan data Infoss yang terhapus di Firebase...\n"
                session.run("""
                    MATCH (i:Infoss)
                    WHERE NOT i.id IN $active_ids
                    DETACH DELETE i
                """, active_ids=active_infoss_ids)


                # ==========================================
                # 3. MIGRASI KAWANSS & RELASI (Nodes + Edges)
                # ==========================================
                kawanss_stream = db.collection('kawanss').stream()
                kawanss_data = []
                active_kawanss_ids = []
                
                for doc in kawanss_stream:
                    data = doc.to_dict() or {}
                    data['id'] = doc.id
                    if 'userId' not in data:
                        data['userId'] = None
                    data = sanitize_firestore_data(data)
                    kawanss_data.append(data)
                    active_kawanss_ids.append(doc.id)
                    
                total_kawanss = len(kawanss_data)
                print("\nkawanss")
                yield f"Memigrasi & Update {total_kawanss} Kawanss beserta Relasi...\n"
                
                count = 0
                for i in range(0, total_kawanss, 200):
                    batch = kawanss_data[i:i+200]
                    session.run("""
                        UNWIND $batch AS kawan
                        MERGE (k:KawanSS {id: kawan.id})
                        SET k.title = kawan.title, k.deskripsi = kawan.deskripsi
                        WITH k, kawan
                        WHERE kawan.userId IS NOT NULL
                        MATCH (u:User {id: kawan.userId})
                        MERGE (u)-[:POSTED]->(k)
                    """, batch=batch)
                    count += len(batch)
                    print(f"{count}/{total_kawanss}")

                yield "Membersihkan data Kawanss yang terhapus di Firebase...\n"
                session.run("""
                    MATCH (k:KawanSS)
                    WHERE NOT k.id IN $active_ids
                    DETACH DELETE k
                """, active_ids=active_kawanss_ids)

            print("\n✅ Proses Sinkronisasi Selesai Sepenuhnya!")
            yield "Proses Migrasi & Sinkronisasi Selesai!\n"

        except Exception as e:
            import traceback
            error_details = traceback.format_exc()
            print(f"\n❌ Error Terhenti:\n{error_details}")
            yield f"Error Terhenti: {str(e)}\n"

    return StreamingResponse(generate_progress(), media_type="text/plain")