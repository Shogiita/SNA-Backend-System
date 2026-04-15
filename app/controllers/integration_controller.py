import io
import json
import os
import pandas as pd
import networkx as nx
import leidenalg as la
import igraph as ig
import gspread
import datetime
from google.oauth2.service_account import Credentials
from fastapi import HTTPException, Response, UploadFile
from fastapi.responses import StreamingResponse
from app.database import neo4j_driver, db
from app import config

# Konfigurasi GSpread menggunakan Service Account Firebase yang sudah ada
def get_gspread_client():
    try:
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        creds = Credentials.from_service_account_info(config.FIREBASE_CREDENTIALS, scopes=scopes)
        return gspread.authorize(creds)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Gagal otentikasi Google Sheets: {str(e)}")


def get_master_dataframe(source_type: str, start_date: str = None, end_date: str = None, selected_columns: list = None, export_all: bool = True) -> pd.DataFrame:
    """Mengambil SEMUA data, memformat tanggal, menambah link, memfilter rentang waktu, dan memfilter kolom (custom)"""
    dataset = []

    # -----------------------------------------------------------------
    # A. SUMBER DATA: APP (Neo4j)
    # -----------------------------------------------------------------
    if source_type == 'app':
        # Mengambil dari Neo4j (KawanSS / Infoss beserta komentarnya)
        query = """
        MATCH (author:User)-[:POSTED|AUTHORED]->(p)
        WHERE (p:KawanSS OR p:Infoss) AND (p.isDeleted = false OR p.isDeleted IS NULL)
        OPTIONAL MATCH (c_author:User)-[:WROTE]->(c)-[:COMMENTED_ON]->(p)
        RETURN 
            coalesce(author.username, author.nama, author.id) AS Post_Author,
            coalesce(p.judul, p.title, p.deskripsi, 'No Content') AS Post_Content,
            coalesce(p.createdAt, p.uploadDate, '') AS Upload_Date,
            coalesce(toInteger(p.jumlahLike), 0) AS Post_Likes,
            coalesce(toInteger(p.jumlahView), 0) AS Post_Views,
            coalesce(toInteger(p.jumlahComment), 0) AS Post_Comments,
            coalesce(toInteger(p.jumlahShare), 0) AS Post_Shares,
            coalesce(c_author.username, c_author.nama, c_author.id) AS Comment_Author,
            coalesce(c.text, c.komentar, '') AS Comment_Content,
            coalesce(toInteger(c.likes), 0) AS Comment_Likes,
            0 AS Comment_Replies_Count,
            p.id AS Target_Post_ID
        """
        with neo4j_driver.session() as session:
            records = session.run(query).data()
            
        for r in records:
            # Baris untuk Postingan Utama
            dataset.append({
                "Interaction_Type": "POST",
                "Source_User": r["Post_Author"],
                "Target": r["Target_Post_ID"],
                "Post_Link": "", # App internal tidak punya permalink eksternal
                "User_Pembuat_Post": r["Post_Author"],
                "Post": r["Post_Content"],
                "Tanggal_Upload": r["Upload_Date"],
                "Jumlah_Like_Post": r["Post_Likes"],
                "Jumlah_Views_Post": r["Post_Views"],
                "Jumlah_Comment_Post": r["Post_Comments"],
                "Jumlah_Share_Post": r["Post_Shares"],
                "Komentar": "",
                "Balasan_Komentar": "",
                "Jumlah_Like_Komentar": 0,
                "Jumlah_Reply_Komentar": 0
            })
            
            # Baris untuk Komentar (Jika ada)
            if r["Comment_Author"] and r["Comment_Content"]:
                dataset.append({
                    "Interaction_Type": "COMMENT",
                    "Source_User": r["Comment_Author"],
                    "Target": r["Target_Post_ID"],
                    "Post_Link": "",
                    "User_Pembuat_Post": r["Post_Author"],
                    "Post": r["Post_Content"],
                    "Tanggal_Upload": r["Upload_Date"],
                    "Jumlah_Like_Post": r["Post_Likes"],
                    "Jumlah_Views_Post": r["Post_Views"],
                    "Jumlah_Comment_Post": r["Post_Comments"],
                    "Jumlah_Share_Post": r["Post_Shares"],
                    "Komentar": r["Comment_Content"],
                    "Balasan_Komentar": "",
                    "Jumlah_Like_Komentar": r["Comment_Likes"],
                    "Jumlah_Reply_Komentar": r["Comment_Replies_Count"]
                })

    # -----------------------------------------------------------------
    # B. SUMBER DATA: INSTAGRAM (JSON Cache)
    # -----------------------------------------------------------------
    elif source_type == 'instagram':
        cache_file = "instagram_data_cache.json"
        if not os.path.exists(cache_file):
            raise HTTPException(status_code=404, detail="Data Instagram belum disinkronisasi. Jalankan /sna/ingest")
            
        with open(cache_file, "r", encoding="utf-8") as f:
            posts = json.load(f)
            
        for p in posts:
            post_id = p.get('id')
            post_caption = p.get('caption', '').replace('\n', ' ')
            post_author = p.get('username', 'Suara_Surabaya_Official')
            permalink = p.get('permalink', '') # Ambil link post IG
            post_likes = p.get('like_count', 0)
            post_comments_count = p.get('comments_count', 0)
            
            # Post Utama
            dataset.append({
                "Interaction_Type": "POST",
                "Source_User": post_author,
                "Target": post_id,
                "Post_Link": permalink,
                "User_Pembuat_Post": post_author,
                "Post": post_caption,
                "Tanggal_Upload": p.get('timestamp', ''),
                "Jumlah_Like_Post": post_likes,
                "Jumlah_Views_Post": 0,
                "Jumlah_Comment_Post": post_comments_count,
                "Jumlah_Share_Post": 0,
                "Komentar": "",
                "Balasan_Komentar": "",
                "Jumlah_Like_Komentar": 0,
                "Jumlah_Reply_Komentar": 0
            })
            
            # Komentar & Reply
            for act in p.get('interactions', []):
                i_type = act.get('interaction_type', 'COMMENT')
                content = act.get('content', '').replace('\n', ' ')
                
                dataset.append({
                    "Interaction_Type": i_type,
                    "Source_User": act.get('source_username', 'Unknown'),
                    "Target": act.get('target_id', post_id),
                    "Post_Link": permalink,
                    "User_Pembuat_Post": post_author,
                    "Post": post_caption,
                    "Tanggal_Upload": act.get('timestamp', ''),
                    "Jumlah_Like_Post": post_likes,
                    "Jumlah_Views_Post": 0,
                    "Jumlah_Comment_Post": post_comments_count,
                    "Jumlah_Share_Post": 0,
                    "Komentar": content if i_type == 'COMMENT' else "",
                    "Balasan_Komentar": content if i_type == 'REPLY' else "",
                    "Jumlah_Like_Komentar": act.get('likes', 0),
                    "Jumlah_Reply_Komentar": 0
                })
    else:
        raise HTTPException(status_code=400, detail="source_type harus 'app' atau 'instagram'")

    # Konversi ke Pandas DataFrame lalu hapus duplikasi
    df = pd.DataFrame(dataset)
    df = df.drop_duplicates()

    if df.empty:
        return df

    # -----------------------------------------------------------------
    # C. FILTER TANGGAL (AMAT PENTING: PARSING WAKTU)
    # -----------------------------------------------------------------
    def parse_to_datetime(val):
        if pd.isna(val) or str(val).strip() == "":
            return pd.NaT
        try:
            # Jika ISO string (Instagram) spt: 2024-05-10T12:00:00+0000
            if isinstance(val, str) and 'T' in val:
                dt = pd.to_datetime(val)
                if dt.tzinfo is not None:
                    dt = dt.tz_localize(None) # Hapus timezone agar perbandingan valid
                return dt
            
            # Jika angka dari Firestore/Neo4j
            val_num = float(val)
            if val_num > 1e11:  # milidetik
                return pd.to_datetime(val_num, unit='ms')
            else:  # detik
                return pd.to_datetime(val_num, unit='s')
        except:
            return pd.NaT

    # 1. Konversi SEMUA data tanggal ke objek datetime yang terstandarisasi
    df['Datetime_Obj'] = df['Tanggal_Upload'].apply(parse_to_datetime)

    # 2. Lakukan Filter menggunakan Objek Waktu (Bukan String)
    if start_date:
        try:
            start_dt = pd.to_datetime(start_date)
            df = df[df['Datetime_Obj'] >= start_dt]
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Format start_date salah: {e}")

    if end_date:
        try:
            # Tambah 1 hari kurang 1 detik agar filter mencakup seluruh jam di hari terakhir
            end_dt = pd.to_datetime(end_date) + pd.Timedelta(days=1, seconds=-1)
            df = df[df['Datetime_Obj'] <= end_dt]
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Format end_date salah: {e}")

    # 3. Ubah kembali menjadi String rapi khusus untuk tampilan di Excel
    df['Tanggal_Upload'] = df['Datetime_Obj'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notnull(x) else "")
    
    # Hapus kolom bantuan
    df = df.drop(columns=['Datetime_Obj'])

    # -----------------------------------------------------------------
    # D. FILTER KOLOM (BERDASARKAN REQUEST USER)
    # -----------------------------------------------------------------
    if not export_all and selected_columns is not None:
        # Kunci kolom mandatory agar jika diimport lagi tidak error saat generate graph
        mandatory_columns = ['Interaction_Type', 'Source_User', 'Target', 'Post_Link', 'Post']
        final_columns = []
        
        # 1. Pastikan kolom mandatory ada di depan
        for col in mandatory_columns:
            if col in df.columns:
                final_columns.append(col)
                
        # 2. Tambahkan kolom pilihan user (hindari duplikasi)
        for col in selected_columns:
            if col in df.columns and col not in final_columns:
                final_columns.append(col)
                
        # 3. Potong DataFrame
        df = df[final_columns]

    return df

# =====================================================================
# 2. EXPORT EXCEL
# =====================================================================
async def export_to_excel(source_type: str, start_date: str = None, end_date: str = None, selected_columns: list = None, export_all: bool = True):
    df = get_master_dataframe(source_type, start_date, end_date, selected_columns, export_all)
    
    if df.empty:
        raise HTTPException(status_code=404, detail="Tidak ada data yang ditemukan pada rentang waktu tersebut.")
        
    stream = io.BytesIO()
    with pd.ExcelWriter(stream, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='SNA_Dataset')
        
    stream.seek(0)
    filename = f"SNA_Export_{source_type.upper()}.xlsx"
    return StreamingResponse(
        stream, 
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )

async def link_to_sheets(email: str, source_type: str, start_date: str = None, end_date: str = None, selected_columns: list = None, export_all: bool = True, existing_sheet_url: str = None):
    if source_type not in ['app', 'instagram']:
        raise HTTPException(status_code=400, detail="Pilihan sumber data (source_type) hanya boleh 'app' atau 'instagram'")

    gc = get_gspread_client()
    try:
        # Cek apakah user memberikan URL Sheet yang sudah ada
        if existing_sheet_url:
            try:
                # OPSI 2: Menggunakan Sheet yang sudah dibuat user
                sh = gc.open_by_url(existing_sheet_url)
                sheet_name = sh.title
            except gspread.exceptions.SpreadsheetNotFound:
                # Tangani error kosong dari gspread dan berikan instruksi jelas
                sa_email = config.FIREBASE_CREDENTIALS.get("client_email", "firebase-adminsdk-bko4f@kp-ss-a8e05.iam.gserviceaccount.com")
                raise HTTPException(
                    status_code=403, 
                    detail=f"Akses Ditolak! Anda belum membagikan Spreadsheet tersebut ke sistem. Buka Google Sheets Anda, klik 'Share/Bagikan', lalu tambahkan email ini sebagai Editor: {sa_email}"
                )
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"URL Spreadsheet tidak valid: {str(e)}")
        else:
            # OPSI 1: Buat file baru dari nol
            sheet_name = f"SNA_Dataset_{source_type.upper()}_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}"
            sh = gc.create(sheet_name)
            sh.share(email, perm_type='user', role='writer')
        
        # Ekstrak dan masukkan data
        df = get_master_dataframe(source_type, start_date, end_date, selected_columns, export_all)
        worksheet = sh.get_worksheet(0)
        
        if not df.empty:
            if existing_sheet_url:
                worksheet.clear() # Bersihkan isi sheet lama jika pakai opsi existing
            worksheet.update([df.columns.values.tolist()] + df.values.tolist())
        
        # Simpan metadata ke Firestore
        doc_data = {
            "sheet_id": sh.id,
            "sheet_url": sh.url,
            "sheet_name": sheet_name,
            "source_type": source_type,
            "shared_email": email,
            "created_at": datetime.datetime.now().isoformat()
        }
        
        _, doc_ref = db.collection('linked_sheets').add(doc_data)
        
        return {
            "status": "success", 
            "message": f"Data {source_type.upper()} berhasil ditautkan ke Spreadsheet.", 
            "data": {
                "id": doc_ref.id,
                "sheet_name": sheet_name,
                "source_type": source_type,
                "status": "Linked"
            }
        }
    except Exception as e:
        if isinstance(e, HTTPException): raise e
        raise HTTPException(status_code=500, detail=f"Gagal menautkan ke Sheets: {str(e)}")
async def get_all_linked_sheets():
    """Mengambil semua daftar spreadsheet untuk ditampilkan di List Frontend"""
    try:
        # Ambil dari Firestore, urutkan dari yang terbaru
        docs = db.collection('linked_sheets').order_by('created_at', direction='DESCENDING').stream()
        sheets = []
        for doc in docs:
            data = doc.to_dict()
            data['id'] = doc.id
            sheets.append(data)
            
        return {"status": "success", "data": sheets}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Gagal mengambil daftar sheets: {str(e)}")

async def sync_to_sheets(sheet_id: str, source_type: str, start_date: str = None, end_date: str = None, selected_columns: list = None, export_all: bool = True):
    gc = get_gspread_client()
    try:
        sh = gc.open_by_key(sheet_id)
        worksheet = sh.get_worksheet(0)
        worksheet.clear()
        
        df = get_master_dataframe(source_type, start_date, end_date, selected_columns, export_all)
        worksheet.update([df.columns.values.tolist()] + df.values.tolist())
        return {"status": "success", "message": "Data di Spreadsheet berhasil disinkronisasi!"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

async def unlink_sheets(doc_id: str):
    """Unlink (Hapus) berdasarkan Document ID dari Firestore"""
    gc = get_gspread_client()
    try:
        doc_ref = db.collection('linked_sheets').document(doc_id)
        doc = doc_ref.get()
        
        if not doc.exists:
            raise HTTPException(status_code=404, detail="Tautan sheet tidak ditemukan di database.")
            
        sheet_data = doc.to_dict()
        sheet_id = sheet_data.get("sheet_id")
        
        # Hapus file secara fisik dari Drive Service Account
        try:
            gc.del_spreadsheet(sheet_id)
        except Exception as e:
            print(f"Warning: File mungkin sudah terhapus manual di Drive: {e}")
            
        # Hapus referensi dari Firestore (Unlink)
        doc_ref.delete()
        
        return {"status": "success", "message": "Spreadsheet berhasil di-unlink."}
    except Exception as e:
        if isinstance(e, HTTPException): raise e
        raise HTTPException(status_code=500, detail=str(e))

async def unlink_all_sheets():
    """Menghapus SEMUA sheet yang ada di database (Clear All)"""
    gc = get_gspread_client()
    try:
        docs = db.collection('linked_sheets').stream()
        deleted_count = 0
        
        for doc in docs:
            sheet_data = doc.to_dict()
            sheet_id = sheet_data.get("sheet_id")
            
            # Hapus file fisiknya
            try:
                gc.del_spreadsheet(sheet_id)
            except Exception as e:
                pass # Abaikan jika file fisik sudah tidak ada
            
            # Hapus dari database
            db.collection('linked_sheets').document(doc.id).delete()
            deleted_count += 1
            
        return {"status": "success", "message": f"Berhasil menghapus (unlink) {deleted_count} sheets."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# =====================================================================
# 4. IMPORT & CALCULATE CENTRALITY (EXCEL / SHEETS)
# =====================================================================
def _calculate_graph_from_dataframe(df: pd.DataFrame):
    """Membaca format file buatan kita sendiri untuk membangun Graf dan Centrality"""
    G = nx.DiGraph()
    
    for index, row in df.iterrows():
        source = str(row.get('Source_User', '')).strip()
        target = str(row.get('Target', '')).strip()
        i_type = str(row.get('Interaction_Type', 'POST')).upper()
        
        if not source or source == 'nan': continue
            
        s_node = f"user_{source}"
        if not G.has_node(s_node):
            G.add_node(s_node, type="user", label=source)
            
        if i_type == 'POST':
            t_node = f"post_{target}"
            if not G.has_node(t_node):
                # NOTE: Menggunakan kolom 'Post' hasil request Anda
                G.add_node(t_node, type="post", label=str(row.get('Post', ''))[:20])
            G.add_edge(s_node, t_node, weight=5, type="AUTHORED")
            
        elif i_type in ['COMMENT', 'REPLY']:
            t_node = f"post_{target}" if i_type == 'COMMENT' else f"user_{target}"
            if not G.has_node(t_node):
                G.add_node(t_node, type="post" if i_type == 'COMMENT' else "user", label=target)
            
            if G.has_edge(s_node, t_node):
                G[s_node][t_node]['weight'] += 3
            else:
                G.add_edge(s_node, t_node, weight=3, type=i_type)

    G.remove_nodes_from(list(nx.isolates(G)))
    if G.number_of_nodes() == 0:
        raise HTTPException(status_code=400, detail="Format data kosong atau tidak valid.")

    for u, v, d in G.edges(data=True):
        d['distance'] = 1.0 / d['weight'] if d.get('weight', 1) > 0 else 1.0

    deg_cent = nx.degree_centrality(G)
    bet_cent = nx.betweenness_centrality(G, weight='distance')
    clo_cent = nx.closeness_centrality(G, distance='distance')
    
    try:
        eig_cent = nx.eigenvector_centrality(G, weight='weight', max_iter=1000)
    except:
        eig_cent = {n: 0.0 for n in G.nodes()}

    ig_G = ig.Graph.TupleList(G.edges(), directed=True)
    partition = la.find_partition(ig_G, la.ModularityVertexPartition, n_iterations=-1)
    community_map = {ig_G.vs[node.index]['name']: comm_id for comm_id, members in enumerate(partition) for node in members}

    nodes_out = []
    for n in G.nodes():
        nodes_out.append({
            "id": n,
            "label": G.nodes[n].get('label', n),
            "attributes": {"community": community_map.get(n, 0)},
            "metrics": {
                "degree": deg_cent.get(n, 0.0),
                "betweenness": bet_cent.get(n, 0.0),
                "closeness": clo_cent.get(n, 0.0),
                "eigenvector": eig_cent.get(n, 0.0)
            }
        })

    return {
        "meta": {"total_nodes": G.number_of_nodes(), "total_edges": G.number_of_edges()},
        "graph_data": {
            "nodes": nodes_out,
            "edges": [{"source": u, "target": v, "weight": d.get('weight', 1)} for u, v, d in G.edges(data=True)]
        }
    }

async def import_from_excel(file: UploadFile):
    try:
        content = await file.read()
        df = pd.read_excel(io.BytesIO(content))
        return _calculate_graph_from_dataframe(df)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Gagal memproses file Excel: {str(e)}")

async def import_from_sheets(sheet_id: str):
    gc = get_gspread_client()
    try:
        sh = gc.open_by_key(sheet_id)
        worksheet = sh.get_worksheet(0)
        data = worksheet.get_all_records()
        df = pd.DataFrame(data)
        return _calculate_graph_from_dataframe(df)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Gagal memproses Google Sheets: {str(e)}")