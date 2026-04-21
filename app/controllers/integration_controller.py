import io
import json
import os
import asyncio
import tempfile
import pandas as pd
import networkx as nx
import leidenalg as la
import igraph as ig
import gspread
import datetime
from google.oauth2.service_account import Credentials
from fastapi import HTTPException, Response, UploadFile
from fastapi.responses import StreamingResponse, FileResponse 
from app.database import neo4j_driver, db
from app import config

# Import dictionary GOOGLE_CREDENTIALS dari config.py
from app.config import GOOGLE_CREDENTIALS

# =================================================================
# MASUKKAN ID SPREADSHEET DARI TAHAP 1 DI SINI
# =================================================================
MASTER_SHEET_ID = "MASUKKAN_ID_SPREADSHEET_DISINI"

def get_gspread_client():
    try:
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        # Menggunakan from_service_account_info untuk membaca dari memory/env
        creds = Credentials.from_service_account_info(GOOGLE_CREDENTIALS, scopes=scopes)
        return gspread.authorize(creds)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Gagal otentikasi Google Sheets: {str(e)}")

def get_master_dataframe(source_type: str, start_date: str = None, end_date: str = None, selected_columns: list = None, export_all: bool = True) -> pd.DataFrame:
    dataset = []

    if source_type == 'app':
        query = """
        MATCH (author:FirebaseUser)-[:POSTED_FB]->(p)
        WHERE (p:FirebaseKawanSS OR p:FirebaseInfoss) AND (p.isDeleted = false OR p.isDeleted IS NULL)
        OPTIONAL MATCH (c_author:FirebaseUser)-[:WROTE_FB]->(c)-[:COMMENTED_ON_FB]->(p)
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
        try:
            with neo4j_driver.session() as session:
                records = session.run(query).data()
        except Exception as db_err:
            raise HTTPException(status_code=500, detail=f"Koneksi Neo4j terputus: {str(db_err)}")
            
        for r in records:
            dataset.append({
                "Interaction_Type": "POST",
                "Source_User": r["Post_Author"],
                "Target": r["Target_Post_ID"],
                "Post_Link": "",
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

    elif source_type == 'instagram':
        cache_file = "instagram_data_cache.json"
        if not os.path.exists(cache_file):
            raise HTTPException(status_code=404, detail="Data Instagram belum disinkronisasi.")
            
        with open(cache_file, "r", encoding="utf-8") as f:
            posts = json.load(f)
            
        for p in posts:
            post_id = p.get('id')
            post_caption = p.get('caption', '').replace('\n', ' ')
            post_author = p.get('username', 'Suara_Surabaya_Official')
            permalink = p.get('permalink', '') 
            post_likes = p.get('like_count', 0)
            post_comments_count = p.get('comments_count', 0)
            
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

    df = pd.DataFrame(dataset).drop_duplicates()
    if df.empty: return df

    def parse_to_datetime(val):
        if pd.isna(val) or str(val).strip() == "": return pd.NaT
        try:
            if isinstance(val, str) and 'T' in val:
                dt = pd.to_datetime(val)
                if dt.tzinfo is not None: dt = dt.tz_localize(None) 
                return dt
            val_num = float(val)
            return pd.to_datetime(val_num, unit='ms') if val_num > 1e11 else pd.to_datetime(val_num, unit='s')
        except: return pd.NaT

    df['Datetime_Obj'] = df['Tanggal_Upload'].apply(parse_to_datetime)

    if start_date:
        try:
            df = df[df['Datetime_Obj'] >= pd.to_datetime(start_date)]
        except: pass
    if end_date:
        try:
            df = df[df['Datetime_Obj'] <= (pd.to_datetime(end_date) + pd.Timedelta(days=1, seconds=-1))]
        except: pass

    df['Tanggal_Upload'] = df['Datetime_Obj'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notnull(x) else "")
    df = df.drop(columns=['Datetime_Obj'])

    if not export_all and selected_columns is not None:
        mandatory = ['Interaction_Type', 'Source_User', 'Target', 'Post_Link', 'Post']
        final_cols = [c for c in mandatory if c in df.columns]
        for c in selected_columns:
            if c in df.columns and c not in final_cols: final_cols.append(c)
        df = df[final_cols]

    return df

async def export_to_csv(source_type: str, start_date: str = None, end_date: str = None, selected_columns: list = None, export_all: bool = True):
    try:
        df = await asyncio.to_thread(get_master_dataframe, source_type, start_date, end_date, selected_columns, export_all)
        if df is None or df.empty:
            raise HTTPException(status_code=404, detail="Tidak ada data ditemukan.")
        
        fd, temp_path = tempfile.mkstemp(suffix=".csv")
        await asyncio.to_thread(df.to_csv, temp_path, index=False, encoding='utf-8-sig')
        os.close(fd)
        
        return FileResponse(path=temp_path, filename=f"SNA_Dataset_{source_type.upper()}.csv", media_type="text/csv")
    except Exception as e:
        if isinstance(e, HTTPException): raise e
        raise HTTPException(status_code=500, detail=str(e))

# =====================================================================
# SPREADSHEET LINKING (BINDING MULTIPLE SPREADSHEETS)
# =====================================================================
async def link_to_sheets(existing_sheet_url: str, source_type: str, start_date: str = None, end_date: str = None, selected_columns: list = None, export_all: bool = True):
    if not existing_sheet_url:
        raise HTTPException(status_code=400, detail="URL Spreadsheet wajib diisi.")
        
    if source_type not in ['app', 'instagram']:
        raise HTTPException(status_code=400, detail="Pilihan sumber data hanya 'app' atau 'instagram'")

    try:
        df = await asyncio.to_thread(get_master_dataframe, source_type, start_date, end_date, selected_columns, export_all)
        if df.empty:
            raise ValueError("Tidak ada data untuk diekspor pada rentang waktu tersebut.")

        def _gspread_operations():
            gc = get_gspread_client()
            try:
                # Membuka spreadsheet berdasarkan URL yang di-paste user
                sh = gc.open_by_url(existing_sheet_url)
            except Exception:
                # Mengambil email langsung dari dictionary GOOGLE_CREDENTIALS di memory
                sa_email = GOOGLE_CREDENTIALS.get("client_email", "Email Service Account tidak ditemukan")
                raise ValueError(f"Akses Ditolak! Pastikan Anda sudah memberikan akses 'Editor' pada Spreadsheet Anda ke email: {sa_email}")
            
            tab_name = "DATA_APP" if source_type == 'app' else "DATA_IG"
            
            # Cari tab-nya. Jika tidak ada, buatkan tab baru di dalam Spreadsheet milik user.
            try:
                worksheet = sh.worksheet(tab_name)
            except gspread.exceptions.WorksheetNotFound:
                worksheet = sh.add_worksheet(title=tab_name, rows=1, cols=1)

            worksheet.clear()
            safe_df = df.fillna("").astype(str)
            worksheet.update([safe_df.columns.tolist()] + safe_df.values.tolist())
            
            return sh.id, sh.url, sh.title

        sh_id, sh_url, s_name = await asyncio.to_thread(_gspread_operations)
        
        doc_data = {
            "sheet_id": sh_id,
            "sheet_url": sh_url,
            "sheet_name": f"{s_name} ({source_type.upper()})",
            "source_type": source_type,
            "created_at": datetime.datetime.now().isoformat()
        }
        
        _, doc_ref = db.collection('linked_sheets').add(doc_data)
        
        return {
            "status": "success", 
            "message": f"Data berhasil ditautkan ke Spreadsheet Anda.", 
            "data": {"id": doc_ref.id, "sheet_url": sh_url}
        }
        
    except ValueError as ve:
        raise HTTPException(status_code=403, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

async def get_all_linked_sheets():
    try:
        docs = db.collection('linked_sheets').order_by('created_at', direction='DESCENDING').stream()
        return {"status": "success", "data": [{"id": d.id, **d.to_dict()} for d in docs]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

async def sync_to_sheets(sheet_id: str, source_type: str, start_date: str = None, end_date: str = None, selected_columns: list = None, export_all: bool = True):
    try:
        df = await asyncio.to_thread(get_master_dataframe, source_type, start_date, end_date, selected_columns, export_all)
        def _sync():
            gc = get_gspread_client()
            sh = gc.open_by_key(MASTER_SHEET_ID)
            tab_name = "DATA_APP" if source_type == 'app' else "DATA_IG"
            ws = sh.worksheet(tab_name)
            ws.clear()
            if not df.empty:
                safe_df = df.fillna("").astype(str)
                ws.update([safe_df.columns.tolist()] + safe_df.values.tolist())
        await asyncio.to_thread(_sync)
        return {"status": "success", "message": "Sinkronisasi berhasil!"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

async def unlink_sheets(doc_id: str):
    try:
        db.collection('linked_sheets').document(doc_id).delete()
        return {"status": "success", "message": "Tautan dihapus dari Dashboard."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

async def unlink_all_sheets():
    try:
        docs = db.collection('linked_sheets').stream()
        for doc in docs: db.collection('linked_sheets').document(doc.id).delete()
        return {"status": "success", "message": "Semua tautan dihapus."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def _calculate_graph_from_dataframe(df: pd.DataFrame):
    G = nx.DiGraph()
    for _, row in df.iterrows():
        source = str(row.get('Source_User', '')).strip()
        target = str(row.get('Target', '')).strip()
        i_type = str(row.get('Interaction_Type', 'POST')).upper()
        if not source or source == 'nan': continue
        s_node = f"user_{source}"
        if not G.has_node(s_node): G.add_node(s_node, type="user", label=source)
        if i_type == 'POST':
            t_node = f"post_{target}"
            if not G.has_node(t_node): G.add_node(t_node, type="post", label=str(row.get('Post', ''))[:20])
            G.add_edge(s_node, t_node, weight=5, type="AUTHORED")
        elif i_type in ['COMMENT', 'REPLY']:
            t_node = f"post_{target}" if i_type == 'COMMENT' else f"user_{target}"
            if not G.has_node(t_node): G.add_node(t_node, type="post" if i_type == 'COMMENT' else "user", label=target)
            if G.has_edge(s_node, t_node): G[s_node][t_node]['weight'] += 3
            else: G.add_edge(s_node, t_node, weight=3, type=i_type)

    G.remove_nodes_from(list(nx.isolates(G)))
    if G.number_of_nodes() == 0: raise HTTPException(status_code=400, detail="Data kosong.")
    for u, v, d in G.edges(data=True): d['distance'] = 1.0 / d['weight'] if d.get('weight', 1) > 0 else 1.0
    deg_cent = nx.degree_centrality(G)
    bet_cent = nx.betweenness_centrality(G, weight='distance')
    clo_cent = nx.closeness_centrality(G, distance='distance')
    try: eig_cent = nx.eigenvector_centrality(G, weight='weight', max_iter=1000)
    except: eig_cent = {n: 0.0 for n in G.nodes()}

    ig_G = ig.Graph.TupleList(G.edges(), directed=True)
    partition = la.find_partition(ig_G, la.ModularityVertexPartition, n_iterations=-1)
    comm_map = {ig_G.vs[node.index]['name']: c_id for c_id, members in enumerate(partition) for node in members}

    nodes_out = [{"id": n, "label": G.nodes[n].get('label', n), "attributes": {"community": comm_map.get(n, 0)}, "metrics": {"degree": deg_cent.get(n, 0.0), "betweenness": bet_cent.get(n, 0.0), "closeness": clo_cent.get(n, 0.0), "eigenvector": eig_cent.get(n, 0.0)}} for n in G.nodes()]
    return {"meta": {"total_nodes": G.number_of_nodes(), "total_edges": G.number_of_edges()}, "graph_data": {"nodes": nodes_out, "edges": [{"source": u, "target": v, "weight": d.get('weight', 1)} for u, v, d in G.edges(data=True)]}}

async def import_from_excel(file: UploadFile):
    try:
        content = await file.read()
        df = await asyncio.to_thread(pd.read_excel, io.BytesIO(content))
        return await asyncio.to_thread(_calculate_graph_from_dataframe, df)
    except Exception as e: raise HTTPException(status_code=400, detail=str(e))

async def import_from_sheets(sheet_id: str):
    try:
        def _fetch_sheet():
            gc = get_gspread_client()
            sh = gc.open_by_key(sheet_id)
            return sh.get_worksheet(0).get_all_records()
        df = pd.DataFrame(await asyncio.to_thread(_fetch_sheet))
        return await asyncio.to_thread(_calculate_graph_from_dataframe, df)
    except Exception as e: raise HTTPException(status_code=400, detail=str(e))