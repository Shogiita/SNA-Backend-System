import io
import json
import os
import asyncio
import tempfile
import datetime

import pandas as pd
import networkx as nx
import gspread

from google.oauth2.service_account import Credentials
from fastapi import HTTPException, Response, UploadFile
from fastapi.responses import StreamingResponse, FileResponse

from app.database import neo4j_driver, db
from app import config
from app.config import GOOGLE_CREDENTIALS
from app.utils.leiden_utils import apply_leiden_communities

from app.config import GOOGLE_CREDENTIALS

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


def get_master_dataframe(
    source_type: str,
    start_date: str = None,
    end_date: str = None,
    selected_columns: list = None,
    export_all: bool = True
) -> pd.DataFrame:
    dataset = []

    community_map = {}

    if source_type == 'app':
        query = """
        MATCH (author:FirebaseUser)-[:POSTED_FB]->(p)
        WHERE (p:FirebaseKawanSS OR p:FirebaseInfoss) AND (p.isDeleted = false OR p.isDeleted IS NULL)
        OPTIONAL MATCH (c_author:FirebaseUser)-[:WROTE_FB]->(c)-[:COMMENTED_ON_FB]->(p)
        RETURN 
            coalesce(author.id, author.username, author.nama) AS Post_Author_ID,
            coalesce(author.username, author.nama, author.id) AS Post_Author,
            coalesce(p.judul, p.title, p.deskripsi, 'No Content') AS Post_Content,
            coalesce(p.createdAt, p.uploadDate, '') AS Upload_Date,
            coalesce(toInteger(p.jumlahLike), 0) AS Post_Likes,
            coalesce(toInteger(p.jumlahView), 0) AS Post_Views,
            coalesce(toInteger(p.jumlahComment), 0) AS Post_Comments,
            coalesce(toInteger(p.jumlahShare), 0) AS Post_Shares,
            coalesce(c_author.id, c_author.username, c_author.nama) AS Comment_Author_ID,
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
            raise HTTPException(
                status_code=500,
                detail=f"Koneksi Neo4j terputus: {str(db_err)}"
            )

        G = nx.Graph()

        for r in records:
            post_author_id = str(r.get("Post_Author_ID") or r.get("Post_Author") or "").strip()
            comment_author_id = str(r.get("Comment_Author_ID") or r.get("Comment_Author") or "").strip()
            target_post_id = str(r.get("Target_Post_ID") or "").strip()

            post_node = f"post_{target_post_id}" if target_post_id else ""
            author_node = f"user_{post_author_id}" if post_author_id else ""
            comment_author_node = f"user_{comment_author_id}" if comment_author_id else ""

            if author_node and post_node:
                G.add_edge(author_node, post_node, weight=5)

            if comment_author_node and post_node:
                G.add_edge(comment_author_node, post_node, weight=3)

        if G.number_of_nodes() > 0:
            community_map = apply_leiden_communities(G, weight_attr="weight")

        for r in records:
            post_author_id = str(r.get("Post_Author_ID") or r.get("Post_Author") or "").strip()
            comment_author_id = str(r.get("Comment_Author_ID") or r.get("Comment_Author") or "").strip()
            target_post_id = str(r.get("Target_Post_ID") or "").strip()

            post_author_node = f"user_{post_author_id}" if post_author_id else ""
            comment_author_node = f"user_{comment_author_id}" if comment_author_id else ""
            post_node = f"post_{target_post_id}" if target_post_id else ""

            dataset.append({
                "Interaction_Type": "POST",
                "Community": community_map.get(post_author_node, community_map.get(post_node, "")),
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
                    "Community": community_map.get(comment_author_node, community_map.get(post_node, "")),
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
        query_ig = """
        MATCH (author:InstagramUser)-[:POSTED_IG]->(p:InstagramPost)
        OPTIONAL MATCH (c_author:InstagramUser)-[:WROTE_IG]->(c:InstagramComment)-[:COMMENTED_ON_IG]->(p)
        RETURN 
            coalesce(author.username, 'Suara_Surabaya_Official') AS Post_Author,
            coalesce(p.caption, 'No Content') AS Post_Content,
            coalesce(p.timestamp, '') AS Upload_Date,
            coalesce(p.permalink, '') AS Permalink,
            coalesce(toInteger(p.like_count), 0) AS Post_Likes,
            coalesce(toInteger(p.comments_count), 0) AS Post_Comments,
            coalesce(c_author.username, '') AS Comment_Author,
            coalesce(c.text, '') AS Comment_Content,
            coalesce(toInteger(c.likes), 0) AS Comment_Likes,
            p.id AS Target_Post_ID
        """

        try:
            with neo4j_driver.session() as session:
                records_ig = session.run(query_ig).data()
        except Exception as db_err:
            raise HTTPException(
                status_code=500,
                detail=f"Koneksi Neo4j terputus saat mengambil data Instagram: {str(db_err)}"
            )

        if not records_ig:
            raise HTTPException(
                status_code=404,
                detail="Data Instagram kosong di database Neo4j. Pastikan scheduler sinkronisasi sudah berjalan."
            )

        G = nx.Graph()

        for r in records_ig:
            post_author = str(r.get("Post_Author") or "").strip()
            comment_author = str(r.get("Comment_Author") or "").strip()
            target_post_id = str(r.get("Target_Post_ID") or "").strip()

            post_node = f"post_{target_post_id}" if target_post_id else ""
            author_node = f"user_{post_author}" if post_author else ""
            comment_author_node = f"user_{comment_author}" if comment_author else ""

            if author_node and post_node:
                G.add_edge(author_node, post_node, weight=5)

            if comment_author_node and post_node:
                G.add_edge(comment_author_node, post_node, weight=3)

        if G.number_of_nodes() > 0:
            community_map = apply_leiden_communities(G, weight_attr="weight")

        for r in records_ig:
            post_author = str(r.get("Post_Author") or "").strip()
            comment_author = str(r.get("Comment_Author") or "").strip()
            target_post_id = str(r.get("Target_Post_ID") or "").strip()

            post_author_node = f"user_{post_author}" if post_author else ""
            comment_author_node = f"user_{comment_author}" if comment_author else ""
            post_node = f"post_{target_post_id}" if target_post_id else ""

            dataset.append({
                "Interaction_Type": "POST",
                "Community": community_map.get(post_author_node, community_map.get(post_node, "")),
                "Source_User": r["Post_Author"],
                "Target": r["Target_Post_ID"],
                "Post_Link": r["Permalink"],
                "User_Pembuat_Post": r["Post_Author"],
                "Post": r["Post_Content"],
                "Tanggal_Upload": r["Upload_Date"],
                "Jumlah_Like_Post": r["Post_Likes"],
                "Jumlah_Views_Post": 0,
                "Jumlah_Comment_Post": r["Post_Comments"],
                "Jumlah_Share_Post": 0,
                "Komentar": "",
                "Balasan_Komentar": "",
                "Jumlah_Like_Komentar": 0,
                "Jumlah_Reply_Komentar": 0
            })

            if r["Comment_Author"] and r["Comment_Content"]:
                dataset.append({
                    "Interaction_Type": "COMMENT",
                    "Community": community_map.get(comment_author_node, community_map.get(post_node, "")),
                    "Source_User": r["Comment_Author"],
                    "Target": r["Target_Post_ID"],
                    "Post_Link": r["Permalink"],
                    "User_Pembuat_Post": r["Post_Author"],
                    "Post": r["Post_Content"],
                    "Tanggal_Upload": r["Upload_Date"],
                    "Jumlah_Like_Post": r["Post_Likes"],
                    "Jumlah_Views_Post": 0,
                    "Jumlah_Comment_Post": r["Post_Comments"],
                    "Jumlah_Share_Post": 0,
                    "Komentar": r["Comment_Content"],
                    "Balasan_Komentar": "",
                    "Jumlah_Like_Komentar": r["Comment_Likes"],
                    "Jumlah_Reply_Komentar": 0
                })

    else:
        raise HTTPException(
            status_code=400,
            detail="source_type harus 'app' atau 'instagram'"
        )

    df = pd.DataFrame(dataset).drop_duplicates()

    if df.empty:
        return df

    def parse_to_datetime(val):
        if pd.isna(val) or str(val).strip() == "":
            return pd.NaT

        try:
            if isinstance(val, str) and 'T' in val:
                dt = pd.to_datetime(val)

                if dt.tzinfo is not None:
                    dt = dt.tz_localize(None)

                return dt

            val_num = float(val)
            return pd.to_datetime(val_num, unit='ms') if val_num > 1e11 else pd.to_datetime(val_num, unit='s')
        except Exception:
            return pd.NaT

    df['Datetime_Obj'] = df['Tanggal_Upload'].apply(parse_to_datetime)

    if start_date:
        try:
            df = df[df['Datetime_Obj'] >= pd.to_datetime(start_date)]
        except Exception:
            pass

    if end_date:
        try:
            df = df[df['Datetime_Obj'] <= (pd.to_datetime(end_date) + pd.Timedelta(days=1, seconds=-1))]
        except Exception:
            pass

    df['Tanggal_Upload'] = df['Datetime_Obj'].apply(
        lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notnull(x) else ""
    )

    df = df.drop(columns=['Datetime_Obj'])

    if not export_all and selected_columns is not None:
        mandatory = [
            'Interaction_Type',
            'Community',
            'Source_User',
            'Target',
            'Post_Link',
            'Post'
        ]

        final_cols = [c for c in mandatory if c in df.columns]

        for c in selected_columns:
            if c in df.columns and c not in final_cols:
                final_cols.append(c)

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

async def unlink_sheets(doc_id: str):
    try:
        db.collection('linked_sheets').document(doc_id).delete()
        return {"status": "success", "message": "Tautan dihapus dari Dashboard."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))