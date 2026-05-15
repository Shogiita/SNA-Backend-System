import os
import re
import asyncio
import tempfile
import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
import networkx as nx
import gspread

from google.oauth2.service_account import Credentials
from google.oauth2.credentials import Credentials as UserCredentials

from fastapi import HTTPException
from fastapi.responses import FileResponse, StreamingResponse

from app.database import neo4j_driver, db
from app.config import GOOGLE_CREDENTIALS
from app.utils.leiden_utils import apply_leiden_communities
from app.controllers import report_controller

GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

MANDATORY_COLUMNS = {
    "instagram": [
        "interaction_type",
        "source_user",
        "target",
        "post_link",
        "user_pembuat_post",
        "post",
        "tanggal_upload",
    ],
    "app": [
        "interaction_type",
        "source_user",
        "target",
        "user_pembuat_post",
        "post",
        "tanggal_upload",
    ],
}


EXTRA_COLUMN_MAP = {
    "likes": "likes",
    "like_count": "like_count",
    "comment": "comment",
    "reply": "reply",
    "hashtag": "hashtag",
    "comment_count": "comment_count",
    "media_type": "media_type",
    "post_id": "post_id",
    "user_id": "user_id",
    "community": "community",
    "jumlah_like_post": "jumlah_like_post",
    "jumlah_views_post": "jumlah_views_post",
    "jumlah_comment_post": "jumlah_comment_post",
    "jumlah_share_post": "jumlah_share_post",
    "jumlah_like_komentar": "jumlah_like_komentar",
    "jumlah_reply_komentar": "jumlah_reply_komentar",
}


LEGACY_COLUMN_TO_NORMALIZED = {
    "Interaction_Type": "interaction_type",
    "Community": "community",
    "Source_User": "source_user",
    "Target": "target",
    "Post_Link": "post_link",
    "User_Pembuat_Post": "user_pembuat_post",
    "Post": "post",
    "Tanggal_Upload": "tanggal_upload",
    "Jumlah_Like_Post": "jumlah_like_post",
    "Jumlah_Views_Post": "jumlah_views_post",
    "Jumlah_Comment_Post": "jumlah_comment_post",
    "Jumlah_Share_Post": "jumlah_share_post",
    "Komentar": "comment",
    "Balasan_Komentar": "reply",
    "Jumlah_Like_Komentar": "jumlah_like_komentar",
    "Jumlah_Reply_Komentar": "jumlah_reply_komentar",
}


NORMALIZED_TO_LEGACY = {
    "interaction_type": "Interaction_Type",
    "community": "Community",
    "source_user": "Source_User",
    "target": "Target",
    "post_link": "Post_Link",
    "user_pembuat_post": "User_Pembuat_Post",
    "post": "Post",
    "tanggal_upload": "Tanggal_Upload",
    "jumlah_like_post": "Jumlah_Like_Post",
    "jumlah_views_post": "Jumlah_Views_Post",
    "jumlah_comment_post": "Jumlah_Comment_Post",
    "jumlah_share_post": "Jumlah_Share_Post",
    "comment": "Komentar",
    "reply": "Balasan_Komentar",
    "jumlah_like_komentar": "Jumlah_Like_Komentar",
    "jumlah_reply_komentar": "Jumlah_Reply_Komentar",
    "likes": "Jumlah_Like_Post",
    "like_count": "Jumlah_Like_Post",
    "comment_count": "Jumlah_Comment_Post",
}

def _extract_spreadsheet_id(spreadsheet_id: Optional[str], spreadsheet_url: Optional[str]) -> str:
    if spreadsheet_id:
        return spreadsheet_id.strip()

    if not spreadsheet_url:
        raise HTTPException(
            status_code=400,
            detail="spreadsheet_id atau spreadsheet_url wajib diisi."
        )

    match = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", spreadsheet_url)

    if not match:
        raise HTTPException(
            status_code=400,
            detail="URL Google Spreadsheet tidak valid."
        )

    return match.group(1)

def import_sheets(payload, current_admin: dict):
    """
    Import data dari Google Spreadsheet.

    Catatan:
    Function ini membaca spreadsheet lalu mengembalikan data normalized.
    Penyimpanan ke Neo4j bisa dibuat tahap berikutnya setelah format kolom final disepakati.
    """
    source_type = payload.source
    spreadsheet_id = _extract_spreadsheet_id(
        payload.spreadsheet_id,
        payload.spreadsheet_url,
    )

    client = get_gspread_user_client(payload.google_access_token)

    try:
        spreadsheet = client.open_by_key(spreadsheet_id)

        if payload.worksheet_name:
            worksheet = spreadsheet.worksheet(payload.worksheet_name)
        else:
            worksheet = spreadsheet.sheet1

        records = worksheet.get_all_records()

        df = pd.DataFrame(records)

        if df.empty:
            return {
                "status": "success",
                "source_active": source_type,
                "message": "Spreadsheet kosong.",
                "total_rows": 0,
                "columns": [],
                "data_preview": [],
            }

        normalized_df = _convert_legacy_df_to_normalized(df, source_type)

        return {
            "status": "success",
            "source_active": source_type,
            "spreadsheet": {
                "id": spreadsheet_id,
                "title": spreadsheet.title,
                "worksheet": worksheet.title,
            },
            "total_rows": len(normalized_df),
            "columns": list(normalized_df.columns),
            "data_preview": normalized_df.head(20).fillna("").to_dict(orient="records"),
            "note": (
                "Data berhasil dibaca dan dinormalisasi. "
                "Tahap berikutnya dapat diarahkan ke penyimpanan Neo4j jika diperlukan."
            )
        }

    except HTTPException:
        raise
    except Exception as error:
        raise HTTPException(
            status_code=500,
            detail=f"Gagal import Google Spreadsheet: {str(error)}"
        )

def get_gspread_client():
    try:
        creds = Credentials.from_service_account_info(
            GOOGLE_CREDENTIALS,
            scopes=GOOGLE_SCOPES,
        )
        return gspread.authorize(creds)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Gagal otentikasi Google Sheets: {str(e)}",
        )

def get_gspread_user_client(google_access_token: str):
    if not google_access_token:
        raise HTTPException(
            status_code=401,
            detail="Google Access Token tidak ditemukan. Silakan logout lalu login ulang dengan Google.",
        )

    try:
        creds = UserCredentials(
            token=google_access_token,
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive.file",
            ],
        )

        return gspread.authorize(creds)

    except Exception as e:
        error_message = str(e)

        if (
            "refresh" in error_message.lower()
            or "refresh_token" in error_message.lower()
            or "access token" in error_message.lower()
        ):
            raise HTTPException(
                status_code=401,
                detail=(
                    "Google Access Token sudah expired atau tidak bisa diperbarui. "
                    "Silakan logout lalu login ulang dengan Google dan izinkan akses Drive/Sheets."
                ),
            )

        raise HTTPException(
            status_code=401,
            detail=f"Gagal otentikasi Google user: {error_message}",
        )

def _parse_to_datetime(value):
    if pd.isna(value) or str(value).strip() == "":
        return pd.NaT

    try:
        value_str = str(value).strip()

        if "T" in value_str or "-" in value_str or "/" in value_str:
            dt = pd.to_datetime(value_str, errors="coerce")
            if pd.isna(dt):
                return pd.NaT

            if getattr(dt, "tzinfo", None) is not None:
                dt = dt.tz_localize(None)

            return dt

        value_num = float(value_str)
        if value_num > 1e11:
            return pd.to_datetime(value_num, unit="ms", errors="coerce")
        return pd.to_datetime(value_num, unit="s", errors="coerce")
    except Exception:
        return pd.NaT

def _apply_date_filter(
    df: pd.DataFrame,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    if df.empty or "Tanggal_Upload" not in df.columns:
        return df

    df = df.copy()
    df["Datetime_Obj"] = df["Tanggal_Upload"].apply(_parse_to_datetime)

    if start_date:
        try:
            df = df[df["Datetime_Obj"] >= pd.to_datetime(start_date)]
        except Exception:
            pass

    if end_date:
        try:
            df = df[
                df["Datetime_Obj"]
                <= (pd.to_datetime(end_date) + pd.Timedelta(days=1, seconds=-1))
            ]
        except Exception:
            pass

    df["Tanggal_Upload"] = df["Datetime_Obj"].apply(
        lambda x: x.strftime("%Y-%m-%d %H:%M:%S") if pd.notnull(x) else ""
    )

    return df.drop(columns=["Datetime_Obj"], errors="ignore")

def _normalize_frontend_columns(selected_columns: Optional[List[str]]) -> List[str]:
    normalized = []

    for col in selected_columns or []:
        clean = str(col).strip()

        if clean in LEGACY_COLUMN_TO_NORMALIZED:
            clean = LEGACY_COLUMN_TO_NORMALIZED[clean]

        clean = clean.lower()

        if clean in EXTRA_COLUMN_MAP:
            mapped = EXTRA_COLUMN_MAP[clean]
            if mapped not in normalized:
                normalized.append(mapped)

    return normalized

def _convert_legacy_df_to_normalized(df: pd.DataFrame, source_type: str) -> pd.DataFrame:
    if df.empty:
        return df

    result = pd.DataFrame()

    for legacy_col, normalized_col in LEGACY_COLUMN_TO_NORMALIZED.items():
        if legacy_col in df.columns:
            result[normalized_col] = df[legacy_col]

    result["likes"] = result.get("jumlah_like_post", 0)
    result["like_count"] = result.get("jumlah_like_post", 0)
    result["comment_count"] = result.get("jumlah_comment_post", 0)

    if "hashtag" not in result.columns:
        result["hashtag"] = ""

    if "media_type" not in result.columns:
        result["media_type"] = ""

    if "post_id" not in result.columns:
        result["post_id"] = result.get("target", "")

    if "user_id" not in result.columns:
        result["user_id"] = result.get("source_user", "")

    if source_type == "app" and "post_link" in result.columns:
        result = result.drop(columns=["post_link"], errors="ignore")

    return result

def _select_export_columns(
    df: pd.DataFrame,
    source_type: str,
    selected_columns: Optional[List[str]] = None,
    export_all: bool = True,
) -> pd.DataFrame:
    mandatory = MANDATORY_COLUMNS[source_type].copy()

    if export_all:
        extras = list(EXTRA_COLUMN_MAP.values())
    else:
        extras = _normalize_frontend_columns(selected_columns)

    final_columns = []

    for col in mandatory + extras:
        if source_type == "app" and col == "post_link":
            continue

        if col not in final_columns:
            final_columns.append(col)

    for col in final_columns:
        if col not in df.columns:
            df[col] = ""

    return df[final_columns]

def _make_legacy_export_dataframe(
    source_type: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    dataset = []
    community_map = {}

    if source_type == "app":
        query = """
        MATCH (author:FirebaseUser)-[:POSTED_FB]->(p)
        WHERE (p:FirebaseKawanSS OR p:FirebaseInfoss)
          AND (p.isDeleted = false OR p.isDeleted IS NULL)
        OPTIONAL MATCH (c_author:FirebaseUser)-[:WROTE_FB]->(c)-[:COMMENTED_ON_FB]->(p)
        RETURN
            coalesce(author.id, author.username, author.nama) AS Post_Author_ID,
            coalesce(author.username, author.nama, author.id) AS Post_Author,
            coalesce(p.judul, p.title, p.deskripsi, p.content, 'No Content') AS Post_Content,
            coalesce(p.createdAt, p.uploadDate, '') AS Upload_Date,
            coalesce(toInteger(p.jumlahLike), 0) AS Post_Likes,
            coalesce(toInteger(p.jumlahView), 0) AS Post_Views,
            coalesce(toInteger(p.jumlahComment), 0) AS Post_Comments,
            coalesce(toInteger(p.jumlahShare), 0) AS Post_Shares,
            coalesce(c_author.id, c_author.username, c_author.nama) AS Comment_Author_ID,
            coalesce(c_author.username, c_author.nama, c_author.id) AS Comment_Author,
            coalesce(c.text, c.komentar, c.content, '') AS Comment_Content,
            coalesce(toInteger(c.likes), 0) AS Comment_Likes,
            0 AS Comment_Replies_Count,
            coalesce(p.id, elementId(p)) AS Target_Post_ID
        """

        try:
            with neo4j_driver.session() as session:
                records = session.run(query).data()
        except Exception as db_err:
            raise HTTPException(
                status_code=500,
                detail=f"Koneksi Neo4j terputus: {str(db_err)}",
            )

        graph = nx.Graph()

        for row in records:
            post_author_id = str(
                row.get("Post_Author_ID") or row.get("Post_Author") or ""
            ).strip()
            comment_author_id = str(
                row.get("Comment_Author_ID") or row.get("Comment_Author") or ""
            ).strip()
            target_post_id = str(row.get("Target_Post_ID") or "").strip()

            post_node = f"post_{target_post_id}" if target_post_id else ""
            author_node = f"user_{post_author_id}" if post_author_id else ""
            comment_author_node = (
                f"user_{comment_author_id}" if comment_author_id else ""
            )

            if author_node and post_node:
                graph.add_edge(author_node, post_node, weight=5)

            if comment_author_node and post_node:
                graph.add_edge(comment_author_node, post_node, weight=3)

        if graph.number_of_nodes() > 0:
            community_map = apply_leiden_communities(graph, weight_attr="weight")

        for row in records:
            post_author_id = str(
                row.get("Post_Author_ID") or row.get("Post_Author") or ""
            ).strip()
            comment_author_id = str(
                row.get("Comment_Author_ID") or row.get("Comment_Author") or ""
            ).strip()
            target_post_id = str(row.get("Target_Post_ID") or "").strip()

            post_author_node = f"user_{post_author_id}" if post_author_id else ""
            comment_author_node = (
                f"user_{comment_author_id}" if comment_author_id else ""
            )
            post_node = f"post_{target_post_id}" if target_post_id else ""

            dataset.append(
                {
                    "Interaction_Type": "POST",
                    "Community": community_map.get(
                        post_author_node, community_map.get(post_node, "")
                    ),
                    "Source_User": row.get("Post_Author", ""),
                    "Target": row.get("Target_Post_ID", ""),
                    "Post_Link": "",
                    "User_Pembuat_Post": row.get("Post_Author", ""),
                    "Post": row.get("Post_Content", ""),
                    "Tanggal_Upload": row.get("Upload_Date", ""),
                    "Jumlah_Like_Post": row.get("Post_Likes", 0),
                    "Jumlah_Views_Post": row.get("Post_Views", 0),
                    "Jumlah_Comment_Post": row.get("Post_Comments", 0),
                    "Jumlah_Share_Post": row.get("Post_Shares", 0),
                    "Komentar": "",
                    "Balasan_Komentar": "",
                    "Jumlah_Like_Komentar": 0,
                    "Jumlah_Reply_Komentar": 0,
                }
            )

            if row.get("Comment_Author") and row.get("Comment_Content"):
                dataset.append(
                    {
                        "Interaction_Type": "COMMENT",
                        "Community": community_map.get(
                            comment_author_node, community_map.get(post_node, "")
                        ),
                        "Source_User": row.get("Comment_Author", ""),
                        "Target": row.get("Target_Post_ID", ""),
                        "Post_Link": "",
                        "User_Pembuat_Post": row.get("Post_Author", ""),
                        "Post": row.get("Post_Content", ""),
                        "Tanggal_Upload": row.get("Upload_Date", ""),
                        "Jumlah_Like_Post": row.get("Post_Likes", 0),
                        "Jumlah_Views_Post": row.get("Post_Views", 0),
                        "Jumlah_Comment_Post": row.get("Post_Comments", 0),
                        "Jumlah_Share_Post": row.get("Post_Shares", 0),
                        "Komentar": row.get("Comment_Content", ""),
                        "Balasan_Komentar": "",
                        "Jumlah_Like_Komentar": row.get("Comment_Likes", 0),
                        "Jumlah_Reply_Komentar": row.get(
                            "Comment_Replies_Count", 0
                        ),
                    }
                )

    elif source_type == "instagram":
        query = """
        MATCH (author:InstagramUser)-[:POSTED_IG]->(p:InstagramPost)
        OPTIONAL MATCH (c_author:InstagramUser)-[:WROTE_IG]->(c:InstagramComment)-[:COMMENTED_ON_IG]->(p)
        RETURN
            coalesce(author.username, author.name, 'Suara_Surabaya_Official') AS Post_Author,
            coalesce(p.caption, p.text, p.title, 'No Content') AS Post_Content,
            coalesce(p.timestamp, p.created_at, p.uploadDate, '') AS Upload_Date,
            coalesce(p.permalink, p.post_link, p.url, p.media_url, '') AS Permalink,
            coalesce(toInteger(p.like_count), toInteger(p.likes), 0) AS Post_Likes,
            coalesce(toInteger(p.comments_count), toInteger(p.comment_count), 0) AS Post_Comments,
            coalesce(c_author.username, c_author.name, '') AS Comment_Author,
            coalesce(c.text, c.comment, c.message, '') AS Comment_Content,
            coalesce(toInteger(c.likes), toInteger(c.like_count), 0) AS Comment_Likes,
            coalesce(p.id, p.post_id, elementId(p)) AS Target_Post_ID
        """

        try:
            with neo4j_driver.session() as session:
                records = session.run(query).data()
        except Exception as db_err:
            raise HTTPException(
                status_code=500,
                detail=f"Koneksi Neo4j terputus saat mengambil data Instagram: {str(db_err)}",
            )

        if not records:
            raise HTTPException(
                status_code=404,
                detail="Data Instagram kosong di database Neo4j. Pastikan migrasi/sinkronisasi Instagram sudah berjalan.",
            )

        graph = nx.Graph()

        for row in records:
            post_author = str(row.get("Post_Author") or "").strip()
            comment_author = str(row.get("Comment_Author") or "").strip()
            target_post_id = str(row.get("Target_Post_ID") or "").strip()

            post_node = f"post_{target_post_id}" if target_post_id else ""
            author_node = f"user_{post_author}" if post_author else ""
            comment_author_node = (
                f"user_{comment_author}" if comment_author else ""
            )

            if author_node and post_node:
                graph.add_edge(author_node, post_node, weight=5)

            if comment_author_node and post_node:
                graph.add_edge(comment_author_node, post_node, weight=3)

        if graph.number_of_nodes() > 0:
            community_map = apply_leiden_communities(graph, weight_attr="weight")

        for row in records:
            post_author = str(row.get("Post_Author") or "").strip()
            comment_author = str(row.get("Comment_Author") or "").strip()
            target_post_id = str(row.get("Target_Post_ID") or "").strip()

            post_author_node = f"user_{post_author}" if post_author else ""
            comment_author_node = (
                f"user_{comment_author}" if comment_author else ""
            )
            post_node = f"post_{target_post_id}" if target_post_id else ""

            dataset.append(
                {
                    "Interaction_Type": "POST",
                    "Community": community_map.get(
                        post_author_node, community_map.get(post_node, "")
                    ),
                    "Source_User": row.get("Post_Author", ""),
                    "Target": row.get("Target_Post_ID", ""),
                    "Post_Link": row.get("Permalink", ""),
                    "User_Pembuat_Post": row.get("Post_Author", ""),
                    "Post": row.get("Post_Content", ""),
                    "Tanggal_Upload": row.get("Upload_Date", ""),
                    "Jumlah_Like_Post": row.get("Post_Likes", 0),
                    "Jumlah_Views_Post": 0,
                    "Jumlah_Comment_Post": row.get("Post_Comments", 0),
                    "Jumlah_Share_Post": 0,
                    "Komentar": "",
                    "Balasan_Komentar": "",
                    "Jumlah_Like_Komentar": 0,
                    "Jumlah_Reply_Komentar": 0,
                }
            )

            if row.get("Comment_Author") and row.get("Comment_Content"):
                dataset.append(
                    {
                        "Interaction_Type": "COMMENT",
                        "Community": community_map.get(
                            comment_author_node, community_map.get(post_node, "")
                        ),
                        "Source_User": row.get("Comment_Author", ""),
                        "Target": row.get("Target_Post_ID", ""),
                        "Post_Link": row.get("Permalink", ""),
                        "User_Pembuat_Post": row.get("Post_Author", ""),
                        "Post": row.get("Post_Content", ""),
                        "Tanggal_Upload": row.get("Upload_Date", ""),
                        "Jumlah_Like_Post": row.get("Post_Likes", 0),
                        "Jumlah_Views_Post": 0,
                        "Jumlah_Comment_Post": row.get("Post_Comments", 0),
                        "Jumlah_Share_Post": 0,
                        "Komentar": row.get("Comment_Content", ""),
                        "Balasan_Komentar": "",
                        "Jumlah_Like_Komentar": row.get("Comment_Likes", 0),
                        "Jumlah_Reply_Komentar": 0,
                    }
                )

    else:
        raise HTTPException(
            status_code=400,
            detail="source_type harus 'app' atau 'instagram'",
        )

    df = pd.DataFrame(dataset).drop_duplicates()

    if df.empty:
        return df

    return _apply_date_filter(df, start_date, end_date)

def get_master_dataframe(
    source_type: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    selected_columns: Optional[List[str]] = None,
    export_all: bool = True,
) -> pd.DataFrame:
    legacy_df = _make_legacy_export_dataframe(
        source_type=source_type,
        start_date=start_date,
        end_date=end_date,
    )

    if legacy_df.empty:
        return legacy_df

    if not export_all and selected_columns is not None:
        mandatory = [
            "Interaction_Type",
            "Community",
            "Source_User",
            "Target",
            "Post_Link",
            "User_Pembuat_Post",
            "Post",
            "Tanggal_Upload",
        ]

        if source_type == "app":
            mandatory = [col for col in mandatory if col != "Post_Link"]

        final_cols = [col for col in mandatory if col in legacy_df.columns]

        for col in selected_columns:
            if col in legacy_df.columns and col not in final_cols:
                final_cols.append(col)

        legacy_df = legacy_df[final_cols]

    return legacy_df

def _safe_get_nested(data: dict, keys: list, default=0):
    try:
        current = data

        for key in keys:
            if not isinstance(current, dict):
                return default

            current = current.get(key)

            if current is None:
                return default

        return current
    except Exception:
        return default

def _normalize_export_value(value):
    if value is None:
        return ""

    if isinstance(value, (dict, list)):
        return str(value)

    return value

def _get_export_summary_rows(payload) -> list[list]:
    """
    Membuat baris ringkasan yang akan ditaruh di atas export CSV
    atau pada worksheet Summary di Google Sheets.

    Data berasal dari:
    1. /report/dashboard/stats
    2. /report/dashboard/google-analytics
    """

    start_date = getattr(payload, "start_date", None)
    end_date = getattr(payload, "end_date", None)
    source = getattr(payload, "source", "app")

    stats_response = report_controller.get_stats_summary()
    stats_data = stats_response.get("data", {}) if isinstance(stats_response, dict) else {}

    ga_response = report_controller.get_google_analytics_summary(
        start_date=start_date,
        end_date=end_date,
    )
    ga_data = ga_response.get("data", {}) if isinstance(ga_response, dict) else {}
    google_analytics = ga_data.get("google_analytics", {})
    ga_summary = google_analytics.get("summary", {})
    ga_date_range = google_analytics.get("date_range", {})

    rows = [
        ["Section", "Metric", "Value"],
        ["Export Info", "Source", str(source).upper()],
        ["Export Info", "Start Date", start_date or ga_date_range.get("start_date", "")],
        ["Export Info", "End Date", end_date or ga_date_range.get("end_date", "")],
        ["", "", ""],

        ["App Summary", "Total Pengguna App", _safe_get_nested(stats_data, ["users", "total"], 0)],
        ["App Summary", "User Baru Bulan Ini", _safe_get_nested(stats_data, ["users", "new_this_month"], 0)],
        ["App Summary", "Total App Posts", _safe_get_nested(stats_data, ["posts", "total"], 0)],
        ["App Summary", "Total InfoSS Posts", _safe_get_nested(stats_data, ["posts", "total_infoss"], 0)],
        ["App Summary", "Total KawanSS Posts", _safe_get_nested(stats_data, ["posts", "total_kawanss"], 0)],
        ["App Summary", "App Posts 30 Hari Terakhir", _safe_get_nested(stats_data, ["posts", "new_30_days"], 0)],
        ["App Summary", "KawanSS Posts 30 Hari Terakhir", _safe_get_nested(stats_data, ["posts", "new_30_days_kawanss"], 0)],
        ["", "", ""],

        ["Google Analytics", "Monthly Active User", ga_summary.get("monthly_active_users", 0)],
        ["Google Analytics", "Monthly New User", ga_summary.get("monthly_new_users", 0)],
        ["Google Analytics", "Monthly Total User", ga_summary.get("monthly_total_users", 0)],
        ["Google Analytics", "Monthly Sessions", ga_summary.get("monthly_sessions", 0)],
        ["Google Analytics", "Monthly Engaged Sessions", ga_summary.get("monthly_engaged_sessions", 0)],
        ["Google Analytics", "Page Views", ga_summary.get("monthly_screen_page_views", 0)],
        ["Google Analytics", "Event Count", ga_summary.get("monthly_event_count", 0)],
        ["Google Analytics", "Average Session Duration Seconds", ga_summary.get("average_session_duration_seconds", 0)],
    ]

    return [
        [_normalize_export_value(cell) for cell in row]
        for row in rows
    ]

def _build_csv_with_summary(summary_rows: list[list], df: pd.DataFrame) -> str:
    """
    Format CSV:
    - Summary di bagian atas
    - Baris kosong
    - Dataset export di bawahnya
    """

    output = []

    for row in summary_rows:
        output.append(row)

    output.append([])
    output.append(["DATASET EXPORT"])
    output.append([])

    output.append(df.columns.tolist())

    safe_df = df.fillna("").astype(str)

    for row in safe_df.values.tolist():
        output.append(row)

    csv_buffer = pd.DataFrame(output).to_csv(
        index=False,
        header=False,
        encoding="utf-8-sig",
    )

    return csv_buffer

def _get_export_dataframe(payload) -> pd.DataFrame:
    source_type = payload.source

    legacy_df = _make_legacy_export_dataframe(
        source_type=source_type,
        start_date=payload.start_date,
        end_date=payload.end_date,
    )

    if legacy_df is None or legacy_df.empty:
        raise HTTPException(
            status_code=404,
            detail="Tidak ada data untuk diexport pada filter yang dipilih.",
        )

    normalized_df = _convert_legacy_df_to_normalized(legacy_df, source_type)

    return _select_export_columns(
        df=normalized_df,
        source_type=source_type,
        selected_columns=payload.selected_columns,
        export_all=payload.export_all,
    )

def export_csv(payload, current_admin=None):
    try:
        df = get_master_dataframe(
            source_type=payload.source,
            start_date=payload.start_date,
            end_date=payload.end_date,
            selected_columns=payload.selected_columns,
            export_all=payload.export_all,
        )

        if df is None or df.empty:
            raise HTTPException(status_code=404, detail="Tidak ada data ditemukan.")

        summary_rows = _get_export_summary_rows(payload)
        csv_buffer = _build_csv_with_summary(summary_rows, df)

        filename = (
            f"SNA_Report_{payload.source.upper()}_"
            f"{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        )

        return StreamingResponse(
            iter([csv_buffer]),
            media_type="text/csv",
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"'
            },
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Export CSV gagal: {str(e)}",
        )

def export_sheets(payload, current_admin: Optional[Dict[str, Any]] = None):
    try:
        df = _get_export_dataframe(payload)

        if df.empty:
            raise HTTPException(status_code=404, detail="Tidak ada data ditemukan.")

        spreadsheet_title = (
            payload.spreadsheet_title
            or f"SNA Export {payload.source.upper()} {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}"
        ).strip()

        if not spreadsheet_title:
            spreadsheet_title = f"SNA Export {payload.source.upper()}"

        if not payload.google_access_token:
            raise HTTPException(
                status_code=401,
                detail=(
                    "Google access token tidak ditemukan. "
                    "Export ke Google Sheets membutuhkan login Google dengan izin Drive dan Sheets."
                ),
            )

        gc = get_gspread_user_client(payload.google_access_token)

        sh = gc.create(spreadsheet_title)

        summary_rows = _get_export_summary_rows(payload)

        summary_worksheet = sh.sheet1
        summary_worksheet.update_title("Summary")
        summary_worksheet.clear()
        summary_worksheet.update(summary_rows, "A1")

        export_worksheet = sh.add_worksheet(
            title="Export Data",
            rows=max(len(df) + 5, 100),
            cols=max(len(df.columns) + 2, 20),
        )

        safe_df = df.fillna("").astype(str)
        export_values = [safe_df.columns.tolist()] + safe_df.values.tolist()

        export_worksheet.clear()
        export_worksheet.update(export_values, "A1")

        admin_email = None

        if current_admin:
            admin_email = current_admin.get("email")

        doc_data = {
            "sheet_id": sh.id,
            "sheet_url": sh.url,
            "sheet_name": spreadsheet_title,
            "source_type": payload.source,
            "rows_count": len(df),
            "columns": df.columns.tolist(),
            "created_by_uid": current_admin.get("uid") if current_admin else None,
            "created_by_email": admin_email,
            "storage_owner": "login_user_drive",
            "created_at": datetime.datetime.now().isoformat(),
            "updated_at": datetime.datetime.now().isoformat(),
        }

        try:
            _, doc_ref = db.collection("linked_sheets").add(doc_data)
            doc_id = doc_ref.id
        except Exception:
            doc_id = None

        return {
            "status": "success",
            "message": "Data berhasil diexport ke Google Sheets dengan worksheet Summary dan Export Data.",
            "spreadsheet_title": spreadsheet_title,
            "spreadsheet_url": sh.url,
            "source": payload.source,
            "summary_rows_count": len(summary_rows),
            "rows_count": len(df),
            "columns": df.columns.tolist(),
            "storage_owner": "login_user_drive",
            "worksheets": [
                "Summary",
                "Export Data",
            ],
            "data": {
                "id": doc_id,
                "sheet_url": sh.url,
                "sheet_name": spreadsheet_title,
            },
        }

    except HTTPException:
        raise
    except gspread.exceptions.APIError as api_error:
        error_text = str(api_error)

        if "insufficient authentication scopes" in error_text.lower():
            raise HTTPException(
                status_code=403,
                detail=(
                    "Akses Google belum memiliki izin Drive/Sheets. "
                    "Silakan login ulang dengan Google dan berikan permission Drive serta Sheets."
                ),
            )

        if "storage quota" in error_text.lower() or "quota" in error_text.lower():
            raise HTTPException(
                status_code=403,
                detail=(
                    "Storage Google Drive akun yang login sudah penuh atau tidak memiliki quota cukup."
                ),
            )

        raise HTTPException(
            status_code=500,
            detail=f"Export Sheets gagal: {str(api_error)}",
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Export Sheets gagal: {str(e)}")

def _first_worksheet(spreadsheet):
    worksheet = spreadsheet.get_worksheet(0)

    if worksheet is None:
        worksheet = spreadsheet.add_worksheet(
            title="Sheet1",
            rows=100,
            cols=20,
        )

    return worksheet

def _format_export_date(value):
    if not value:
        return "-"

    return str(value)

def _first_worksheet(spreadsheet):
    worksheet = spreadsheet.get_worksheet(0)

    if worksheet is None:
        worksheet = spreadsheet.add_worksheet(
            title="Sheet1",
            rows=100,
            cols=20,
        )

    return worksheet

def _get_app_summary_rows():
    summary = {
        "total_users": "-",
        "new_users_this_month": "-",
        "total_app_posts": "-",
        "total_infoss_posts": "-",
        "total_kawanss_posts": "-",
        "app_posts_30_days": "-",
        "kawanss_posts_30_days": "-",
    }

    try:
        with neo4j_driver.session() as session:
            result = session.run(
                """
                MATCH (u:FirebaseUser)
                RETURN count(u) AS total_users
                """
            ).single()

            if result:
                summary["total_users"] = result.get("total_users", 0)
    except Exception:
        pass

    try:
        with neo4j_driver.session() as session:
            result = session.run(
                """
                MATCH (p)
                WHERE p:FirebaseInfoss OR p:FirebaseKawanSS
                RETURN count(p) AS total_app_posts
                """
            ).single()

            if result:
                summary["total_app_posts"] = result.get("total_app_posts", 0)
    except Exception:
        pass

    try:
        with neo4j_driver.session() as session:
            result = session.run(
                """
                MATCH (p:FirebaseInfoss)
                RETURN count(p) AS total_infoss_posts
                """
            ).single()

            if result:
                summary["total_infoss_posts"] = result.get(
                    "total_infoss_posts",
                    0,
                )
    except Exception:
        pass

    try:
        with neo4j_driver.session() as session:
            result = session.run(
                """
                MATCH (p:FirebaseKawanSS)
                RETURN count(p) AS total_kawanss_posts
                """
            ).single()

            if result:
                summary["total_kawanss_posts"] = result.get(
                    "total_kawanss_posts",
                    0,
                )
    except Exception:
        pass

    try:
        with neo4j_driver.session() as session:
            result = session.run(
                """
                MATCH (p)
                WHERE p:FirebaseInfoss OR p:FirebaseKawanSS
                WITH p, coalesce(p.createdAt, p.uploadDate, p.tanggalUpload, p.date) AS rawDate
                WITH p, datetime(toString(rawDate)) AS dt
                WHERE dt >= datetime() - duration({days: 30})
                RETURN count(p) AS app_posts_30_days
                """
            ).single()

            if result:
                summary["app_posts_30_days"] = result.get(
                    "app_posts_30_days",
                    0,
                )
    except Exception:
        pass

    try:
        with neo4j_driver.session() as session:
            result = session.run(
                """
                MATCH (p:FirebaseKawanSS)
                WITH p, coalesce(p.createdAt, p.uploadDate, p.tanggalUpload, p.date) AS rawDate
                WITH p, datetime(toString(rawDate)) AS dt
                WHERE dt >= datetime() - duration({days: 30})
                RETURN count(p) AS kawanss_posts_30_days
                """
            ).single()

            if result:
                summary["kawanss_posts_30_days"] = result.get(
                    "kawanss_posts_30_days",
                    0,
                )
    except Exception:
        pass

    try:
        now = datetime.datetime.now()
        first_day = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

        with neo4j_driver.session() as session:
            result = session.run(
                """
                MATCH (u:FirebaseUser)
                WITH u, coalesce(u.createdAt, u.created_at, u.registeredAt, u.joinedAt) AS rawDate
                WITH u, datetime(toString(rawDate)) AS dt
                WHERE dt >= datetime($first_day)
                RETURN count(u) AS new_users_this_month
                """,
                first_day=first_day.isoformat(),
            ).single()

            if result:
                summary["new_users_this_month"] = result.get(
                    "new_users_this_month",
                    0,
                )
    except Exception:
        pass

    return [
        ["App Summary", "Total Pengguna App", summary["total_users"]],
        ["App Summary", "User Baru Bulan Ini", summary["new_users_this_month"]],
        ["App Summary", "Total App Posts", summary["total_app_posts"]],
        ["App Summary", "Total InfoSS Posts", summary["total_infoss_posts"]],
        ["App Summary", "Total KawanSS Posts", summary["total_kawanss_posts"]],
        ["App Summary", "App Posts 30 Hari Terakhir", summary["app_posts_30_days"]],
        [
            "App Summary",
            "KawanSS Posts 30 Hari Terakhir",
            summary["kawanss_posts_30_days"],
        ],
    ]

def _get_google_analytics_rows():
    analytics = {
        "monthly_active_user": "-",
        "monthly_new_user": "-",
        "monthly_total_user": "-",
        "monthly_sessions": "-",
        "monthly_engaged_sessions": "-",
        "page_views": "-",
        "event_count": "-",
        "average_session_duration_seconds": "-",
    }

    try:
        doc = db.collection("google_analytics").document("monthly_summary").get()

        if doc.exists:
            data = doc.to_dict() or {}

            analytics["monthly_active_user"] = data.get(
                "monthly_active_user",
                data.get("monthly_active_users", "-"),
            )
            analytics["monthly_new_user"] = data.get(
                "monthly_new_user",
                data.get("monthly_new_users", "-"),
            )
            analytics["monthly_total_user"] = data.get(
                "monthly_total_user",
                data.get("monthly_total_users", "-"),
            )
            analytics["monthly_sessions"] = data.get("monthly_sessions", "-")
            analytics["monthly_engaged_sessions"] = data.get(
                "monthly_engaged_sessions",
                "-",
            )
            analytics["page_views"] = data.get(
                "page_views",
                data.get("monthly_screen_page_views", "-"),
            )
            analytics["event_count"] = data.get(
                "event_count",
                data.get("monthly_event_count", "-"),
            )
            analytics["average_session_duration_seconds"] = data.get(
                "average_session_duration_seconds",
                "-",
            )
    except Exception:
        pass

    return [
        ["Google Analytics", "Monthly Active User", analytics["monthly_active_user"]],
        ["Google Analytics", "Monthly New User", analytics["monthly_new_user"]],
        ["Google Analytics", "Monthly Total User", analytics["monthly_total_user"]],
        ["Google Analytics", "Monthly Sessions", analytics["monthly_sessions"]],
        [
            "Google Analytics",
            "Monthly Engaged Sessions",
            analytics["monthly_engaged_sessions"],
        ],
        ["Google Analytics", "Page Views", analytics["page_views"]],
        ["Google Analytics", "Event Count", analytics["event_count"]],
        [
            "Google Analytics",
            "Average Session Duration Seconds",
            analytics["average_session_duration_seconds"],
        ],
    ]

def _build_sheet_export_values(payload, df: pd.DataFrame):
    source = str(payload.source).upper()
    start_date = _format_export_date(getattr(payload, "start_date", None))
    end_date = _format_export_date(getattr(payload, "end_date", None))

    safe_df = df.fillna("").astype(str)

    values = []

    values.append(["Section", "Metric", "Value"])

    values.extend(
        [
            ["Export Info", "Source", source],
            ["Export Info", "Start Date", start_date],
            ["Export Info", "End Date", end_date],
        ]
    )

    values.append(["", "", ""])

    if payload.source == "app":
        values.extend(_get_app_summary_rows())
    else:
        values.extend(
            [
                ["Instagram Summary", "Total Export Rows", len(df)],
                ["Instagram Summary", "Total Columns", len(df.columns)],
            ]
        )

    values.append(["", "", ""])

    values.extend(_get_google_analytics_rows())

    values.append(["", "", ""])
    values.append(["Export Data", "Rows Count", len(df)])
    values.append(["Export Data", "Columns Count", len(df.columns)])
    values.append(["", "", ""])

    values.append(safe_df.columns.tolist())
    values.extend(safe_df.values.tolist())

    return values

def export_existing_sheets(payload, current_admin: Optional[Dict[str, Any]] = None):
    try:
        df = _get_export_dataframe(payload)

        if df.empty:
            raise HTTPException(status_code=404, detail="Tidak ada data ditemukan.")

        spreadsheet_id = _extract_spreadsheet_id(
            getattr(payload, "spreadsheet_id", None),
            getattr(payload, "spreadsheet_url", None),
        )

        gc = get_gspread_client()

        try:
            spreadsheet = gc.open_by_key(spreadsheet_id)
        except gspread.exceptions.APIError as api_error:
            error_text = str(api_error)

            if "PERMISSION_DENIED" in error_text or "403" in error_text:
                raise HTTPException(
                    status_code=403,
                    detail=(
                        "Service Account belum memiliki akses Editor ke Google Sheets tersebut. "
                        "Silakan share Google Sheets ke email Service Account sebagai Editor."
                    ),
                )

            raise

        worksheet = _first_worksheet(spreadsheet)

        values = _build_sheet_export_values(payload, df)

        worksheet.clear()

        needed_rows = max(len(values) + 10, 100)
        needed_cols = max(max(len(row) for row in values) + 5, 20)

        try:
            worksheet.resize(rows=needed_rows, cols=needed_cols)
        except Exception:
            pass

        worksheet.update(values, "A1")

        now_iso = datetime.datetime.now().isoformat()

        doc_data = {
            "sheet_id": spreadsheet.id,
            "sheet_url": spreadsheet.url,
            "sheet_name": spreadsheet.title,
            "worksheet_name": worksheet.title,
            "source_type": payload.source,
            "rows_count": len(df),
            "columns": df.columns.tolist(),
            "created_by_uid": current_admin.get("uid") if current_admin else None,
            "created_by_email": current_admin.get("email") if current_admin else None,
            "storage_owner": "user_existing_sheet_service_account_editor",
            "created_at": now_iso,
            "updated_at": now_iso,
        }

        history_saved = True
        history_error = None
        doc_id = None

        try:
            _, doc_ref = db.collection("linked_sheets").add(doc_data)
            doc_id = doc_ref.id
        except Exception as history_exception:
            history_saved = False
            history_error = str(history_exception)

        return {
            "status": "success",
            "message": "Data berhasil diexport ke tab pertama Google Sheets user.",
            "spreadsheet_id": spreadsheet.id,
            "spreadsheet_url": spreadsheet.url,
            "spreadsheet_title": spreadsheet.title,
            "worksheet_name": worksheet.title,
            "source": payload.source,
            "rows_count": len(df),
            "columns": df.columns.tolist(),
            "storage_owner": "user_existing_sheet_service_account_editor",
            "history": {
                "saved": history_saved,
                "doc_id": doc_id,
                "error": history_error,
            },
            "data": {
                "id": doc_id,
                "sheet_url": spreadsheet.url,
                "sheet_name": spreadsheet.title,
                "worksheet_name": worksheet.title,
            },
            "debug": {
                "target_tab": worksheet.title,
                "values_rows_sent": len(values),
                "values_cols_sent": max(len(row) for row in values),
                "data_rows_count": len(df),
                "history_saved": history_saved,
                "history_error": history_error,
            },
        }

    except HTTPException:
        raise
    except gspread.exceptions.APIError as api_error:
        raise HTTPException(
            status_code=500,
            detail=f"Export Sheets gagal: {str(api_error)}",
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Export Sheets gagal: {str(e)}",
        )

def _normalize_firestore_datetime(value):
    if value is None:
        return ""

    try:
        # Firestore DatetimeWithNanoseconds biasanya punya isoformat()
        if hasattr(value, "isoformat"):
            return value.isoformat()
    except Exception:
        pass

    return str(value)

def _history_sort_key(item: Dict[str, Any]):
    raw_value = item.get("updated_at") or item.get("created_at") or ""

    try:
        if hasattr(raw_value, "timestamp"):
            return raw_value.timestamp()
    except Exception:
        pass

    try:
        raw_str = str(raw_value)
        parsed = datetime.datetime.fromisoformat(raw_str.replace("Z", "+00:00"))
        return parsed.timestamp()
    except Exception:
        return 0

def get_exported_sheets_history(current_admin: Optional[Dict[str, Any]] = None):
    try:
        current_uid = current_admin.get("uid") if current_admin else None
        current_email = current_admin.get("email") if current_admin else None

        docs = db.collection("linked_sheets").stream()

        items = []

        for doc in docs:
            data = doc.to_dict() or {}

            created_by_uid = data.get("created_by_uid")
            created_by_email = data.get("created_by_email")

            is_owner = False

            if current_uid and created_by_uid == current_uid:
                is_owner = True

            if current_email and created_by_email == current_email:
                is_owner = True

            # Debug fallback:
            # Supaya data lama yang belum punya owner tetap muncul dulu.
            if not created_by_uid and not created_by_email:
                is_owner = True

            if not is_owner:
                continue

            created_at_raw = data.get("created_at")
            updated_at_raw = data.get("updated_at")

            items.append(
                {
                    "id": doc.id,
                    "sheet_id": data.get("sheet_id", ""),
                    "sheet_url": data.get("sheet_url", ""),
                    "sheet_name": data.get("sheet_name", ""),
                    "spreadsheet_title": data.get("sheet_name", ""),
                    "worksheet_name": data.get("worksheet_name", ""),
                    "source_type": data.get("source_type", ""),
                    "source": data.get("source_type", ""),
                    "rows_count": data.get("rows_count", 0),
                    "columns": data.get("columns", []),
                    "storage_owner": data.get("storage_owner", ""),
                    "created_by_uid": created_by_uid,
                    "created_by_email": created_by_email,

                    # Penting:
                    # Return ke frontend selalu string, jangan object Firestore timestamp.
                    "created_at": _normalize_firestore_datetime(created_at_raw),
                    "updated_at": _normalize_firestore_datetime(updated_at_raw),

                    # Internal sort value, nanti dihapus sebelum return.
                    "_sort_created_at": created_at_raw,
                    "_sort_updated_at": updated_at_raw,
                }
            )

        items.sort(
            key=lambda item: _history_sort_key(
                {
                    "created_at": item.get("_sort_created_at"),
                    "updated_at": item.get("_sort_updated_at"),
                }
            ),
            reverse=True,
        )

        for item in items:
            item.pop("_sort_created_at", None)
            item.pop("_sort_updated_at", None)

        return {
            "status": "success",
            "message": "Riwayat export Google Sheets berhasil dimuat.",
            "total": len(items),
            "current_user": {
                "uid": current_uid,
                "email": current_email,
            },
            "data": items,
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Gagal memuat riwayat export Google Sheets: {str(e)}",
        )
           
def get_linked_sheets(current_admin=None):
    try:
        current_uid = current_admin.get("uid") if current_admin else None
        current_email = current_admin.get("email") if current_admin else None

        docs = (
            db.collection("linked_sheets")
            .order_by("created_at", direction="DESCENDING")
            .stream()
        )

        result = []

        for doc in docs:
            item = doc.to_dict()
            item["id"] = doc.id

            created_by_uid = item.get("created_by_uid")
            created_by_email = item.get("created_by_email")

            if created_by_uid:
                if current_uid and created_by_uid != current_uid:
                    continue
            elif created_by_email:
                if current_email and created_by_email != current_email:
                    continue

            result.append(item)

        return {
            "status": "success",
            "data": result,
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Gagal mengambil daftar Spreadsheet: {str(e)}",
        )

def unlink_sheet(doc_id: str, current_admin=None):
    try:
        current_uid = current_admin.get("uid") if current_admin else None
        current_email = current_admin.get("email") if current_admin else None

        doc_ref = db.collection("linked_sheets").document(doc_id)
        snapshot = doc_ref.get()

        if not snapshot.exists:
            raise HTTPException(
                status_code=404,
                detail="Riwayat Spreadsheet tidak ditemukan.",
            )

        data = snapshot.to_dict()

        created_by_uid = data.get("created_by_uid")
        created_by_email = data.get("created_by_email")

        if created_by_uid and current_uid and created_by_uid != current_uid:
            raise HTTPException(
                status_code=403,
                detail="Anda tidak memiliki akses untuk menghapus riwayat ini.",
            )

        if not created_by_uid and created_by_email and current_email:
            if created_by_email != current_email:
                raise HTTPException(
                    status_code=403,
                    detail="Anda tidak memiliki akses untuk menghapus riwayat ini.",
                )

        doc_ref.delete()

        return {
            "status": "success",
            "message": "Riwayat Spreadsheet berhasil dihapus.",
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Gagal menghapus riwayat Spreadsheet: {str(e)}",
        )

    try:
        docs = db.collection("linked_sheets").order_by(
            "created_at",
            direction="DESCENDING",
        ).stream()

        return {
            "status": "success",
            "data": [{"id": doc.id, **doc.to_dict()} for doc in docs],
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    try:
        db.collection("linked_sheets").document(doc_id).delete()

        return {
            "status": "success",
            "message": "Tautan dihapus dari Dashboard.",
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))