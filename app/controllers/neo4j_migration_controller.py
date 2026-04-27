import asyncio
import traceback
from datetime import datetime, timezone
from typing import Any

from fastapi import BackgroundTasks, HTTPException

from app.database import db, neo4j_driver


MIGRATION_META_COLLECTION = "_migration_meta"
MIGRATION_META_DOCUMENT = "firebase_to_neo4j"

BATCH_SIZE = 500
STALE_LOCK_MINUTES = 10

_migration_lock = asyncio.Lock()

def start_migration(background_tasks: BackgroundTasks) -> bool:
    status = _get_status_doc()

    if _is_migration_running_and_not_stale(status):
        print("[MIGRATION] Request ditolak karena migrasi masih berjalan.")
        print(f"[MIGRATION] Status sekarang: {status.get('status')}")
        print(f"[MIGRATION] Updated at: {status.get('updated_at')}")
        return False

    if status.get("is_running") is True:
        print("[MIGRATION] Stale lock terdeteksi. Migration akan diambil alih ulang.")

    _set_status_doc({
        "is_running": True,
        "status": "queued",
        "mode": "full_scan_upsert",
        "started_at": _now_iso(),
        "finished_at": None,
        "updated_at": _now_iso(),
        "heartbeat_at": _now_iso(),
        "last_error": None,
        "processed": {
            "users": _empty_progress("users"),
            "kawanss": _empty_progress("kawanss"),
            "infoss": _empty_progress("infoss"),
            "comments": _empty_progress("comments"),
        },
        "summary": {}
    })

    background_tasks.add_task(run_migration_background)

    print("[MIGRATION] Background migration queued.")

    return True


def get_migration_status():
    status = _get_status_doc()

    if not status:
        return {
            "is_running": False,
            "status": "idle",
            "mode": None,
            "started_at": None,
            "finished_at": None,
            "updated_at": None,
            "heartbeat_at": None,
            "last_error": None,
            "processed": {},
            "summary": {}
        }

    status["is_stale"] = _is_status_stale(status)

    return status


def unlock_migration():
    _update_status_doc({
        "is_running": False,
        "status": "unlocked",
        "finished_at": _now_iso(),
        "updated_at": _now_iso(),
        "heartbeat_at": _now_iso(),
    })

    print("[MIGRATION] Lock migration dibuka manual.")

    return {
        "status": "success",
        "message": "Migration lock berhasil dibuka. Sekarang Anda bisa hit POST /neo4j/migrate lagi."
    }


async def run_migration_background():
    if _migration_lock.locked():
        print("[MIGRATION] Local Python lock aktif. Migration tidak dijalankan ulang.")
        return

    async with _migration_lock:
        try:
            _update_status_doc({
                "is_running": True,
                "status": "running",
                "updated_at": _now_iso(),
                "heartbeat_at": _now_iso(),
            })

            await asyncio.to_thread(_run_full_migration_sync)

            _update_status_doc({
                "is_running": False,
                "status": "success",
                "finished_at": _now_iso(),
                "updated_at": _now_iso(),
                "heartbeat_at": _now_iso(),
                "last_error": None,
            })

        except Exception as e:
            traceback.print_exc()

            _update_status_doc({
                "is_running": False,
                "status": "failed",
                "finished_at": _now_iso(),
                "updated_at": _now_iso(),
                "heartbeat_at": _now_iso(),
                "last_error": str(e),
            })


async def delete_all_neo4j_data():
    try:
        with neo4j_driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")

        try:
            db.collection(MIGRATION_META_COLLECTION).document(MIGRATION_META_DOCUMENT).delete()
        except Exception:
            pass

        print("[NEO4J] Semua data berhasil dihapus.")

        return {
            "status": "success",
            "message": "Semua data Neo4j berhasil dihapus dan metadata migration berhasil direset."
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Gagal menghapus data Neo4j: {str(e)}"
        )

def _run_full_migration_sync():
    start_time = datetime.now(timezone.utc)

    _ensure_neo4j_constraints()

    print("")
    print("=" * 100)
    print("[FIREBASE -> NEO4J MIGRATION] START")
    print("[MODE] FULL SCAN + UPSERT")
    print("[INFO] Semua data Firebase dibaca. Neo4j akan MERGE berdasarkan id.")
    print("=" * 100)

    users_total = _migrate_users()

    kawanss_total = _migrate_posts(
        collection_name="kawanss",
        neo4j_label="FirebaseKawanSS",
        progress_key="kawanss"
    )

    infoss_total = _migrate_posts(
        collection_name="infoss",
        neo4j_label="FirebaseInfoss",
        progress_key="infoss"
    )

    comments_total = _migrate_comments()

    finished_at = datetime.now(timezone.utc)
    duration_seconds = round((finished_at - start_time).total_seconds(), 2)

    summary = {
        "users": users_total,
        "kawanss": kawanss_total,
        "infoss": infoss_total,
        "comments": comments_total,
        "duration_seconds": duration_seconds,
    }

    _update_status_doc({
        "last_migrated_at": finished_at.isoformat(),
        "updated_at": finished_at.isoformat(),
        "heartbeat_at": finished_at.isoformat(),
        "summary": summary,
    })

    print("=" * 100)
    print("[FIREBASE -> NEO4J MIGRATION] FINISHED")
    print(f"[SUMMARY] Users   : {users_total}")
    print(f"[SUMMARY] KawanSS : {kawanss_total}")
    print(f"[SUMMARY] InfoSS  : {infoss_total}")
    print(f"[SUMMARY] Comments: {comments_total}")
    print(f"[SUMMARY] Duration: {duration_seconds} seconds")
    print("=" * 100)
    print("")

def _ensure_neo4j_constraints():
    queries = [
        """
        CREATE CONSTRAINT firebase_user_id IF NOT EXISTS
        FOR (u:FirebaseUser)
        REQUIRE u.id IS UNIQUE
        """,
        """
        CREATE CONSTRAINT firebase_kawanss_id IF NOT EXISTS
        FOR (p:FirebaseKawanSS)
        REQUIRE p.id IS UNIQUE
        """,
        """
        CREATE CONSTRAINT firebase_infoss_id IF NOT EXISTS
        FOR (p:FirebaseInfoss)
        REQUIRE p.id IS UNIQUE
        """,
        """
        CREATE CONSTRAINT firebase_comment_id IF NOT EXISTS
        FOR (c:FirebaseComment)
        REQUIRE c.id IS UNIQUE
        """
    ]

    with neo4j_driver.session() as session:
        for query in queries:
            session.run(query)

    print("[NEO4J] Constraints checked/created.")

def _migrate_users() -> int:
    collection_name = "users"
    progress_key = "users"

    print("")
    print(f"[MIGRATION] Start users from Firebase collection '{collection_name}'")

    batch = []
    total_read = 0
    total_saved = 0
    last_id = None
    last_data = None

    try:
        docs = db.collection(collection_name).stream()

        for doc in docs:
            raw = doc.to_dict() or {}
            raw["id"] = doc.id

            user = _normalize_user(raw)

            batch.append(user)
            total_read += 1
            last_id = doc.id
            last_data = user

            if total_read % 100 == 0:
                print(f"[MIGRATION][users] read: {total_read}, saved: {total_saved}, last_id: {last_id}")
                _update_progress_seen(progress_key, total_read, total_saved, last_id, last_data)

            if len(batch) >= BATCH_SIZE:
                _upsert_users(batch)
                total_saved += len(batch)

                print(f"[MIGRATION][users] batch saved: {len(batch)} | total read: {total_read} | total saved: {total_saved}")
                _update_progress_seen(progress_key, total_read, total_saved, last_id, last_data)

                batch.clear()

        if batch:
            _upsert_users(batch)
            total_saved += len(batch)

            print(f"[MIGRATION][users] final batch saved: {len(batch)} | total read: {total_read} | total saved: {total_saved}")

        _update_progress_done(progress_key, total_read, total_saved, last_id, last_data)

        print(f"[MIGRATION] Done users. Total migrated/upserted: {total_saved}")

        return total_saved

    except Exception as e:
        print(f"[MIGRATION][users] ERROR: {e}")
        _update_progress_error(progress_key, e)
        return total_saved


def _normalize_user(data: dict[str, Any]) -> dict[str, Any]:
    user_id = str(data.get("id", ""))

    username = _first_not_empty(
        data,
        ["username", "userName", "nama", "name", "displayName", "fullName"],
        user_id
    )

    nama = _first_not_empty(
        data,
        ["nama", "name", "displayName", "fullName", "username"],
        username
    )

    return {
        "id": user_id,
        "username": str(username),
        "nama": str(nama),
        "email": str(data.get("email", "")),
        "phone": str(_first_not_empty(data, ["phone", "phoneNumber"], "")),
        "photoUrl": str(_first_not_empty(data, ["photoUrl", "photoURL"], "")),
        "createdAt": _safe_datetime_value(data.get("createdAt")),
        "updatedAt": _safe_datetime_value(data.get("updatedAt")),
    }


def _upsert_users(users: list[dict[str, Any]]):
    query = """
    UNWIND $users AS user

    MERGE (u:FirebaseUser {id: user.id})
    SET u.username = user.username,
        u.nama = user.nama,
        u.email = user.email,
        u.phone = user.phone,
        u.photoUrl = user.photoUrl,
        u.createdAt = user.createdAt,
        u.updatedAt = user.updatedAt,
        u.lastMigratedAt = datetime()
    """

    with neo4j_driver.session() as session:
        session.run(query, users=users)

def _migrate_posts(collection_name: str, neo4j_label: str, progress_key: str) -> int:
    print("")
    print(f"[MIGRATION] Start posts from Firebase collection '{collection_name}' -> Neo4j label '{neo4j_label}'")

    batch = []
    total_read = 0
    total_saved = 0
    last_id = None
    last_data = None

    try:
        docs = db.collection(collection_name).stream()

        for doc in docs:
            raw = doc.to_dict() or {}
            raw["id"] = doc.id

            post = _normalize_post(raw)

            batch.append(post)
            total_read += 1
            last_id = doc.id
            last_data = post

            if total_read % 100 == 0:
                print(f"[MIGRATION][{collection_name}] read: {total_read}, saved: {total_saved}, last_id: {last_id}")
                _update_progress_seen(progress_key, total_read, total_saved, last_id, last_data)

            if len(batch) >= BATCH_SIZE:
                _upsert_posts(batch, neo4j_label)
                total_saved += len(batch)

                print(f"[MIGRATION][{collection_name}] batch saved: {len(batch)} | total read: {total_read} | total saved: {total_saved}")
                _update_progress_seen(progress_key, total_read, total_saved, last_id, last_data)

                batch.clear()

        if batch:
            _upsert_posts(batch, neo4j_label)
            total_saved += len(batch)

            print(f"[MIGRATION][{collection_name}] final batch saved: {len(batch)} | total read: {total_read} | total saved: {total_saved}")

        _update_progress_done(progress_key, total_read, total_saved, last_id, last_data)

        print(f"[MIGRATION] Done {collection_name}. Total migrated/upserted: {total_saved}")

        return total_saved

    except Exception as e:
        print(f"[MIGRATION][{collection_name}] ERROR: {e}")
        _update_progress_error(progress_key, e)
        return total_saved


def _normalize_post(data: dict[str, Any]) -> dict[str, Any]:
    post_id = str(data.get("id", ""))

    author_id = _first_not_empty(
        data,
        ["userId", "authorId", "uid", "createdBy", "ownerId", "idUser", "user_id"],
        ""
    )

    title = _first_not_empty(
        data,
        ["judul", "title", "nama", "name"],
        ""
    )

    description = _first_not_empty(
        data,
        ["deskripsi", "description", "content", "text", "caption", "body"],
        ""
    )

    created_at = _first_not_empty(
        data,
        ["createdAt", "uploadDate", "created_at", "tanggalUpload"],
        ""
    )

    updated_at = _first_not_empty(
        data,
        ["updatedAt", "updated_at"],
        ""
    )

    upload_date = _first_not_empty(
        data,
        ["uploadDate", "createdAt", "created_at", "tanggalUpload"],
        ""
    )

    return {
        "id": post_id,
        "author_id": str(author_id),
        "judul": str(title),
        "title": str(title),
        "deskripsi": str(description),
        "createdAt": _safe_datetime_value(created_at),
        "updatedAt": _safe_datetime_value(updated_at),
        "uploadDate": _safe_datetime_value(upload_date),
        "jumlahLike": _safe_int(_first_not_empty(data, ["jumlahLike", "likeCount", "likes"], 0)),
        "jumlahView": _safe_int(_first_not_empty(data, ["jumlahView", "viewCount", "views"], 0)),
        "jumlahComment": _safe_int(_first_not_empty(data, ["jumlahComment", "commentCount", "commentsCount"], 0)),
        "jumlahShare": _safe_int(_first_not_empty(data, ["jumlahShare", "shareCount", "shares"], 0)),
        "isDeleted": _safe_bool(data.get("isDeleted"), False),
    }


def _upsert_posts(posts: list[dict[str, Any]], neo4j_label: str):
    allowed_labels = {"FirebaseKawanSS", "FirebaseInfoss"}

    if neo4j_label not in allowed_labels:
        raise ValueError(f"Neo4j label tidak valid: {neo4j_label}")

    query = f"""
    UNWIND $posts AS post

    MERGE (p:{neo4j_label} {{id: post.id}})
    SET p.judul = post.judul,
        p.title = post.title,
        p.deskripsi = post.deskripsi,
        p.createdAt = post.createdAt,
        p.updatedAt = post.updatedAt,
        p.uploadDate = post.uploadDate,
        p.jumlahLike = post.jumlahLike,
        p.jumlahView = post.jumlahView,
        p.jumlahComment = post.jumlahComment,
        p.jumlahShare = post.jumlahShare,
        p.isDeleted = post.isDeleted,
        p.lastMigratedAt = datetime()

    WITH p, post
    FOREACH (_ IN CASE WHEN post.author_id <> '' THEN [1] ELSE [] END |
        MERGE (u:FirebaseUser {{id: post.author_id}})
        MERGE (u)-[:POSTED_FB]->(p)
    )
    """

    with neo4j_driver.session() as session:
        session.run(query, posts=posts)


def _migrate_comments() -> int:
    collection_name = "comments"
    progress_key = "comments"

    print("")
    print(f"[MIGRATION] Start comments from Firebase collection '{collection_name}'")

    batch = []
    total_read = 0
    total_saved = 0
    last_id = None
    last_data = None

    try:
        docs = db.collection(collection_name).stream()

        for doc in docs:
            raw = doc.to_dict() or {}
            raw["id"] = doc.id

            comment = _normalize_comment(raw)

            batch.append(comment)
            total_read += 1
            last_id = doc.id
            last_data = comment

            if total_read % 100 == 0:
                print(f"[MIGRATION][comments] read: {total_read}, saved: {total_saved}, last_id: {last_id}")
                _update_progress_seen(progress_key, total_read, total_saved, last_id, last_data)

            if len(batch) >= BATCH_SIZE:
                _upsert_comments(batch)
                total_saved += len(batch)

                print(f"[MIGRATION][comments] batch saved: {len(batch)} | total read: {total_read} | total saved: {total_saved}")
                _update_progress_seen(progress_key, total_read, total_saved, last_id, last_data)

                batch.clear()

        if batch:
            _upsert_comments(batch)
            total_saved += len(batch)

            print(f"[MIGRATION][comments] final batch saved: {len(batch)} | total read: {total_read} | total saved: {total_saved}")

        _update_progress_done(progress_key, total_read, total_saved, last_id, last_data)

        print(f"[MIGRATION] Done comments. Total migrated/upserted: {total_saved}")

        return total_saved

    except Exception as e:
        print(f"[MIGRATION][comments] ERROR: {e}")
        _update_progress_error(progress_key, e)
        return total_saved


def _normalize_comment(data: dict[str, Any]) -> dict[str, Any]:
    text = _first_not_empty(
        data,
        ["text", "komentar", "comment", "content", "body", "message"],
        ""
    )

    post_id = _first_not_empty(
        data,
        ["postId", "targetPostId", "target_id", "targetId", "idPost", "post_id"],
        ""
    )

    author_id = _first_not_empty(
        data,
        ["userId", "authorId", "uid", "createdBy", "ownerId", "idUser", "user_id"],
        ""
    )

    return {
        "id": str(data.get("id", "")),
        "author_id": str(author_id),
        "post_id": str(post_id),
        "text": str(text),
        "komentar": str(text),
        "likes": _safe_int(_first_not_empty(data, ["likes", "jumlahLike", "likeCount"], 0)),
        "createdAt": _safe_datetime_value(_first_not_empty(data, ["createdAt", "created_at", "timestamp"], "")),
        "updatedAt": _safe_datetime_value(_first_not_empty(data, ["updatedAt", "updated_at"], "")),
        "isDeleted": _safe_bool(data.get("isDeleted"), False),
    }


def _upsert_comments(comments: list[dict[str, Any]]):
    query = """
    UNWIND $comments AS comment

    MERGE (c:FirebaseComment {id: comment.id})
    SET c.text = comment.text,
        c.komentar = comment.komentar,
        c.likes = comment.likes,
        c.createdAt = comment.createdAt,
        c.updatedAt = comment.updatedAt,
        c.isDeleted = comment.isDeleted,
        c.lastMigratedAt = datetime()

    WITH c, comment
    FOREACH (_ IN CASE WHEN comment.author_id <> '' THEN [1] ELSE [] END |
        MERGE (u:FirebaseUser {id: comment.author_id})
        MERGE (u)-[:WROTE_FB]->(c)
    )

    WITH c, comment
    OPTIONAL MATCH (p1:FirebaseKawanSS {id: comment.post_id})
    OPTIONAL MATCH (p2:FirebaseInfoss {id: comment.post_id})
    WITH c, comment, coalesce(p1, p2) AS post
    FOREACH (_ IN CASE WHEN post IS NOT NULL THEN [1] ELSE [] END |
        MERGE (c)-[:COMMENTED_ON_FB]->(post)
    )
    """

    with neo4j_driver.session() as session:
        session.run(query, comments=comments)


def _empty_progress(collection_name: str) -> dict[str, Any]:
    return {
        "collection": collection_name,
        "total_read": 0,
        "total_saved": 0,
        "last_id": None,
        "last_data": None,
        "last_error": None,
        "done": False,
    }


def _update_progress_seen(
    key: str,
    total_read: int,
    total_saved: int,
    last_id: str,
    last_data: dict[str, Any]
):
    _update_status_doc({
        f"processed.{key}.total_read": total_read,
        f"processed.{key}.total_saved": total_saved,
        f"processed.{key}.last_id": last_id,
        f"processed.{key}.last_data": _sanitize_status_data(last_data),
        f"processed.{key}.done": False,
        "updated_at": _now_iso(),
        "heartbeat_at": _now_iso(),
    })


def _update_progress_done(
    key: str,
    total_read: int,
    total_saved: int,
    last_id: str | None,
    last_data: dict[str, Any] | None
):
    _update_status_doc({
        f"processed.{key}.total_read": total_read,
        f"processed.{key}.total_saved": total_saved,
        f"processed.{key}.last_id": last_id,
        f"processed.{key}.last_data": _sanitize_status_data(last_data or {}),
        f"processed.{key}.done": True,
        "updated_at": _now_iso(),
        "heartbeat_at": _now_iso(),
    })


def _update_progress_error(key: str, error: Exception):
    _update_status_doc({
        f"processed.{key}.last_error": str(error),
        "updated_at": _now_iso(),
        "heartbeat_at": _now_iso(),
    })


def _get_status_doc() -> dict[str, Any]:
    try:
        doc = db.collection(MIGRATION_META_COLLECTION).document(MIGRATION_META_DOCUMENT).get()

        if not doc.exists:
            return {}

        return doc.to_dict() or {}

    except Exception as e:
        print(f"[MIGRATION STATUS] Failed to get status doc: {e}")
        return {}


def _set_status_doc(data: dict[str, Any]):
    db.collection(MIGRATION_META_COLLECTION).document(MIGRATION_META_DOCUMENT).set(data)


def _update_status_doc(data: dict[str, Any]):
    db.collection(MIGRATION_META_COLLECTION).document(MIGRATION_META_DOCUMENT).set(data, merge=True)


def _is_migration_running_and_not_stale(status: dict[str, Any]) -> bool:
    if status.get("is_running") is not True:
        return False

    return not _is_status_stale(status)


def _is_status_stale(status: dict[str, Any]) -> bool:
    heartbeat_at = status.get("heartbeat_at") or status.get("updated_at")

    if not heartbeat_at:
        return True

    try:
        heartbeat_dt = datetime.fromisoformat(str(heartbeat_at))
        if heartbeat_dt.tzinfo is None:
            heartbeat_dt = heartbeat_dt.replace(tzinfo=timezone.utc)

        age_seconds = (datetime.now(timezone.utc) - heartbeat_dt).total_seconds()

        return age_seconds > (STALE_LOCK_MINUTES * 60)

    except Exception:
        return True

def _sanitize_status_data(data: dict[str, Any]) -> dict[str, Any]:
    sanitized = {}

    for key, value in data.items():
        if isinstance(value, (str, int, float, bool)) or value is None:
            sanitized[key] = value
        else:
            sanitized[key] = str(value)

    return sanitized


def _first_not_empty(data: dict[str, Any], keys: list[str], default: Any = "") -> Any:
    for key in keys:
        value = data.get(key)

        if value is not None and value != "":
            return value

    return default


def _safe_datetime_value(value: Any) -> str:
    if value is None:
        return ""

    if isinstance(value, datetime):
        return value.isoformat()

    if hasattr(value, "isoformat"):
        return value.isoformat()

    return str(value)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default

        return int(value)

    except Exception:
        return default


def _safe_bool(value: Any, default: bool = False) -> bool:
    if value is None or value == "":
        return default

    if isinstance(value, bool):
        return value

    if isinstance(value, str):
        return value.lower() in ["true", "1", "yes", "y"]

    return bool(value)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()