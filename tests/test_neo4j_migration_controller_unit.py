import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.controllers import neo4j_migration_controller as migration
from app.controllers.neo4j_migration_controller import (
    _empty_progress,
    _ensure_neo4j_constraints,
    _first_not_empty,
    _get_status_doc,
    _is_migration_running_and_not_stale,
    _is_status_stale,
    _migrate_comments,
    _migrate_posts,
    _migrate_users,
    _normalize_comment,
    _normalize_post,
    _normalize_user,
    _now_iso,
    _run_full_migration_sync,
    _safe_bool,
    _safe_datetime_value,
    _safe_int,
    _sanitize_status_data,
    _set_status_doc,
    _update_progress_done,
    _update_progress_error,
    _update_progress_seen,
    _update_status_doc,
    _upsert_comments,
    _upsert_posts,
    _upsert_users,
    run_migration_background,
)


class FakeDoc:
    def __init__(self, doc_id="doc-1", data=None, exists=True):
        self.id = doc_id
        self._data = data or {}
        self.exists = exists

    def to_dict(self):
        return self._data


class FakeSession:
    def __init__(self):
        self.run = MagicMock()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False


def test_empty_progress():
    result = _empty_progress("users")

    assert result["collection"] == "users"
    assert result["total_read"] == 0
    assert result["done"] is False


def test_first_not_empty():
    data = {
        "a": "",
        "b": None,
        "c": "value",
    }

    assert _first_not_empty(data, ["a", "b", "c"], "default") == "value"
    assert _first_not_empty(data, ["x"], "default") == "default"


def test_safe_datetime_value():
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)

    assert _safe_datetime_value(None) == ""
    assert _safe_datetime_value(now) == now.isoformat()
    assert _safe_datetime_value("date-string") == "date-string"


def test_safe_int():
    assert _safe_int("10") == 10
    assert _safe_int(None, default=5) == 5
    assert _safe_int("invalid", default=7) == 7


def test_safe_bool():
    assert _safe_bool(True) is True
    assert _safe_bool(False) is False
    assert _safe_bool("true") is True
    assert _safe_bool("1") is True
    assert _safe_bool("yes") is True
    assert _safe_bool("false") is False
    assert _safe_bool(None, default=True) is True


def test_now_iso():
    result = _now_iso()

    assert isinstance(result, str)
    assert "T" in result


def test_sanitize_status_data():
    result = _sanitize_status_data(
        {
            "text": "abc",
            "number": 1,
            "none": None,
            "object": {"a": 1},
        }
    )

    assert result["text"] == "abc"
    assert result["number"] == 1
    assert result["none"] is None
    assert result["object"] == "{'a': 1}"


def test_is_status_stale():
    fresh = {
        "heartbeat_at": datetime.now(timezone.utc).isoformat(),
    }

    stale = {
        "heartbeat_at": (
            datetime.now(timezone.utc) - timedelta(minutes=20)
        ).isoformat(),
    }

    invalid = {
        "heartbeat_at": "invalid-date",
    }

    assert _is_status_stale(fresh) is False
    assert _is_status_stale(stale) is True
    assert _is_status_stale(invalid) is True
    assert _is_status_stale({}) is True


def test_is_migration_running_and_not_stale():
    assert _is_migration_running_and_not_stale({"is_running": False}) is False

    status = {
        "is_running": True,
        "heartbeat_at": datetime.now(timezone.utc).isoformat(),
    }

    assert _is_migration_running_and_not_stale(status) is True


def test_normalize_user():
    result = _normalize_user(
        {
            "id": "user-1",
            "username": "budi",
            "nama": "Budi",
            "email": "budi@test.com",
            "phoneNumber": "081",
            "photoURL": "photo",
        }
    )

    assert result["id"] == "user-1"
    assert result["username"] == "budi"
    assert result["nama"] == "Budi"
    assert result["email"] == "budi@test.com"
    assert result["phone"] == "081"
    assert result["photoUrl"] == "photo"


def test_normalize_post():
    result = _normalize_post(
        {
            "id": "post-1",
            "userId": "user-1",
            "judul": "Judul",
            "deskripsi": "Isi",
            "jumlahLike": "10",
            "jumlahView": "20",
            "jumlahComment": "3",
            "jumlahShare": "4",
            "isDeleted": "true",
        }
    )

    assert result["id"] == "post-1"
    assert result["author_id"] == "user-1"
    assert result["judul"] == "Judul"
    assert result["jumlahLike"] == 10
    assert result["jumlahView"] == 20
    assert result["isDeleted"] is True


def test_normalize_comment():
    result = _normalize_comment(
        {
            "id": "comment-1",
            "userId": "user-1",
            "postId": "post-1",
            "text": "Komentar",
            "likes": "5",
            "isDeleted": False,
        }
    )

    assert result["id"] == "comment-1"
    assert result["author_id"] == "user-1"
    assert result["post_id"] == "post-1"
    assert result["text"] == "Komentar"
    assert result["likes"] == 5


def test_get_status_doc_exists(monkeypatch):
    doc = MagicMock()
    doc.exists = True
    doc.to_dict.return_value = {
        "status": "running",
    }

    fake_db = MagicMock()
    fake_db.collection.return_value.document.return_value.get.return_value = doc

    monkeypatch.setattr(migration, "db", fake_db)

    result = _get_status_doc()

    assert result == {
        "status": "running",
    }


def test_get_status_doc_not_exists(monkeypatch):
    doc = MagicMock()
    doc.exists = False

    fake_db = MagicMock()
    fake_db.collection.return_value.document.return_value.get.return_value = doc

    monkeypatch.setattr(migration, "db", fake_db)

    assert _get_status_doc() == {}


def test_set_status_doc(monkeypatch):
    fake_db = MagicMock()
    monkeypatch.setattr(migration, "db", fake_db)

    _set_status_doc({"status": "queued"})

    fake_db.collection.return_value.document.return_value.set.assert_called_once_with(
        {"status": "queued"}
    )


def test_update_status_doc(monkeypatch):
    fake_db = MagicMock()
    monkeypatch.setattr(migration, "db", fake_db)

    _update_status_doc({"status": "running"})

    fake_db.collection.return_value.document.return_value.set.assert_called_once_with(
        {"status": "running"},
        merge=True,
    )


def test_update_progress_seen(monkeypatch):
    mock_update = MagicMock()
    monkeypatch.setattr(migration, "_update_status_doc", mock_update)

    _update_progress_seen(
        key="users",
        total_read=1,
        total_saved=1,
        last_id="user-1",
        last_data={"a": object()},
    )

    called_data = mock_update.call_args.args[0]
    assert called_data["processed.users.total_read"] == 1
    assert called_data["processed.users.done"] is False


def test_update_progress_done(monkeypatch):
    mock_update = MagicMock()
    monkeypatch.setattr(migration, "_update_status_doc", mock_update)

    _update_progress_done(
        key="users",
        total_read=2,
        total_saved=2,
        last_id="user-2",
        last_data={"name": "Budi"},
    )

    called_data = mock_update.call_args.args[0]
    assert called_data["processed.users.total_read"] == 2
    assert called_data["processed.users.done"] is True


def test_update_progress_error(monkeypatch):
    mock_update = MagicMock()
    monkeypatch.setattr(migration, "_update_status_doc", mock_update)

    _update_progress_error("users", Exception("error test"))

    called_data = mock_update.call_args.args[0]
    assert called_data["processed.users.last_error"] == "error test"


def test_ensure_neo4j_constraints(monkeypatch):
    session = FakeSession()
    fake_driver = MagicMock()
    fake_driver.session.return_value = session

    monkeypatch.setattr(migration, "neo4j_driver", fake_driver)

    _ensure_neo4j_constraints()

    assert session.run.call_count == 5


def test_upsert_users(monkeypatch):
    session = FakeSession()
    fake_driver = MagicMock()
    fake_driver.session.return_value = session

    monkeypatch.setattr(migration, "neo4j_driver", fake_driver)

    _upsert_users([{"id": "user-1"}])

    session.run.assert_called_once()


def test_upsert_posts_success(monkeypatch):
    session = FakeSession()
    fake_driver = MagicMock()
    fake_driver.session.return_value = session

    monkeypatch.setattr(migration, "neo4j_driver", fake_driver)

    _upsert_posts([{"id": "post-1"}], "FirebaseKawanSS")

    session.run.assert_called_once()


def test_upsert_posts_invalid_label():
    with pytest.raises(ValueError):
        _upsert_posts([{"id": "post-1"}], "InvalidLabel")


def test_upsert_comments(monkeypatch):
    session = FakeSession()
    fake_driver = MagicMock()
    fake_driver.session.return_value = session

    monkeypatch.setattr(migration, "neo4j_driver", fake_driver)

    _upsert_comments([{"id": "comment-1"}])

    session.run.assert_called_once()


def test_migrate_users(monkeypatch):
    docs = [
        FakeDoc(
            doc_id="user-1",
            data={
                "username": "budi",
            },
        )
    ]

    fake_db = MagicMock()
    fake_db.collection.return_value.stream.return_value = docs

    monkeypatch.setattr(migration, "db", fake_db)
    monkeypatch.setattr(migration, "_upsert_users", MagicMock())
    monkeypatch.setattr(migration, "_update_progress_done", MagicMock())
    monkeypatch.setattr(migration, "_update_progress_error", MagicMock())

    result = _migrate_users()

    assert result == 1
    migration._upsert_users.assert_called_once()


def test_migrate_posts(monkeypatch):
    docs = [
        FakeDoc(
            doc_id="post-1",
            data={
                "userId": "user-1",
                "judul": "Judul",
            },
        )
    ]

    fake_db = MagicMock()
    fake_db.collection.return_value.stream.return_value = docs

    monkeypatch.setattr(migration, "db", fake_db)
    monkeypatch.setattr(migration, "_upsert_posts", MagicMock())
    monkeypatch.setattr(migration, "_update_progress_done", MagicMock())
    monkeypatch.setattr(migration, "_update_progress_error", MagicMock())

    result = _migrate_posts(
        collection_name="kawanss",
        neo4j_label="FirebaseKawanSS",
        progress_key="kawanss",
    )

    assert result == 1
    migration._upsert_posts.assert_called_once()


def test_migrate_comments(monkeypatch):
    docs = [
        FakeDoc(
            doc_id="comment-1",
            data={
                "userId": "user-1",
                "postId": "post-1",
                "text": "Komentar",
            },
        )
    ]

    fake_db = MagicMock()
    fake_db.collection.return_value.stream.return_value = docs

    monkeypatch.setattr(migration, "db", fake_db)
    monkeypatch.setattr(migration, "_upsert_comments", MagicMock())
    monkeypatch.setattr(migration, "_update_progress_done", MagicMock())
    monkeypatch.setattr(migration, "_update_progress_error", MagicMock())

    result = _migrate_comments()

    assert result == 1
    migration._upsert_comments.assert_called_once()


def test_run_full_migration_sync(monkeypatch):
    monkeypatch.setattr(migration, "_ensure_neo4j_constraints", MagicMock())
    monkeypatch.setattr(migration, "_migrate_users", MagicMock(return_value=1))
    monkeypatch.setattr(migration, "_migrate_posts", MagicMock(return_value=2))
    monkeypatch.setattr(migration, "_migrate_comments", MagicMock(return_value=3))
    monkeypatch.setattr(migration, "_update_status_doc", MagicMock())

    _run_full_migration_sync()

    migration._ensure_neo4j_constraints.assert_called_once()
    migration._migrate_users.assert_called_once()
    assert migration._migrate_posts.call_count == 2
    migration._migrate_comments.assert_called_once()


@pytest.mark.asyncio
async def test_run_migration_background_success(monkeypatch):
    monkeypatch.setattr(migration, "_update_status_doc", MagicMock())
    monkeypatch.setattr(migration, "_run_full_migration_sync", MagicMock())

    async def fake_to_thread(func):
        func()

    monkeypatch.setattr(migration.asyncio, "to_thread", fake_to_thread)

    await run_migration_background()

    assert migration._update_status_doc.call_count >= 2
    migration._run_full_migration_sync.assert_called_once()


@pytest.mark.asyncio
async def test_run_migration_background_failed(monkeypatch):
    monkeypatch.setattr(migration, "_update_status_doc", MagicMock())

    def raise_error():
        raise Exception("migration failed")

    monkeypatch.setattr(migration, "_run_full_migration_sync", raise_error)

    async def fake_to_thread(func):
        func()

    monkeypatch.setattr(migration.asyncio, "to_thread", fake_to_thread)

    await run_migration_background()

    last_call_data = migration._update_status_doc.call_args.args[0]
    assert last_call_data["status"] == "failed"
    assert "migration failed" in last_call_data["last_error"]