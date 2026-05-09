import os
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException

from app.controllers import auth_controller


class MockAsyncClient:
    def __init__(self, response=None, exception=None):
        self.response = response
        self.exception = exception

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def get(self, url, params=None):
        if self.exception:
            raise self.exception
        return self.response


def test_update_env_file_updates_existing_key(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    env_file = tmp_path / ".env"
    env_file.write_text(
        "INSTAGRAM_ACCESS_TOKEN=old_token\nOTHER_KEY=value\n",
        encoding="utf-8",
    )

    auth_controller.update_env_file("INSTAGRAM_ACCESS_TOKEN", "new_token")

    content = env_file.read_text(encoding="utf-8")
    assert "INSTAGRAM_ACCESS_TOKEN=new_token" in content
    assert "OTHER_KEY=value" in content


def test_update_env_file_adds_key_when_missing(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    env_file = tmp_path / ".env"
    env_file.write_text("OTHER_KEY=value\n", encoding="utf-8")

    auth_controller.update_env_file("INSTAGRAM_ACCESS_TOKEN", "new_token")

    content = env_file.read_text(encoding="utf-8")
    assert "OTHER_KEY=value" in content
    assert "INSTAGRAM_ACCESS_TOKEN=new_token" in content


def test_update_env_file_does_nothing_when_env_missing(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    auth_controller.update_env_file("INSTAGRAM_ACCESS_TOKEN", "new_token")

    assert not (tmp_path / ".env").exists()


@pytest.mark.asyncio
async def test_refresh_instagram_token_success(monkeypatch):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "access_token": "new_long_lived_token_12345",
        "expires_in": 5184000,
    }

    monkeypatch.setattr(auth_controller.config, "IG_ACCESS_TOKEN", "old_token")
    monkeypatch.setattr(auth_controller.config, "IG_APP_ID", "app_id")
    monkeypatch.setattr(auth_controller.config, "IG_APP_SECRET", "app_secret")
    monkeypatch.setattr(auth_controller, "update_env_file", MagicMock())

    monkeypatch.setattr(
        auth_controller.httpx,
        "AsyncClient",
        lambda: MockAsyncClient(response=mock_response),
    )

    result = await auth_controller.refresh_instagram_token()

    assert result["status"] == "success"
    assert result["expires_in_days"] == 60
    assert result["preview"] == "new_long_lived..."
    assert auth_controller.config.IG_ACCESS_TOKEN == "new_long_lived_token_12345"
    auth_controller.update_env_file.assert_called_once_with(
        "INSTAGRAM_ACCESS_TOKEN",
        "new_long_lived_token_12345",
    )


@pytest.mark.asyncio
async def test_refresh_instagram_token_without_current_token(monkeypatch):
    monkeypatch.setattr(auth_controller.config, "IG_ACCESS_TOKEN", None)

    with pytest.raises(HTTPException) as excinfo:
        await auth_controller.refresh_instagram_token()

    assert excinfo.value.status_code == 400
    assert "Token tidak ditemukan" in excinfo.value.detail


@pytest.mark.asyncio
async def test_refresh_instagram_token_api_error_becomes_internal_error(monkeypatch):
    mock_response = MagicMock()
    mock_response.status_code = 400
    mock_response.json.return_value = {
        "error": {
            "message": "Token expired",
        }
    }

    monkeypatch.setattr(auth_controller.config, "IG_ACCESS_TOKEN", "old_token")
    monkeypatch.setattr(auth_controller.config, "IG_APP_ID", "app_id")
    monkeypatch.setattr(auth_controller.config, "IG_APP_SECRET", "app_secret")

    monkeypatch.setattr(
        auth_controller.httpx,
        "AsyncClient",
        lambda: MockAsyncClient(response=mock_response),
    )

    with pytest.raises(HTTPException) as excinfo:
        await auth_controller.refresh_instagram_token()

    # Controller saat ini menangkap HTTPException di except Exception,
    # sehingga status akhirnya menjadi 500.
    assert excinfo.value.status_code == 500
    assert "Token expired" in excinfo.value.detail


@pytest.mark.asyncio
async def test_refresh_instagram_token_no_access_token_in_success_response(monkeypatch):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "expires_in": 5184000,
    }

    monkeypatch.setattr(auth_controller.config, "IG_ACCESS_TOKEN", "old_token")
    monkeypatch.setattr(auth_controller.config, "IG_APP_ID", "app_id")
    monkeypatch.setattr(auth_controller.config, "IG_APP_SECRET", "app_secret")

    monkeypatch.setattr(
        auth_controller.httpx,
        "AsyncClient",
        lambda: MockAsyncClient(response=mock_response),
    )

    with pytest.raises(HTTPException) as excinfo:
        await auth_controller.refresh_instagram_token()

    assert excinfo.value.status_code == 400
    assert "Gagal memperbarui token" in excinfo.value.detail