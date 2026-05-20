from unittest.mock import MagicMock

import httpx
import pytest
from fastapi import HTTPException, status

from app.controllers import instagram_controller


class MockAsyncClient:
    def __init__(self, response=None, exception=None, *args, **kwargs):
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


@pytest.mark.asyncio
async def test_check_config_missing_business_account_id(monkeypatch):
    monkeypatch.setattr(instagram_controller.config, "IG_BUSINESS_ACCOUNT_ID", None)
    monkeypatch.setattr(instagram_controller.config, "IG_ACCESS_TOKEN", "token")

    with pytest.raises(HTTPException) as excinfo:
        await instagram_controller._check_config()

    assert excinfo.value.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
    assert "IG_BUSINESS_ACCOUNT_ID" in excinfo.value.detail


@pytest.mark.asyncio
async def test_check_config_missing_access_token(monkeypatch):
    monkeypatch.setattr(instagram_controller.config, "IG_BUSINESS_ACCOUNT_ID", "business_id")
    monkeypatch.setattr(instagram_controller.config, "IG_ACCESS_TOKEN", None)

    with pytest.raises(HTTPException) as excinfo:
        await instagram_controller._check_config()

    assert excinfo.value.status_code == status.HTTP_401_UNAUTHORIZED
    assert "IG_ACCESS_TOKEN" in excinfo.value.detail


@pytest.mark.asyncio
async def test_make_ig_api_request_success(monkeypatch):
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = {
        "id": "123",
        "username": "suarasurabaya",
    }

    monkeypatch.setattr(instagram_controller.config, "IG_BUSINESS_ACCOUNT_ID", "business_id")
    monkeypatch.setattr(instagram_controller.config, "IG_ACCESS_TOKEN", "token")
    monkeypatch.setattr(instagram_controller.config, "GRAPH_API_URL", "https://graph.facebook.com/v19.0")

    monkeypatch.setattr(
        instagram_controller.httpx,
        "AsyncClient",
        lambda *args, **kwargs: MockAsyncClient(response=mock_response),
    )

    result = await instagram_controller._make_ig_api_request(
        endpoint="/business_id",
        params={"fields": "id,username"},
    )

    assert result == {
        "id": "123",
        "username": "suarasurabaya",
    }


@pytest.mark.asyncio
async def test_make_ig_api_request_http_status_error(monkeypatch):
    request = httpx.Request("GET", "https://graph.facebook.com/v19.0/test")
    response = httpx.Response(
        status_code=401,
        json={
            "error": {
                "message": "Invalid OAuth access token.",
            }
        },
        request=request,
    )

    http_error = httpx.HTTPStatusError(
        message="401 Unauthorized",
        request=request,
        response=response,
    )

    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = http_error

    monkeypatch.setattr(instagram_controller.config, "IG_BUSINESS_ACCOUNT_ID", "business_id")
    monkeypatch.setattr(instagram_controller.config, "IG_ACCESS_TOKEN", "token")

    monkeypatch.setattr(
        instagram_controller.httpx,
        "AsyncClient",
        lambda *args, **kwargs: MockAsyncClient(response=mock_response),
    )

    with pytest.raises(HTTPException) as excinfo:
        await instagram_controller._make_ig_api_request("business_id")

    assert excinfo.value.status_code == 401
    assert "Invalid OAuth access token" in excinfo.value.detail


@pytest.mark.asyncio
async def test_make_ig_api_request_request_error(monkeypatch):
    request = httpx.Request("GET", "https://graph.facebook.com/v19.0/test")
    request_error = httpx.RequestError("Connection failed", request=request)

    monkeypatch.setattr(instagram_controller.config, "IG_BUSINESS_ACCOUNT_ID", "business_id")
    monkeypatch.setattr(instagram_controller.config, "IG_ACCESS_TOKEN", "token")

    monkeypatch.setattr(
        instagram_controller.httpx,
        "AsyncClient",
        lambda *args, **kwargs: MockAsyncClient(exception=request_error),
    )

    with pytest.raises(HTTPException) as excinfo:
        await instagram_controller._make_ig_api_request("business_id")

    assert excinfo.value.status_code == status.HTTP_503_SERVICE_UNAVAILABLE
    assert "Gagal menghubungi Instagram API" in excinfo.value.detail


@pytest.mark.asyncio
async def test_get_user_profile_success(monkeypatch):
    expected = {
        "id": "123",
        "username": "suarasurabaya",
    }

    async def mock_make_request(endpoint, params=None):
        assert endpoint == "business_id"
        assert "fields" in params
        return expected

    monkeypatch.setattr(instagram_controller.config, "IG_BUSINESS_ACCOUNT_ID", "business_id")
    monkeypatch.setattr(instagram_controller, "_make_ig_api_request", mock_make_request)

    result = await instagram_controller.get_user_profile()

    assert result == expected