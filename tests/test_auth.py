from unittest.mock import AsyncMock, patch

from fastapi import HTTPException


def test_refresh_token_endpoint_success(api_client):
    expected = {
        "status": "success",
        "message": "Instagram Access Token berhasil diperbarui.",
        "expires_in_days": 60,
        "preview": "new_token...",
    }

    with patch(
        "app.routers.auth_router.auth_controller.refresh_instagram_token",
        new_callable=AsyncMock,
        return_value=expected,
    ) as mock_refresh:
        response = api_client.get("/auth/refresh-token")

    assert response.status_code == 200
    assert response.json() == expected
    mock_refresh.assert_awaited_once()


def test_refresh_token_endpoint_error(api_client):
    with patch(
        "app.routers.auth_router.auth_controller.refresh_instagram_token",
        new_callable=AsyncMock,
        side_effect=HTTPException(
            status_code=400,
            detail="Token tidak ditemukan di konfigurasi.",
        ),
    ):
        response = api_client.get("/auth/refresh-token")

    assert response.status_code == 400
    assert response.json()["detail"] == "Token tidak ditemukan di konfigurasi."