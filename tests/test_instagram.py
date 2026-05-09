from unittest.mock import AsyncMock, patch

from fastapi import HTTPException


def test_get_instagram_profile_success(api_client):
    expected = {
        "id": "123",
        "username": "suarasurabaya",
        "followers_count": 5000,
    }

    with patch(
        "app.routers.instagram_router.instagram_controller.get_user_profile",
        new_callable=AsyncMock,
        return_value=expected,
    ) as mock_controller:
        response = api_client.get("/instagram/profile")

    assert response.status_code == 200
    assert response.json() == expected
    mock_controller.assert_awaited_once()


def test_get_instagram_profile_error(api_client):
    with patch(
        "app.routers.instagram_router.instagram_controller.get_user_profile",
        new_callable=AsyncMock,
        side_effect=HTTPException(status_code=401, detail="IG_ACCESS_TOKEN belum di-generate atau belum diatur di server."),
    ):
        response = api_client.get("/instagram/profile")

    assert response.status_code == 401
    assert "IG_ACCESS_TOKEN" in response.json()["detail"]


def test_get_instagram_media_success(api_client):
    expected = {
        "data": [
            {
                "id": "media_1",
                "caption": "Post test",
                "like_count": 10,
            }
        ]
    }

    with patch(
        "app.routers.instagram_router.instagram_controller.get_user_media",
        new_callable=AsyncMock,
        return_value=expected,
        create=True,
    ) as mock_controller:
        response = api_client.get("/instagram/media?limit=5")

    assert response.status_code == 200
    assert response.json() == expected
    mock_controller.assert_awaited_once_with(limit=5)


def test_get_instagram_media_invalid_limit(api_client):
    response = api_client.get("/instagram/media?limit=101")

    assert response.status_code == 422


def test_debug_token_success(api_client):
    expected = {
        "message": "Ini adalah token yang dibaca server dari file .env Anda:",
        "token_length": 15,
        "first_5_chars": "abcde",
        "last_5_chars": "vwxyz",
        "loaded_token": "abcde12345vwxyz",
    }

    with patch(
        "app.routers.instagram_router.instagram_controller.debug_token",
        new_callable=AsyncMock,
        return_value=expected,
    ) as mock_controller:
        response = api_client.get("/instagram/debug-token")

    assert response.status_code == 200
    assert response.json() == expected
    mock_controller.assert_awaited_once()