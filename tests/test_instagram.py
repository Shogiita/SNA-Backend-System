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
        side_effect=HTTPException(
            status_code=401,
            detail="IG_ACCESS_TOKEN belum di-generate atau belum diatur di server.",
        ),
    ):
        response = api_client.get("/instagram/profile")

    assert response.status_code == 401
    assert "IG_ACCESS_TOKEN" in response.json()["detail"]