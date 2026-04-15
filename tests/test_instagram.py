import pytest
from fastapi import HTTPException
from unittest.mock import patch, MagicMock
from app.controllers.instagram_controller import get_user_profile

@pytest.mark.asyncio
@patch("app.controllers.instagram_controller.httpx.AsyncClient.get")
async def test_get_user_profile_success(mock_get):
    # Arrange: Buat mock response HTTPX
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": "123", "username": "suarasurabaya", "followers_count": 5000}
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response

    # Act
    result = await get_user_profile()

    # Assert
    assert result["username"] == "suarasurabaya"
    assert result["followers_count"] == 5000
    mock_get.assert_called_once()