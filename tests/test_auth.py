# import pytest
# from unittest.mock import patch, MagicMock, mock_open
# from fastapi import HTTPException

# @pytest.mark.asyncio
# @patch("app.controllers.auth_controller.httpx.AsyncClient.get")
# @patch("builtins.open", new_callable=mock_open, read_data="INSTAGRAM_ACCESS_TOKEN=old_token\n")
# @patch("os.path.exists", return_value=True)
# async def test_refresh_token_success(mock_exists, mock_file, mock_get, api_client):
#     # Arrange
#     mock_resp = MagicMock()
#     mock_resp.status_code = 200
#     mock_resp.json.return_value = {"access_token": "new_long_lived_token", "expires_in": 5184000}
#     mock_get.return_value = mock_resp

#     # Act
#     response = api_client.get("/auth/refresh-token")

#     # Assert
#     assert response.status_code == 200
#     assert response.json()["status"] == "success"
#     assert response.json()["expires_in_days"] == 60.0
#     mock_file.assert_called() # Pastikan file .env ditulis ulang

# @pytest.mark.asyncio
# @patch("app.controllers.auth_controller.httpx.AsyncClient.get")
# async def test_refresh_token_failed(mock_get, api_client):
#     # Arrange
#     mock_resp = MagicMock()
#     mock_resp.status_code = 400
#     mock_resp.json.return_value = {"error": {"message": "Token expired"}}
#     mock_get.return_value = mock_resp

#     # Act
#     response = api_client.get("/auth/refresh-token")

#     # Assert
#     assert response.status_code == 400
#     assert "Gagal refresh token" in response.json()["detail"]