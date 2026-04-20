import pytest
from unittest.mock import patch, MagicMock, mock_open
from fastapi import HTTPException
import httpx

# =====================================================================
# TEST: AUTH CONTROLLER (INSTAGRAM TOKEN)
# =====================================================================
@pytest.mark.asyncio
@patch("app.controllers.auth_controller.httpx.AsyncClient.get")
@patch("builtins.open", new_callable=mock_open, read_data="INSTAGRAM_ACCESS_TOKEN=old_token\n")
@patch("os.path.exists", return_value=True)
async def test_refresh_token_success(mock_exists, mock_file, mock_get, api_client):
    # Arrange
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"access_token": "new_long_lived_token", "expires_in": 5184000}
    mock_get.return_value = mock_resp

    # Act
    response = api_client.get("/auth/refresh-token")
    
    # Assert
    assert response.status_code == 200
    assert response.json()["status"] == "success"


@pytest.mark.asyncio
@patch("app.controllers.auth_controller.httpx.AsyncClient.get")
async def test_refresh_token_failed(mock_get, api_client):
    # Arrange
    mock_resp = MagicMock()
    mock_resp.status_code = 400
    mock_resp.json.return_value = {"error": {"message": "Token expired"}}
    
    # Mensimulasikan raise_for_status() jika digunakan oleh httpx
    def raise_err():
        raise httpx.HTTPStatusError("Error", request=MagicMock(), response=mock_resp)
    mock_resp.raise_for_status.side_effect = raise_err
    mock_get.return_value = mock_resp

    # Act
    response = api_client.get("/auth/refresh-token")
    
    # Assert (akan mengembalikan HTTP Exception dari FastAPI, biasanya 400/500)
    assert response.status_code >= 400


@patch("app.controllers.auth_controller.config")
def test_refresh_token_no_token(mock_config, api_client):
    # Arrange: kosongkan token
    mock_config.IG_ACCESS_TOKEN = None
    
    # Act
    response = api_client.get("/auth/refresh-token")
    
    # Assert
    assert response.status_code >= 400


# =====================================================================
# TEST: SECURITY.PY (API KEY VALIDATION)
# =====================================================================
@pytest.mark.asyncio
@patch("app.security.API_KEY", "test_key_valid")
async def test_get_api_key_valid():
    from app.security import get_api_key
    # Jika API key yang dikirim sama dengan config, harus return key tersebut
    res = await get_api_key("test_key_valid")
    assert res == "test_key_valid"


@pytest.mark.asyncio
@patch("app.security.API_KEY", "test_key_valid")
async def test_get_api_key_invalid():
    from app.security import get_api_key
    # Jika API key salah, harus raise HTTPException (403 Forbidden)
    with pytest.raises(HTTPException) as excinfo:
        await get_api_key("wrong_key")
    
    assert excinfo.value.status_code == 403