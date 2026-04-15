import pytest
from fastapi import HTTPException
from unittest.mock import MagicMock, patch
from app.controllers.user_controller import get_user_by_id

@pytest.mark.asyncio
@patch("app.controllers.user_controller.db") # <-- KUNCI PERBAIKAN: Mock langsung ke controllernos
async def test_get_user_by_id_found(mock_db):
    # Arrange: Mock kembalian Firestore
    mock_doc = MagicMock()
    mock_doc.exists = True
    mock_doc.to_dict.return_value = {"nama": "Jonathan", "email": "jonathan@test.com"}
    mock_db.collection.return_value.document.return_value.get.return_value = mock_doc

    # Act
    result = await get_user_by_id("user_123")

    # Assert
    assert result["nama"] == "Jonathan"
    assert result["email"] == "jonathan@test.com"

@pytest.mark.asyncio
@patch("app.controllers.user_controller.db")
async def test_get_user_by_id_not_found(mock_db):
    # Arrange
    mock_doc = MagicMock()
    mock_doc.exists = False
    mock_db.collection.return_value.document.return_value.get.return_value = mock_doc

    # Act & Assert
    with pytest.raises(HTTPException) as excinfo:
        await get_user_by_id("user_999")
        
    assert excinfo.value.status_code == 404