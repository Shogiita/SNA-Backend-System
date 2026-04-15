import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from fastapi import HTTPException

# 1. Test Export Excel (Sumber: App/Neo4j)
@pytest.mark.asyncio
@patch("app.controllers.integration_controller.neo4j_driver.session")
async def test_export_excel_app_source(mock_session, api_client):
    # Arrange: Mock kembalian query Neo4j
    mock_tx = MagicMock()
    mock_tx.run.return_value.data.return_value = [
        {
            "Post_Author": "Budi", "Target_Post_ID": "post_1", 
            "Post_Content": "Halo Dunia", "Upload_Date": 1700000000000,
            "Post_Likes": 10, "Post_Views": 50, "Post_Comments": 2, "Post_Shares": 0,
            "Comment_Author": "Andi", "Comment_Content": "Keren!", "Comment_Likes": 1,
            "Comment_Replies_Count": 0
        }
    ]
    mock_session.return_value.__enter__.return_value = mock_tx

    # Act
    payload = {"source": "app", "export_all": True}
    response = api_client.post("/integration/export/excel", json=payload)

    # Assert
    assert response.status_code == 200
    assert response.headers["content-disposition"].startswith("attachment; filename=SNA_Export_APP.xlsx")

# 2. Test Link to Google Sheets (Mocking GSpread)
@pytest.mark.asyncio
@patch("app.controllers.integration_controller.get_gspread_client")
@patch("app.controllers.integration_controller.db")
@patch("app.controllers.integration_controller.neo4j_driver.session")
async def test_link_to_sheets_new(mock_session, mock_db, mock_gspread, api_client):
    # Arrange: Mock Gspread Client
    mock_gc = MagicMock()
    mock_sh = MagicMock()
    mock_sh.id = "sheet_123"
    mock_sh.url = "https://docs.google.com/spreadsheets/d/123"
    mock_gc.create.return_value = mock_sh
    mock_gspread.return_value = mock_gc

    # Mock DB Firestore untuk simpan metadata
    mock_doc_ref = MagicMock()
    mock_doc_ref.id = "doc_123"
    mock_db.collection.return_value.add.return_value = (None, mock_doc_ref)

    # Mock Neo4j untuk Master Dataframe
    mock_tx = MagicMock()
    mock_tx.run.return_value.data.return_value = [{"Post_Author": "A", "Target_Post_ID": "1", "Post_Content": "X", "Upload_Date": 0, "Post_Likes": 0, "Post_Views": 0, "Post_Comments": 0, "Post_Shares": 0, "Comment_Author": "", "Comment_Content": "", "Comment_Likes": 0, "Comment_Replies_Count": 0}]
    mock_session.return_value.__enter__.return_value = mock_tx

    # Act
    payload = {"source": "app", "email": "test@test.com", "export_all": True}
    response = api_client.post("/integration/sheets/link", json=payload)

    # Assert
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "success"
    assert data["data"]["id"] == "doc_123"
    mock_gc.create.assert_called_once()
    mock_sh.share.assert_called_with("test@test.com", perm_type='user', role='writer')

# 3. Test Ambil Daftar Linked Sheets
@pytest.mark.asyncio
@patch("app.controllers.integration_controller.db")
async def test_get_linked_sheets(mock_db, api_client):
    mock_doc = MagicMock()
    mock_doc.id = "doc_1"
    mock_doc.to_dict.return_value = {"sheet_name": "SNA_Dataset", "source_type": "app"}
    mock_db.collection.return_value.order_by.return_value.stream.return_value = [mock_doc]

    response = api_client.get("/integration/sheets/linked")
    assert response.status_code == 200
    assert len(response.json()["data"]) == 1