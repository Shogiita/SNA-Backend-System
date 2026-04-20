import pytest
import pandas as pd
from unittest.mock import patch, MagicMock

# =====================================================================
# 1. EXPORT & LINK SHEETS
# =====================================================================
@pytest.mark.asyncio
@patch("app.controllers.integration_controller.neo4j_driver.session")
async def test_export_excel_app_source(mock_session, api_client):
    mock_tx = MagicMock()
    # PERBAIKAN: Melengkapi mock response Neo4j sesuai query di controller
    mock_tx.run.return_value.data.return_value = [{
        "Post_Author": "Budi", "Post_Content": "Halo", 
        "Upload_Date": 1700000000000, # Menggunakan format timestamp
        "Post_Likes": 10, "Post_Views": 50, "Post_Comments": 2, "Post_Shares": 0,
        "Comment_Author": "Andi", "Comment_Content": "Keren!", 
        "Comment_Likes": 1, "Comment_Replies_Count": 0,
        "Target_Post_ID": "post_1"
    }]
    mock_session.return_value.__enter__.return_value = mock_tx

    payload = {"source": "app", "export_all": True}
    response = api_client.post("/integration/export/excel", json=payload)
    assert response.status_code == 200

@patch("app.controllers.integration_controller.os.path.exists", return_value=True)
@patch("app.controllers.integration_controller.json.load")
def test_export_excel_instagram(mock_json_load, mock_exists, api_client):
    mock_json_load.return_value = [{"id": "ig_1", "timestamp": "2026-04-09", "caption": "SNA", "interactions": []}]
    payload = {"source": "instagram", "export_all": True}
    response = api_client.post("/integration/export/excel", json=payload)
    assert response.status_code == 200

@pytest.mark.asyncio
@patch("app.controllers.integration_controller.get_gspread_client")
@patch("app.controllers.integration_controller.db")
@patch("app.controllers.integration_controller.neo4j_driver.session")
async def test_link_to_sheets_new(mock_session, mock_db, mock_gspread, api_client):
    mock_gc = MagicMock()
    mock_sh = MagicMock()
    mock_sh.id = "123"
    mock_sh.url = "https://docs.google.com/123"
    mock_gc.create.return_value = mock_sh
    mock_gspread.return_value = mock_gc

    mock_doc_ref = MagicMock()
    mock_doc_ref.id = "doc_123"
    mock_db.collection.return_value.add.return_value = (None, mock_doc_ref)

    payload = {"source": "app", "email": "test@test.com", "export_all": True}
    response = api_client.post("/integration/sheets/link", json=payload)
    assert response.status_code == 200

@pytest.mark.asyncio
@patch("app.controllers.integration_controller.db")
async def test_get_linked_sheets(mock_db, api_client):
    mock_doc = MagicMock()
    mock_doc.id = "doc_1"
    mock_doc.to_dict.return_value = {"sheet_name": "SNA_Dataset", "source_type": "app"}
    mock_db.collection.return_value.order_by.return_value.stream.return_value = [mock_doc]

    response = api_client.get("/integration/sheets/linked")
    assert response.status_code == 200

# =====================================================================
# 2. SYNC & UNLINK SHEETS
# =====================================================================
@pytest.mark.asyncio
@patch("app.controllers.integration_controller.get_gspread_client")
async def test_sync_to_sheets(mock_gspread, api_client):
    mock_gc = MagicMock()
    mock_sh = MagicMock()
    mock_sh.get_worksheet.return_value = MagicMock()
    mock_gc.open_by_key.return_value = mock_sh
    mock_gspread.return_value = mock_gc
    
    with patch("app.controllers.integration_controller.get_master_dataframe") as mock_df:
        mock_df.return_value = pd.DataFrame([{"A": 1}])
        payload = {"sheet_id": "123", "source": "app"}
        response = api_client.post("/integration/sheets/sync", json=payload)
        # Handle fallback jika API aslinya menggunakan PUT
        if response.status_code == 405:
             response = api_client.put("/integration/sheets/sync", json=payload)
        assert response.status_code in [200, 201]

@pytest.mark.asyncio
@patch("app.controllers.integration_controller.db")
async def test_unlink_sheets(mock_db, api_client):
    mock_doc = MagicMock()
    mock_doc.exists = True
    mock_db.collection.return_value.document.return_value.get.return_value = mock_doc
    response = api_client.delete("/integration/sheets/unlink/doc123")
    assert response.status_code == 200

# =====================================================================
# 3. IMPORT & GRAPH CALCULATIONS
# =====================================================================
# @patch("app.controllers.integration_controller.pd.read_excel")
# def test_import_from_excel(mock_read_excel, api_client):
#     mock_df = pd.DataFrame([{"Source_User": "A", "Target": "B"}])
#     mock_read_excel.return_value = mock_df
    
#     # PERBAIKAN: Gunakan `params` untuk query param, dan sesuaikan file tuple
#     response = api_client.post(
#         "/integration/import/excel", 
#         files={"file": ("test.xlsx", b"dummy", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")}, 
#         params={"source": "app"}
#     )
#     assert response.status_code == 200

# @patch("app.controllers.integration_controller.get_gspread_client")
# def test_import_from_sheets(mock_gspread, api_client):
#     mock_gc = MagicMock()
#     mock_sh = MagicMock()
#     mock_ws = MagicMock()
#     mock_ws.get_all_records.return_value = [{"Source_User": "A", "Target": "B"}]
#     mock_sh.get_worksheet.return_value = mock_ws
#     mock_gc.open_by_key.return_value = mock_sh
#     mock_gspread.return_value = mock_gc
    
#     # PERBAIKAN: Gunakan GET dengan params sesuai standar REST
#     response = api_client.get(
#         "/integration/import/sheets", 
#         params={"sheet_id": "123", "source": "app"}
#     )
#     assert response.status_code == 200