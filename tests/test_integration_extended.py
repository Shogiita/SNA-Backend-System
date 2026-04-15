import pytest
import pandas as pd
from unittest.mock import patch, MagicMock

# =====================================================================
# 1. TEST UNLINK / HAPUS TAUTAN SPREADSHEET
# =====================================================================
@pytest.mark.asyncio
@patch("app.controllers.integration_controller.get_gspread_client")
@patch("app.controllers.integration_controller.db")
async def test_unlink_sheets_success(mock_db, mock_gspread, api_client):
    # Arrange
    mock_doc = MagicMock()
    mock_doc.exists = True
    mock_doc.to_dict.return_value = {"sheet_id": "google_sheet_123"}
    
    # Rantai mock Firestore: db.collection().document().get()
    mock_doc_ref = MagicMock()
    mock_doc_ref.get.return_value = mock_doc
    mock_db.collection.return_value.document.return_value = mock_doc_ref

    mock_gc = MagicMock()
    mock_gspread.return_value = mock_gc

    # Act
    response = api_client.delete("/integration/sheets/unlink/doc_123")

    # Assert
    assert response.status_code == 200
    assert response.json()["status"] == "success"
    # Memastikan file fisik di Drive dihapus & dokumen di DB dihapus
    mock_gc.del_spreadsheet.assert_called_with("google_sheet_123")
    mock_doc_ref.delete.assert_called_once()


# =====================================================================
# 2. TEST UNLINK ERROR (DOKUMEN TIDAK DITEMUKAN)
# =====================================================================
@pytest.mark.asyncio
@patch("app.controllers.integration_controller.db")
async def test_unlink_sheets_not_found(mock_db, api_client):
    # Arrange
    mock_doc = MagicMock()
    mock_doc.exists = False
    mock_db.collection.return_value.document.return_value.get.return_value = mock_doc

    # Act
    response = api_client.delete("/integration/sheets/unlink/doc_999")
    
    # Assert
    assert response.status_code == 404


# =====================================================================
# 3. TEST IMPORT EXCEL & REKONSTRUKSI JARINGAN (FIX 400 ERROR)
# =====================================================================
@pytest.mark.asyncio
@patch("app.controllers.integration_controller.pd.read_excel")
@patch("app.controllers.integration_controller._calculate_graph_from_dataframe")
async def test_import_excel_endpoint(mock_calc, mock_read_excel, api_client):
    # Arrange: Mock hasil kalkulasi algoritma
    mock_calc.return_value = {"meta": {"total_nodes": 5}, "graph_data": {}}
    
    # Arrange: Mock pandas agar tidak membaca byte string dummy sebagai file Excel sungguhan
    mock_read_excel.return_value = pd.DataFrame() 
    
    # Simulasi file upload
    dummy_file_content = b"dummy content"
    files = {"file": ("dataset.xlsx", dummy_file_content, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")}
    
    # Act
    response = api_client.post("/integration/import/excel", files=files)
    
    # Assert
    assert response.status_code == 200
    mock_read_excel.assert_called_once()
    mock_calc.assert_called_once()


# =====================================================================
# 4. TEST IMPORT FROM GOOGLE SHEETS
# =====================================================================
@pytest.mark.asyncio
@patch("app.controllers.integration_controller.get_gspread_client")
@patch("app.controllers.integration_controller._calculate_graph_from_dataframe")
async def test_import_sheets_endpoint(mock_calc, mock_gspread, api_client):
    # Arrange: Mock hasil algoritma
    mock_calc.return_value = {"meta": {"total_nodes": 10}, "graph_data": {}}
    
    # Arrange: Mock Gspread untuk simulasi menarik data dari Google Sheets
    mock_gc = MagicMock()
    mock_sh = MagicMock()
    mock_worksheet = MagicMock()
    
    mock_worksheet.get_all_records.return_value = [{"Source_User": "A", "Target": "B"}]
    mock_sh.get_worksheet.return_value = mock_worksheet
    mock_gc.open_by_key.return_value = mock_sh
    mock_gspread.return_value = mock_gc

    # Act
    response = api_client.get("/integration/import/sheets?sheet_id=12345")

    # Assert
    assert response.status_code == 200
    mock_gspread.assert_called_once()
    mock_calc.assert_called_once()