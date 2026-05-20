from unittest.mock import patch


def test_import_sheets_success(api_client):
    expected = {
        "status": "success",
        "source_active": "app",
        "total_rows": 1,
        "columns": ["interaction_type", "source_user"],
        "data_preview": [],
    }

    payload = {
        "source": "app",
        "spreadsheet_id": "sheet_123",
        "worksheet_name": "Sheet1",
        "google_access_token": "token",
    }

    with patch(
        "app.routers.integration_router.integration_controller.import_sheets",
        return_value=expected,
    ) as mock_controller:
        response = api_client.post("/integration/import/sheets", json=payload)

    assert response.status_code == 200
    assert response.json() == expected

    called_payload = mock_controller.call_args.args[0]
    called_admin = mock_controller.call_args.kwargs["current_admin"]

    assert called_payload.source == "app"
    assert called_payload.spreadsheet_id == "sheet_123"
    assert called_admin is None


def test_export_existing_sheets_success(api_client):
    expected = {
        "status": "success",
        "message": "Data berhasil diexport ke Google Sheets.",
        "spreadsheet_url": "https://docs.google.com/spreadsheets/d/test",
    }

    payload = {
        "source": "instagram",
        "selected_columns": ["likes"],
        "export_all": False,
        "spreadsheet_id": "sheet_123",
        "worksheet_name": "Export Data",
        "start_date": "2026-01-01",
        "end_date": "2026-01-31",
    }

    with patch(
        "app.routers.integration_router.integration_controller.export_existing_sheets",
        return_value=expected,
    ) as mock_controller:
        response = api_client.post(
            "/integration/export/sheets/existing",
            json=payload,
        )

    assert response.status_code == 200
    assert response.json() == expected

    called_payload = mock_controller.call_args.args[0]
    called_admin = mock_controller.call_args.kwargs["current_admin"]

    assert called_payload.source == "instagram"
    assert called_payload.selected_columns == ["likes"]
    assert called_payload.export_all is False
    assert called_payload.spreadsheet_id == "sheet_123"
    assert called_admin is None