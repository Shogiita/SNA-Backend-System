from unittest.mock import patch

from fastapi import HTTPException


def test_export_csv_success(api_client, mock_admin):
    expected = {
        "status": "success",
        "message": "CSV berhasil dibuat.",
        "filename": "export.csv",
    }

    payload = {
        "source": "app",
        "export_all": False,
        "selected_columns": ["likes", "comment"],
        "start_date": "2026-01-01",
        "end_date": "2026-01-31",
    }

    with patch(
        "app.routers.integration_router.integration_controller.export_csv",
        return_value=expected,
    ) as mock_controller:
        response = api_client.post("/integration/export/csv", json=payload)

    assert response.status_code == 200
    assert response.json() == expected

    called_payload, called_admin = mock_controller.call_args.args
    assert called_payload.source == "app"
    assert called_payload.export_all is False
    assert called_payload.selected_columns == ["likes", "comment"]
    assert called_admin == mock_admin


def test_export_sheets_success(api_client, mock_admin):
    expected = {
        "status": "success",
        "message": "Data berhasil diexport ke Google Sheets.",
        "spreadsheet_url": "https://docs.google.com/spreadsheets/d/test",
    }

    payload = {
        "source": "instagram",
        "export_all": True,
        "spreadsheet_title": "SNA Instagram Export",
        "google_access_token": "google-token-test",
    }

    with patch(
        "app.routers.integration_router.integration_controller.export_sheets",
        return_value=expected,
    ) as mock_controller:
        response = api_client.post("/integration/export/sheets", json=payload)

    assert response.status_code == 200
    assert response.json() == expected

    called_payload, called_admin = mock_controller.call_args.args
    assert called_payload.source == "instagram"
    assert called_payload.spreadsheet_title == "SNA Instagram Export"
    assert called_admin == mock_admin


def test_get_linked_sheets_success(api_client, mock_admin):
    expected = {
        "status": "success",
        "data": [
            {
                "doc_id": "doc_1",
                "sheet_name": "SNA Export",
                "source": "app",
            }
        ],
    }

    with patch(
        "app.routers.integration_router.integration_controller.get_linked_sheets",
        return_value=expected,
    ) as mock_controller:
        response = api_client.get("/integration/sheets/linked")

    assert response.status_code == 200
    assert response.json() == expected
    mock_controller.assert_called_once_with(mock_admin)


def test_unlink_sheet_success(api_client, mock_admin):
    expected = {
        "status": "success",
        "message": "Spreadsheet berhasil di-unlink.",
    }

    with patch(
        "app.routers.integration_router.integration_controller.unlink_sheet",
        return_value=expected,
    ) as mock_controller:
        response = api_client.delete("/integration/sheets/unlink/doc_123")

    assert response.status_code == 200
    assert response.json() == expected
    mock_controller.assert_called_once_with("doc_123", mock_admin)


def test_unlink_sheet_not_found(api_client):
    with patch(
        "app.routers.integration_router.integration_controller.unlink_sheet",
        side_effect=HTTPException(status_code=404, detail="Dokumen tidak ditemukan."),
    ):
        response = api_client.delete("/integration/sheets/unlink/doc_999")

    assert response.status_code == 404
    assert response.json()["detail"] == "Dokumen tidak ditemukan."


def test_export_csv_rejects_invalid_source(api_client):
    payload = {
        "source": "twitter",
        "export_all": True,
    }

    response = api_client.post("/integration/export/csv", json=payload)

    assert response.status_code == 422