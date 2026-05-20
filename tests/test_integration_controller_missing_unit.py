from types import SimpleNamespace
from unittest.mock import MagicMock

import pandas as pd
import pytest
from fastapi import HTTPException

from app.controllers import integration_controller
from app.controllers.integration_controller import (
    _apply_date_filter,
    _build_csv_with_summary,
    _build_sheet_export_values,
    _first_worksheet,
    _format_export_date,
    _get_app_summary_rows,
    _get_export_dataframe,
    _get_export_summary_rows,
    _get_google_analytics_rows,
    _history_sort_key,
    _make_legacy_export_dataframe,
    _normalize_export_value,
    _normalize_firestore_datetime,
    _parse_to_datetime,
    _safe_get_nested,
    export_sheets,
    get_exported_sheets_history,
    get_gspread_client,
    get_gspread_user_client,
    get_master_dataframe,
)


def test_parse_to_datetime_valid_and_invalid_values():
    assert pd.notnull(_parse_to_datetime("2026-01-01"))
    assert pd.notnull(_parse_to_datetime("1704067200"))
    assert pd.isna(_parse_to_datetime(""))
    assert pd.isna(_parse_to_datetime(None))


def test_apply_date_filter_filters_tanggal_upload():
    df = pd.DataFrame(
        [
            {"Tanggal_Upload": "2026-01-01 10:00:00", "value": 1},
            {"Tanggal_Upload": "2026-02-01 10:00:00", "value": 2},
        ]
    )

    result = _apply_date_filter(
        df,
        start_date="2026-01-01",
        end_date="2026-01-31",
    )

    assert len(result) == 1
    assert result.iloc[0]["value"] == 1
    assert "Datetime_Obj" not in result.columns


def test_apply_date_filter_returns_original_when_missing_date_column():
    df = pd.DataFrame([{"value": 1}])

    result = _apply_date_filter(df)

    assert result.equals(df)


def test_safe_get_nested_success_and_default():
    data = {
        "a": {
            "b": {
                "c": 10,
            }
        }
    }

    assert _safe_get_nested(data, ["a", "b", "c"], 0) == 10
    assert _safe_get_nested(data, ["a", "x"], "-") == "-"


def test_normalize_export_value():
    assert _normalize_export_value(None) == ""
    assert _normalize_export_value({"a": 1}) == "{'a': 1}"
    assert _normalize_export_value([1, 2]) == "[1, 2]"
    assert _normalize_export_value("text") == "text"


def test_get_export_summary_rows(monkeypatch):
    payload = SimpleNamespace(
        source="app",
        start_date="2026-01-01",
        end_date="2026-01-31",
    )

    monkeypatch.setattr(
        integration_controller.report_controller,
        "get_stats_summary",
        lambda: {
            "data": {
                "users": {
                    "total": 10,
                    "new_this_month": 2,
                },
                "posts": {
                    "total": 5,
                    "total_infoss": 3,
                    "total_kawanss": 2,
                    "new_30_days": 4,
                    "new_30_days_kawanss": 1,
                },
            }
        },
    )

    monkeypatch.setattr(
        integration_controller.report_controller,
        "get_google_analytics_summary",
        lambda start_date=None, end_date=None: {
            "data": {
                "google_analytics": {
                    "summary": {
                        "monthly_active_users": 100,
                        "monthly_new_users": 50,
                        "monthly_total_users": 200,
                        "monthly_sessions": 300,
                        "monthly_engaged_sessions": 250,
                        "monthly_screen_page_views": 1000,
                        "monthly_event_count": 500,
                        "average_session_duration_seconds": 45,
                    },
                    "date_range": {
                        "start_date": start_date,
                        "end_date": end_date,
                    },
                }
            }
        },
    )

    rows = _get_export_summary_rows(payload)

    assert rows[0] == ["Section", "Metric", "Value"]
    assert ["Export Info", "Source", "APP"] in rows
    assert ["App Summary", "Total Pengguna App", 10] in rows
    assert ["Google Analytics", "Monthly Active User", 100] in rows


def test_build_csv_with_summary():
    summary_rows = [
        ["Section", "Metric", "Value"],
        ["Export Info", "Source", "APP"],
    ]

    df = pd.DataFrame(
        [
            {
                "Interaction_Type": "POST",
                "Source_User": "user_1",
            }
        ]
    )

    csv_result = _build_csv_with_summary(summary_rows, df)

    assert "DATASET EXPORT" in csv_result
    assert "Interaction_Type" in csv_result
    assert "user_1" in csv_result


def test_get_export_dataframe_success(monkeypatch):
    payload = SimpleNamespace(
        source="app",
        start_date=None,
        end_date=None,
        selected_columns=None,
        export_all=True,
    )

    legacy_df = pd.DataFrame(
        [
            {
                "Interaction_Type": "POST",
                "Source_User": "user_1",
                "Target": "post_1",
                "User_Pembuat_Post": "user_1",
                "Post": "content",
                "Tanggal_Upload": "2026-01-01",
            }
        ]
    )

    monkeypatch.setattr(
        integration_controller,
        "_make_legacy_export_dataframe",
        lambda source_type, start_date=None, end_date=None: legacy_df,
    )

    result = _get_export_dataframe(payload)

    assert not result.empty
    assert "interaction_type" in result.columns


def test_get_export_dataframe_empty_raises_404(monkeypatch):
    payload = SimpleNamespace(
        source="app",
        start_date=None,
        end_date=None,
        selected_columns=None,
        export_all=True,
    )

    monkeypatch.setattr(
        integration_controller,
        "_make_legacy_export_dataframe",
        lambda source_type, start_date=None, end_date=None: pd.DataFrame(),
    )

    with pytest.raises(HTTPException) as excinfo:
        _get_export_dataframe(payload)

    assert excinfo.value.status_code == 404


def test_make_legacy_export_dataframe_invalid_source():
    with pytest.raises(HTTPException) as excinfo:
        _make_legacy_export_dataframe("twitter")

    assert excinfo.value.status_code == 400


def test_get_master_dataframe(monkeypatch):
    legacy_df = pd.DataFrame(
        [
            {
                "Interaction_Type": "POST",
                "Community": 1,
                "Source_User": "user_1",
                "Target": "post_1",
                "Post_Link": "link",
                "User_Pembuat_Post": "user_1",
                "Post": "content",
                "Tanggal_Upload": "2026-01-01",
                "Jumlah_Like_Post": 10,
            }
        ]
    )

    monkeypatch.setattr(
        integration_controller,
        "_make_legacy_export_dataframe",
        lambda source_type, start_date=None, end_date=None: legacy_df,
    )

    result = get_master_dataframe(
        source_type="app",
        selected_columns=["Jumlah_Like_Post"],
        export_all=False,
    )

    assert "Post_Link" not in result.columns
    assert "Jumlah_Like_Post" in result.columns


def test_get_gspread_client_success(monkeypatch):
    fake_creds = object()
    fake_client = object()

    monkeypatch.setattr(
        integration_controller.Credentials,
        "from_service_account_info",
        lambda info, scopes=None: fake_creds,
    )
    monkeypatch.setattr(
        integration_controller.gspread,
        "authorize",
        lambda creds: fake_client,
    )

    result = get_gspread_client()

    assert result is fake_client


def test_get_gspread_client_error(monkeypatch):
    def raise_error(*args, **kwargs):
        raise Exception("credential error")

    monkeypatch.setattr(
        integration_controller.Credentials,
        "from_service_account_info",
        raise_error,
    )

    with pytest.raises(HTTPException) as excinfo:
        get_gspread_client()

    assert excinfo.value.status_code == 500


def test_get_gspread_user_client_missing_token():
    with pytest.raises(HTTPException) as excinfo:
        get_gspread_user_client("")

    assert excinfo.value.status_code == 401


def test_get_gspread_user_client_success(monkeypatch):
    fake_client = object()

    monkeypatch.setattr(
        integration_controller.gspread,
        "authorize",
        lambda creds: fake_client,
    )

    result = get_gspread_user_client("google-token")

    assert result is fake_client


def test_export_sheets_success(monkeypatch):
    payload = SimpleNamespace(
        source="app",
        spreadsheet_title="Test Sheet",
        google_access_token="google-token",
        start_date=None,
        end_date=None,
    )

    df = pd.DataFrame(
        [
            {
                "interaction_type": "POST",
                "source_user": "user_1",
            }
        ]
    )

    summary_worksheet = MagicMock()
    export_worksheet = MagicMock()

    spreadsheet = MagicMock()
    spreadsheet.id = "sheet-id"
    spreadsheet.url = "https://docs.google.com/spreadsheets/d/sheet-id"
    spreadsheet.sheet1 = summary_worksheet
    spreadsheet.add_worksheet.return_value = export_worksheet

    client = MagicMock()
    client.create.return_value = spreadsheet

    doc_ref = MagicMock()
    doc_ref.id = "doc-id"

    fake_db = MagicMock()
    fake_db.collection.return_value.add.return_value = (None, doc_ref)

    monkeypatch.setattr(integration_controller, "_get_export_dataframe", lambda payload: df)
    monkeypatch.setattr(integration_controller, "_get_export_summary_rows", lambda payload: [["Summary"]])
    monkeypatch.setattr(integration_controller, "get_gspread_user_client", lambda token: client)
    monkeypatch.setattr(integration_controller, "db", fake_db)

    result = export_sheets(
        payload,
        current_admin={
            "uid": "admin-uid",
            "email": "admin@test.com",
        },
    )

    assert result["status"] == "success"
    assert result["spreadsheet_url"] == spreadsheet.url
    assert result["data"]["id"] == "doc-id"


def test_export_sheets_without_google_token(monkeypatch):
    payload = SimpleNamespace(
        source="app",
        spreadsheet_title="Test Sheet",
        google_access_token=None,
    )

    monkeypatch.setattr(
        integration_controller,
        "_get_export_dataframe",
        lambda payload: pd.DataFrame([{"a": 1}]),
    )

    with pytest.raises(HTTPException) as excinfo:
        export_sheets(payload)

    assert excinfo.value.status_code == 401


def test_first_worksheet_existing():
    worksheet = MagicMock()
    spreadsheet = MagicMock()
    spreadsheet.get_worksheet.return_value = worksheet

    result = _first_worksheet(spreadsheet)

    assert result is worksheet


def test_first_worksheet_created_when_missing():
    worksheet = MagicMock()
    spreadsheet = MagicMock()
    spreadsheet.get_worksheet.return_value = None
    spreadsheet.add_worksheet.return_value = worksheet

    result = _first_worksheet(spreadsheet)

    assert result is worksheet
    spreadsheet.add_worksheet.assert_called_once()


def test_format_export_date():
    assert _format_export_date(None) == "-"
    assert _format_export_date("") == "-"
    assert _format_export_date("2026-01-01") == "2026-01-01"


def test_get_app_summary_rows(monkeypatch):
    class FakeResult:
        def __init__(self, value_key, value):
            self.value_key = value_key
            self.value = value

        def get(self, key, default=None):
            if key == self.value_key:
                return self.value
            return default

    class FakeSession:
        def __init__(self):
            self.counter = 0

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, traceback):
            return False

        def run(self, *args, **kwargs):
            self.counter += 1
            result_map = {
                1: FakeResult("total_users", 10),
                2: FakeResult("total_app_posts", 5),
                3: FakeResult("total_infoss_posts", 3),
                4: FakeResult("total_kawanss_posts", 2),
                5: FakeResult("app_posts_30_days", 4),
                6: FakeResult("kawanss_posts_30_days", 1),
                7: FakeResult("new_users_this_month", 2),
            }
            fake_result = result_map.get(self.counter)

            query_result = MagicMock()
            query_result.single.return_value = fake_result
            return query_result

    fake_session = FakeSession()
    fake_driver = MagicMock()
    fake_driver.session.return_value = fake_session

    monkeypatch.setattr(integration_controller, "neo4j_driver", fake_driver)

    rows = _get_app_summary_rows()

    assert ["App Summary", "Total Pengguna App", 10] in rows


def test_get_google_analytics_rows(monkeypatch):
    doc = MagicMock()
    doc.exists = True
    doc.to_dict.return_value = {
        "monthly_active_user": 100,
        "monthly_new_user": 20,
        "monthly_total_user": 120,
        "monthly_sessions": 300,
        "monthly_engaged_sessions": 250,
        "page_views": 1000,
        "event_count": 500,
        "average_session_duration_seconds": 45,
    }

    fake_db = MagicMock()
    fake_db.collection.return_value.document.return_value.get.return_value = doc

    monkeypatch.setattr(integration_controller, "db", fake_db)

    rows = _get_google_analytics_rows()

    assert ["Google Analytics", "Monthly Active User", 100] in rows


def test_build_sheet_export_values_app(monkeypatch):
    payload = SimpleNamespace(
        source="app",
        start_date="2026-01-01",
        end_date="2026-01-31",
    )

    df = pd.DataFrame(
        [
            {
                "interaction_type": "POST",
                "source_user": "user_1",
            }
        ]
    )

    monkeypatch.setattr(
        integration_controller,
        "_get_app_summary_rows",
        lambda: [["App Summary", "Total Pengguna App", 10]],
    )
    monkeypatch.setattr(
        integration_controller,
        "_get_google_analytics_rows",
        lambda: [["Google Analytics", "Monthly Active User", 100]],
    )

    values = _build_sheet_export_values(payload, df)

    assert ["Export Info", "Source", "APP"] in values
    assert ["App Summary", "Total Pengguna App", 10] in values
    assert ["Google Analytics", "Monthly Active User", 100] in values


def test_normalize_firestore_datetime():
    assert _normalize_firestore_datetime(None) == ""
    assert _normalize_firestore_datetime("2026-01-01") == "2026-01-01"


def test_history_sort_key():
    newer = _history_sort_key({"updated_at": "2026-01-02T00:00:00"})
    older = _history_sort_key({"updated_at": "2026-01-01T00:00:00"})

    assert newer > older
    assert _history_sort_key({"updated_at": "invalid-date"}) == 0


def test_get_exported_sheets_history(monkeypatch):
    doc = MagicMock()
    doc.id = "doc-1"
    doc.to_dict.return_value = {
        "sheet_id": "sheet-1",
        "sheet_url": "url",
        "sheet_name": "Sheet",
        "worksheet_name": "Export Data",
        "source_type": "app",
        "rows_count": 1,
        "columns": ["a"],
        "created_by_uid": "admin-uid",
        "created_by_email": "admin@test.com",
        "created_at": "2026-01-01T00:00:00",
        "updated_at": "2026-01-02T00:00:00",
    }

    fake_db = MagicMock()
    fake_db.collection.return_value.stream.return_value = [doc]

    monkeypatch.setattr(integration_controller, "db", fake_db)

    result = get_exported_sheets_history(
        current_admin={
            "uid": "admin-uid",
            "email": "admin@test.com",
        }
    )

    assert result["status"] == "success"
    assert result["total"] == 1
    assert result["data"][0]["id"] == "doc-1"