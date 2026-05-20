import pandas as pd
import pytest
from fastapi import HTTPException

from app.controllers.integration_controller import (
    _extract_spreadsheet_id,
    _normalize_frontend_columns,
    _convert_legacy_df_to_normalized,
    _select_export_columns,
)


def test_extract_spreadsheet_id_from_id():
    result = _extract_spreadsheet_id(
        spreadsheet_id=" sheet_123 ",
        spreadsheet_url=None,
    )

    assert result == "sheet_123"


def test_extract_spreadsheet_id_from_url():
    result = _extract_spreadsheet_id(
        spreadsheet_id=None,
        spreadsheet_url="https://docs.google.com/spreadsheets/d/sheet_123/edit#gid=0",
    )

    assert result == "sheet_123"


def test_extract_spreadsheet_id_missing_value():
    with pytest.raises(HTTPException) as excinfo:
        _extract_spreadsheet_id(
            spreadsheet_id=None,
            spreadsheet_url=None,
        )

    assert excinfo.value.status_code == 400
    assert "wajib diisi" in excinfo.value.detail


def test_extract_spreadsheet_id_invalid_url():
    with pytest.raises(HTTPException) as excinfo:
        _extract_spreadsheet_id(
            spreadsheet_id=None,
            spreadsheet_url="https://example.com/not-sheet",
        )

    assert excinfo.value.status_code == 400
    assert "tidak valid" in excinfo.value.detail


def test_normalize_frontend_columns():
    result = _normalize_frontend_columns(
        [
            "Jumlah_Like_Post",
            "Jumlah_Comment_Post",
            "likes",
            "unknown",
            "likes",
        ]
    )

    assert result == [
        "jumlah_like_post",
        "jumlah_comment_post",
        "likes",
    ]


def test_convert_legacy_df_to_normalized_instagram():
    df = pd.DataFrame(
        [
            {
                "Interaction_Type": "comment",
                "Source_User": "user_1",
                "Target": "post_1",
                "Post_Link": "https://instagram.com/p/abc",
                "User_Pembuat_Post": "creator",
                "Post": "caption",
                "Tanggal_Upload": "2026-01-01",
                "Jumlah_Like_Post": 10,
                "Jumlah_Comment_Post": 5,
            }
        ]
    )

    result = _convert_legacy_df_to_normalized(df, "instagram")

    assert result.loc[0, "interaction_type"] == "comment"
    assert result.loc[0, "source_user"] == "user_1"
    assert result.loc[0, "target"] == "post_1"
    assert result.loc[0, "post_link"] == "https://instagram.com/p/abc"
    assert result.loc[0, "likes"] == 10
    assert result.loc[0, "like_count"] == 10
    assert result.loc[0, "comment_count"] == 5
    assert "hashtag" in result.columns
    assert "media_type" in result.columns
    assert "post_id" in result.columns
    assert "user_id" in result.columns


def test_convert_legacy_df_to_normalized_app_removes_post_link():
    df = pd.DataFrame(
        [
            {
                "Interaction_Type": "comment",
                "Source_User": "user_1",
                "Target": "post_1",
                "Post_Link": "https://example.com/post",
                "User_Pembuat_Post": "creator",
                "Post": "content",
                "Tanggal_Upload": "2026-01-01",
            }
        ]
    )

    result = _convert_legacy_df_to_normalized(df, "app")

    assert "post_link" not in result.columns
    assert result.loc[0, "interaction_type"] == "comment"


def test_convert_legacy_df_to_normalized_empty_df():
    df = pd.DataFrame()

    result = _convert_legacy_df_to_normalized(df, "app")

    assert result.empty


def test_select_export_columns_app_without_extra():
    df = pd.DataFrame(
        [
            {
                "interaction_type": "comment",
                "source_user": "user_1",
                "target": "post_1",
                "user_pembuat_post": "creator",
                "post": "content",
                "tanggal_upload": "2026-01-01",
                "post_link": "should_not_exist",
            }
        ]
    )

    result = _select_export_columns(
        df=df,
        source_type="app",
        selected_columns=None,
        export_all=False,
    )

    assert list(result.columns) == [
        "interaction_type",
        "source_user",
        "target",
        "user_pembuat_post",
        "post",
        "tanggal_upload",
    ]


def test_select_export_columns_instagram_without_extra():
    df = pd.DataFrame(
        [
            {
                "interaction_type": "comment",
                "source_user": "user_1",
                "target": "post_1",
                "post_link": "https://instagram.com/p/abc",
                "user_pembuat_post": "creator",
                "post": "caption",
                "tanggal_upload": "2026-01-01",
            }
        ]
    )

    result = _select_export_columns(
        df=df,
        source_type="instagram",
        selected_columns=None,
        export_all=False,
    )

    assert list(result.columns) == [
        "interaction_type",
        "source_user",
        "target",
        "post_link",
        "user_pembuat_post",
        "post",
        "tanggal_upload",
    ]


def test_select_export_columns_adds_missing_columns():
    df = pd.DataFrame([{}])

    result = _select_export_columns(
        df=df,
        source_type="app",
        selected_columns=["likes"],
        export_all=False,
    )

    assert "interaction_type" in result.columns
    assert "likes" in result.columns