from typing import List, Optional, Literal
from pydantic import BaseModel


class ExportRequest(BaseModel):
    source: Literal["app", "instagram"]
    selected_columns: Optional[List[str]] = None
    export_all: bool = True
    spreadsheet_title: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    google_access_token: Optional[str] = None


class ImportSheetRequest(BaseModel):
    source: Literal["app", "instagram"]
    spreadsheet_id: Optional[str] = None
    spreadsheet_url: Optional[str] = None
    worksheet_name: Optional[str] = None
    google_access_token: Optional[str] = None

class ExportExistingSheetRequest(BaseModel):
    source: Literal["app", "instagram"]
    selected_columns: Optional[List[str]] = None
    export_all: bool = True
    spreadsheet_id: Optional[str] = None
    spreadsheet_url: Optional[str] = None
    worksheet_name: Optional[str] = "Export Data"
    start_date: Optional[str] = None
    end_date: Optional[str] = None