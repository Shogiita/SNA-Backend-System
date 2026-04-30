from typing import List, Optional, Literal
from pydantic import BaseModel, Field


class ExportRequest(BaseModel):
    source: Literal["app", "instagram"]
    export_all: bool = True
    selected_columns: List[str] = Field(default_factory=list)
    spreadsheet_title: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None

    google_access_token: Optional[str] = None