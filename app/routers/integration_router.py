from fastapi import APIRouter, Query, UploadFile, File
from pydantic import BaseModel
from typing import List, Optional
from app.controllers import integration_controller

router = APIRouter(
    prefix="/integration",
    tags=["CSV & Sheets Integration"]
)

class ExportPayload(BaseModel):
    source: str 
    export_all: bool = True
    selected_columns: List[str] = []
    start_date: Optional[str] = None
    end_date: Optional[str] = None

# Menerima URL Spreadsheet langsung dari user
class SheetsLinkPayload(ExportPayload):
    existing_sheet_url: str

@router.post("/export/csv")
async def export_csv_endpoint(payload: ExportPayload):
    return await integration_controller.export_to_csv(
        payload.source, payload.start_date, payload.end_date, payload.selected_columns, payload.export_all
    )

@router.post("/sheets/link")
async def link_sheets_endpoint(payload: SheetsLinkPayload):
    return await integration_controller.link_to_sheets(
        payload.existing_sheet_url, payload.source, payload.start_date, payload.end_date, payload.selected_columns, payload.export_all
    )

@router.get("/sheets/linked")
async def get_linked_sheets_endpoint():
    return await integration_controller.get_all_linked_sheets()

@router.delete("/sheets/unlink/{doc_id}")
async def unlink_sheets_doc_endpoint(doc_id: str):
    return await integration_controller.unlink_sheets(doc_id)