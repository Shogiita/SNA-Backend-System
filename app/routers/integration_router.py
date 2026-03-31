from fastapi import APIRouter, Query, UploadFile, File, Body
from pydantic import BaseModel
from typing import List, Optional
from app.controllers import integration_controller

router = APIRouter(
    prefix="/integration",
    tags=["Excel & Sheets Integration"]
)

class ExportPayload(BaseModel):
    source: str
    export_all: bool = True
    selected_columns: List[str] = []
    start_date: Optional[str] = None
    end_date: Optional[str] = None

class SheetsLinkPayload(ExportPayload):
    email: str

class SheetsSyncPayload(ExportPayload):
    sheet_id: str

@router.post("/export/excel")
async def export_excel_endpoint(payload: ExportPayload):
    """Export file Excel berdasarkan kustomisasi kolom user"""
    return await integration_controller.export_to_excel(
        payload.source, payload.start_date, payload.end_date, payload.selected_columns, payload.export_all
    )

@router.post("/sheets/link")
async def link_sheets_endpoint(payload: SheetsLinkPayload):
    """Membuat Spreadsheet baru dan membagikannya ke email Anda"""
    return await integration_controller.link_to_sheets(
        payload.email, payload.source, payload.start_date, payload.end_date, payload.selected_columns, payload.export_all
    )

@router.put("/sheets/sync")
async def sync_sheets_endpoint(payload: SheetsSyncPayload):
    """Memperbarui data di Spreadsheet yang sudah ada"""
    return await integration_controller.sync_to_sheets(
        payload.sheet_id, payload.source, payload.start_date, payload.end_date, payload.selected_columns, payload.export_all
    )

@router.delete("/sheets/unlink")
async def unlink_sheets_endpoint(sheet_id: str = Query(...)):
    """Menghapus/Unlink Spreadsheet"""
    return await integration_controller.unlink_sheets(sheet_id)

@router.post("/import/excel")
async def import_excel_endpoint(file: UploadFile = File(...)):
    """Upload file Excel hasil export untuk direkonstruksi menjadi Graf dan Centrality"""
    return await integration_controller.import_from_excel(file)

@router.get("/import/sheets")
async def import_sheets_endpoint(sheet_id: str):
    """Impor data dari Google Sheets untuk direkonstruksi menjadi Graf dan Centrality"""
    return await integration_controller.import_from_sheets(sheet_id)