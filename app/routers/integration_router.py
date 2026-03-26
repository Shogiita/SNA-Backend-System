from fastapi import APIRouter, Query, UploadFile, File
from app.controllers import integration_controller

router = APIRouter(
    prefix="/integration",
    tags=["Excel & Sheets Integration"]
)

@router.get("/export/excel")
async def export_excel_endpoint(source: str = Query(..., description="'app' atau 'instagram'")):
    return await integration_controller.export_to_excel(source)

@router.post("/sheets/link")
async def link_sheets_endpoint(email: str, source: str = Query(..., description="'app' atau 'instagram'")):
    """Membuat Spreadsheet baru dan membagikannya ke email Anda"""
    return await integration_controller.link_to_sheets(email, source)

@router.put("/sheets/sync")
async def sync_sheets_endpoint(sheet_id: str, source: str = Query(..., description="'app' atau 'instagram'")):
    """Memperbarui data di Spreadsheet yang sudah ada"""
    return await integration_controller.sync_to_sheets(sheet_id, source)

@router.delete("/sheets/unlink")
async def unlink_sheets_endpoint(sheet_id: str):
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