from fastapi import APIRouter, Depends, Body

from app.controllers import integration_controller
from app.middleware.firebase_auth import get_current_admin
from app.schema.integration_schema import ExportRequest, ImportSheetRequest, ExportExistingSheetRequest

router = APIRouter(tags=["Integration"], prefix="/integration")


@router.post("/export/csv")
def export_csv(payload: ExportRequest):
    return integration_controller.export_csv(payload)

@router.post("/export/sheets")
def export_sheets(payload: ExportRequest):
    return integration_controller.export_sheets(payload)
    
@router.post("/import/sheets")
def import_sheets(
    payload: ImportSheetRequest,
    current_admin: dict = Depends(get_current_admin),
):
    return integration_controller.import_sheets(payload, current_admin)

@router.post("/export/sheets/existing")
def export_existing_sheets(
    payload: ExportExistingSheetRequest,
    current_admin: dict = Depends(get_current_admin),
):
    return integration_controller.export_existing_sheets(payload, current_admin)

@router.get("/sheets/linked")
def get_linked_sheets(
    current_admin: dict = Depends(get_current_admin),
):
    return integration_controller.get_linked_sheets(current_admin)

@router.get("/export/sheets/history")
def get_exported_sheets_history(
    current_admin: dict = Depends(get_current_admin),
):
    return integration_controller.get_exported_sheets_history(current_admin)


@router.delete("/sheets/unlink/{doc_id}")
def unlink_sheet(
    doc_id: str,
    current_admin: dict = Depends(get_current_admin),
):
    return integration_controller.unlink_sheet(doc_id, current_admin)