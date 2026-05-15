from fastapi import APIRouter

from app.controllers import integration_controller
from app.schema.integration_schema import (
    ExportRequest,
    ImportSheetRequest,
    ExportExistingSheetRequest,
)

router = APIRouter(tags=["Integration"], prefix="/integration")


@router.post("/export/csv")
def export_csv(payload: ExportRequest):
    return integration_controller.export_csv(payload)

@router.post("/import/sheets")
def import_sheets(payload: ImportSheetRequest):
    return integration_controller.import_sheets(payload, current_admin=None)

@router.post("/export/sheets/existing")
def export_existing_sheets(payload: ExportExistingSheetRequest):
    return integration_controller.export_existing_sheets(
        payload,
        current_admin=None,
    )

@router.get("/sheets/linked")
def get_linked_sheets():
    return integration_controller.get_linked_sheets(current_admin=None)

@router.delete("/sheets/unlink/{doc_id}")
def unlink_sheet(doc_id: str):
    return integration_controller.unlink_sheet(
        doc_id,
        current_admin=None,
    )