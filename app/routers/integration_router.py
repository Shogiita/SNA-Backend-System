from fastapi import APIRouter, Depends

from app.controllers import integration_controller
from app.schema.integration_schema import ExportRequest
from app.middleware.firebase_auth import get_current_admin

router = APIRouter(tags=["Integration"], prefix="/integration")


@router.post("/export/csv")
def export_csv(
    payload: ExportRequest,
    current_admin: dict = Depends(get_current_admin),
):
    return integration_controller.export_csv(payload, current_admin)


@router.post("/export/sheets")
def export_sheets(
    payload: ExportRequest,
    current_admin: dict = Depends(get_current_admin),
):
    return integration_controller.export_sheets(payload, current_admin)


@router.get("/sheets/linked")
def get_linked_sheets(
    current_admin: dict = Depends(get_current_admin),
):
    return integration_controller.get_linked_sheets(current_admin)


@router.delete("/sheets/unlink/{doc_id}")
def unlink_sheet(
    doc_id: str,
    current_admin: dict = Depends(get_current_admin),
):
    return integration_controller.unlink_sheet(doc_id, current_admin)