from fastapi import APIRouter
from app.controllers import report_controller

router = APIRouter(
    prefix="/report",
    tags=["Report & Dashboard"]
)

@router.get("/dashboard")
def get_main_dashboard():
    """
    Data Statistik Internal (Users, Posts, Top Content).
    Ringan dan cepat.
    """
    return report_controller.get_main_dashboard_summary()

@router.get("/analytics")
def get_google_analytics_data():
    """
    Data External dari Google Analytics.
    Pisahkan request ini di Frontend agar tidak memblokir loading dashboard utama.
    """
    return report_controller.get_analytics_summary()

@router.post("/export/sheets")
def export_data_to_sheets():
    """
    Trigger untuk export data ke Google Sheets.
    """
    return report_controller.export_to_google_sheets()