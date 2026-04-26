from fastapi import APIRouter, Query
from app.controllers import report_controller

router = APIRouter(
    prefix="/report",
    tags=["Report & Dashboard"]
)

#done

@router.get("/dashboard/stats")
def get_stats():
    return report_controller.get_stats_summary()

@router.get("/dashboard/top-content")
def get_dashboard_top_content(
    source: str = Query("app", description="Pilih sumber data: 'app' atau 'instagram'"),
    start_date: str = Query(None, description="Format: YYYY-MM-DD (Kosongkan untuk bulan ini)"),
    end_date: str = Query(None, description="Format: YYYY-MM-DD (Kosongkan untuk bulan ini)")
):
    """Mengambil Top 10 Posts dan Top 10 Hashtags secara instan."""
    return report_controller.get_top_content_summary(source=source, start_date=start_date, end_date=end_date)

@router.get("/dashboard/network-metrics")
def get_dashboard_network_metrics(
    source: str = Query("app", description="Pilih sumber data: 'app' atau 'instagram'")
):
    """Mengambil kalkulasi kompleks SNA (Centrality, Geodesic, Cliques)."""
    return report_controller.get_network_metrics_summary(source)

@router.get("/dashboard/live-analytics") 
def get_dashboard_live_analytics():
    """Mengambil data pengguna aktif real-time dari integrasi Google Analytics 4."""
    return report_controller.get_live_analytics_summary()