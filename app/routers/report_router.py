from fastapi import APIRouter, Query
from app.controllers import report_controller

router = APIRouter(
    prefix="/report",
    tags=["Report & Dashboard"]
)

#start

@router.get("/dashboard/stats")
def get_dashboard_stats(
    source: str = Query("app", description="Pilih sumber data: 'app' (Suara Surabaya) atau 'instagram'")
):
    """Mengambil metrik angka total (User, Post) secara instan."""
    return report_controller.get_stats_summary(source)

@router.get("/dashboard/top-content")
def get_dashboard_top_content(
    source: str = Query("app", description="Pilih sumber data: 'app' atau 'instagram'"),
    start_date: str = Query(None, description="Format: YYYY-MM-DD (Kosongkan untuk bulan ini)"),
    end_date: str = Query(None, description="Format: YYYY-MM-DD (Kosongkan untuk bulan ini)")
):
    """Mengambil Top 10 Posts dan Top 10 Hashtags secara instan."""
    return report_controller.get_top_content_summary(source)

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


#end

@router.get("/dashboard")
def get_main_dashboard(
    source: str = Query("app", description="Pilih sumber data: 'app' (Suara Surabaya) atau 'instagram'")
):
    """
    Data Statistik Internal (Users, Posts, Top Content, Geodesic, Centrality).
    Menggunakan parameter '?source=' untuk filter dari dropdown Frontend.
    """
    return report_controller.get_main_dashboard_summary(source)

@router.get("/analytics")
def get_google_analytics_data():
    """
    Data External dari Google Analytics.
    """
    return report_controller.get_analytics_summary()

@router.get("/export/csv/neo4j")
async def export_neo4j_endpoint():
    """
    Download file CSV berisi data statistik relasi user dari database Neo4j.
    Gunakan metode GET agar file langsung terunduh saat URL dibuka.
    """
    return await report_controller.export_neo4j_to_csv()

@router.get("/export/csv/instagram")
async def export_instagram_endpoint():
    """
    Download file CSV berisi data hasil crawling Instagram dari cache internal.
    Gunakan metode GET agar file langsung terunduh saat URL dibuka.
    Harap pastikan endpoint /sna/ingest sudah pernah dijalankan sebelumnya.
    """
    return await report_controller.export_instagram_to_csv()

@router.get("/top-hashtags")
def get_top_hashtags_endpoint():
    """
    Endpoint untuk mendapatkan daftar Top 10 Hashtag yang paling 
    sering digunakan pada postingan pengguna.
    """
    return report_controller.get_top_10_hashtags()