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