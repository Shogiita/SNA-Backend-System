from fastapi import APIRouter, Query

from app.controllers import network_analysis_controller, report_controller

router = APIRouter(
    prefix="/report",
    tags=["Report & Dashboard"],
)


@router.get("/dashboard/stats")
def get_dashboard_stats():
    return report_controller.get_stats_summary()


@router.get("/dashboard/top-content")
def get_dashboard_top_content(
    source: str = Query("app", description="Pilih sumber data: 'app' atau 'instagram'"),
    start_date: str | None = Query(None, description="Format: YYYY-MM-DD"),
    end_date: str | None = Query(None, description="Format: YYYY-MM-DD"),
):
    return report_controller.get_top_content_summary(
        source=source,
        start_date=start_date,
        end_date=end_date,
    )

@router.get("/dashboard/network-metrics")
def get_dashboard_network_metrics(
    source: str = Query("app", description="Pilih sumber data: 'app' atau 'instagram'"),
):
    return network_analysis_controller.get_network_metrics_full_summary(source)


@router.get("/dashboard/network-analysis-summary")
def get_dashboard_network_analysis_summary(
    source: str = Query("app", description="Pilih sumber data: 'app' atau 'instagram'"),
):
    return report_controller.get_network_analysis_summary(source)


@router.get("/dashboard/live-analytics")
def get_dashboard_live_analytics():
    return report_controller.get_live_analytics_summary()


@router.get("/dashboard/google-analytics")
def get_dashboard_google_analytics(
    start_date: str | None = Query(None, description="Format: YYYY-MM-DD"),
    end_date: str | None = Query(None, description="Format: YYYY-MM-DD"),
):
    return report_controller.get_google_analytics_summary(
        start_date=start_date,
        end_date=end_date,
    )


@router.get("/dashboard/monthly-report/history")
def get_dashboard_monthly_report_history(
    limit: int = Query(
        20,
        ge=1,
        le=100,
        description="Jumlah history report yang dikembalikan",
    ),
):
    return network_analysis_controller.list_monthly_report_history(limit=limit)