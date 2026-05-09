from fastapi import APIRouter, Query

from app.controllers import report_controller
from app.controllers import network_analysis_controller

router = APIRouter(
    prefix="/report",
    tags=["Report & Dashboard"],
)


@router.get("/dashboard/stats")
def get_stats():
    return report_controller.get_stats_summary()


@router.get("/dashboard/top-content")
def get_dashboard_top_content(
    source: str = Query("app", description="Pilih sumber data: 'app' atau 'instagram'"),
    start_date: str = Query(None, description="Format: YYYY-MM-DD"),
    end_date: str = Query(None, description="Format: YYYY-MM-DD"),
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


@router.get("/dashboard/live-analytics")
def get_dashboard_live_analytics():
    return report_controller.get_live_analytics_summary()


@router.get("/dashboard/monthly-report")
def get_dashboard_monthly_report(
    source: str = Query("app", description="Pilih sumber data: 'app' atau 'instagram'"),
    year: int = Query(None, description="Tahun laporan, contoh: 2026"),
    month: int = Query(None, description="Bulan laporan, 1-12"),
    save_history: bool = Query(False, description="Simpan hasil report ke Firestore"),
):
    return network_analysis_controller.get_monthly_report(
        source=source,
        year=year,
        month=month,
        save_history=save_history,
    )


@router.get("/dashboard/monthly-report/history")
def get_dashboard_monthly_report_history(
    limit: int = Query(20, description="Jumlah history report yang dikembalikan"),
):
    return network_analysis_controller.list_monthly_report_history(limit=limit)


@router.get("/network/nodes")
def get_network_nodes(
    source: str = Query("app"),
    keyword: str = Query(""),
    max_edges: int = Query(25000),
    limit: int = Query(20),
):
    return network_analysis_controller.list_available_nodes(
        source=source,
        keyword=keyword,
        max_edges=max_edges,
        limit=limit,
    )


@router.get("/network/neighbors")
def get_network_neighbors(
    source: str = Query("app"),
    node: str = Query(...),
    max_edges: int = Query(25000),
    limit: int = Query(20),
):
    return network_analysis_controller.get_node_neighbors(
        source=source,
        node=node,
        max_edges=max_edges,
        limit=limit,
    )


@router.get("/network/mentions")
def get_network_mentions(
    source: str = Query("instagram"),
    max_edges: int = Query(25000),
    limit: int = Query(50),
):
    return network_analysis_controller.get_mention_edges(
        source=source,
        max_edges=max_edges,
        limit=limit,
    )


@router.get("/network/shortest-path")
def get_network_shortest_path(
    source: str = Query("app"),
    source_node: str = Query(...),
    target_node: str = Query(...),
    max_edges: int = Query(25000),
):
    return network_analysis_controller.get_shortest_path(
        source=source,
        source_node=source_node,
        target_node=target_node,
        max_edges=max_edges,
    )


@router.get("/network/cliques")
def get_network_cliques(
    source: str = Query("app"),
    max_edges: int = Query(25000),
    min_size: int = Query(3),
    limit: int = Query(10),
):
    return network_analysis_controller.get_cliques(
        source=source,
        max_edges=max_edges,
        min_size=min_size,
        limit=limit,
    )


@router.get("/network/weight-schema")
def get_network_weight_schema():
    return network_analysis_controller.get_edge_weight_schema()


@router.get("/network/export-image-data")
def get_network_export_image_data(
    source: str = Query("app"),
    max_edges: int = Query(25000),
    limit: int = Query(500),
):
    return network_analysis_controller.get_graph_png_data(
        source=source,
        max_edges=max_edges,
        limit=limit,
    )