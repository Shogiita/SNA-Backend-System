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


@router.get("/network/nodes")
def get_network_nodes(
    source: str = Query("app", description="Pilih sumber data: 'app' atau 'instagram'"),
    keyword: str = Query("", description="Keyword pencarian node"),
    max_edges: int = Query(25000, ge=100, le=50000),
    limit: int = Query(20, ge=1, le=100),
):
    return network_analysis_controller.list_available_nodes(
        source=source,
        keyword=keyword,
        max_edges=max_edges,
        limit=limit,
    )


@router.get("/network/neighbors")
def get_network_neighbors(
    source: str = Query("app", description="Pilih sumber data: 'app' atau 'instagram'"),
    node: str = Query(..., description="ID, username, atau label node"),
    max_edges: int = Query(25000, ge=100, le=50000),
    limit: int = Query(20, ge=1, le=100),
):
    return network_analysis_controller.get_node_neighbors(
        source=source,
        node=node,
        max_edges=max_edges,
        limit=limit,
    )


@router.get("/network/mentions")
def get_network_mentions(
    source: str = Query("instagram", description="Pilih sumber data: 'app' atau 'instagram'"),
    max_edges: int = Query(25000, ge=100, le=50000),
    limit: int = Query(50, ge=1, le=500),
):
    return network_analysis_controller.get_mention_edges(
        source=source,
        max_edges=max_edges,
        limit=limit,
    )


@router.get("/network/shortest-path")
def get_network_shortest_path(
    source: str = Query("app", description="Pilih sumber data: 'app' atau 'instagram'"),
    source_node: str = Query(..., description="Node asal"),
    target_node: str = Query(..., description="Node tujuan"),
    max_edges: int = Query(25000, ge=100, le=50000),
):
    return network_analysis_controller.get_shortest_path(
        source=source,
        source_node=source_node,
        target_node=target_node,
        max_edges=max_edges,
    )


@router.get("/network/cliques")
def get_network_cliques(
    source: str = Query("app", description="Pilih sumber data: 'app' atau 'instagram'"),
    max_edges: int = Query(25000, ge=100, le=50000),
    min_size: int = Query(3, ge=2, le=20),
    limit: int = Query(10, ge=1, le=100),
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


@router.get("/network/edge-weight-schema")
def get_legacy_network_edge_weight_schema():
    return network_analysis_controller.get_edge_weight_schema()


@router.get("/network/export-image-data")
def get_network_export_image_data(
    source: str = Query("app", description="Pilih sumber data: 'app' atau 'instagram'"),
    max_edges: int = Query(25000, ge=100, le=50000),
    limit: int = Query(500, ge=10, le=5000),
):
    return network_analysis_controller.get_graph_png_data(
        source=source,
        max_edges=max_edges,
        limit=limit,
    )