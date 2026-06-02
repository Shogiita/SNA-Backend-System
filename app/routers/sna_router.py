# app\routers\sna_router.py

from fastapi import APIRouter, BackgroundTasks, Query

from app.controllers import (
    neo4j_graph_controller,
    network_analysis_controller,
    sna_controller,
)

router = APIRouter(
    prefix="/sna",
    tags=["SNA (Advanced - Neo4j)"],
)


@router.get("/metrics")
def get_sna_dashboard_metrics(
    start_date: str = Query(None, description="Format tanggal start, contoh: 2024-01-01"),
    end_date: str = Query(None, description="Format tanggal end, contoh: 2024-12-31"),
):
    return sna_controller.get_instagram_metrics(
        start_date=start_date,
        end_date=end_date,
    )


@router.get("/ingest")
async def run_ingestion_endpoint(background_tasks: BackgroundTasks):
    background_tasks.add_task(sna_controller.background_ingestion_task)

    return {
        "status": "success",
        "message": (
            "Proses sinkronisasi data Instagram Suara Surabaya sedang berjalan "
            "di latar belakang. Silakan cek /sna/dataset beberapa menit lagi."
        ),
    }


@router.get("/dataset")
def get_sna_dataset_endpoint():
    return sna_controller.get_dataset_flat()


@router.post("/neo4j/visualization/app")
async def create_app_visualization_graph_endpoint(
    limit: int = Query(5000, ge=1, le=25000),
    mode: int = Query(2, description="1: User-User, 2: User-Post"),
    max_edges: int = Query(25000, ge=1, le=25000),
):
    return await neo4j_graph_controller.create_graph_visualization_from_neo4j(
        limit=limit,
        mode=mode,
        max_edges=max_edges,
    )


@router.post("/neo4j/visualization/instagram")
async def create_instagram_visualization_graph_endpoint(
    limit: int = Query(5000, ge=1, le=25000),
    mode: int = Query(2, description="1: User-User, 2: User-Post-Comment-Hashtag"),
    max_edges: int = Query(25000, ge=1, le=25000),
):
    return await sna_controller.create_instagram_graph_visualization_from_neo4j(
        limit=limit,
        mode=mode,
        max_edges=max_edges,
    )

@router.get("/neo4j/nodes")
def get_sna_neo4j_nodes(
    source: str = Query("app", description="Pilih sumber data: 'app' atau 'instagram'"),
    keyword: str = Query("", description="Keyword pencarian node"),
    max_edges: int = Query(25000, ge=100, le=50000),
    limit: int = Query(100, ge=1, le=1000),
):
    return network_analysis_controller.list_available_nodes(
        source=source,
        keyword=keyword,
        max_edges=max_edges,
        limit=limit,
    )


@router.get("/neo4j/neighbors")
def get_sna_neo4j_neighbors(
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


@router.get("/neo4j/shortest-path")
def get_sna_neo4j_shortest_path(
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


@router.get("/neo4j/mentions/summary")
def get_sna_neo4j_mentions_summary(
    source: str = Query("instagram", description="Pilih sumber data: 'app' atau 'instagram'"),
    max_edges: int = Query(25000, ge=100, le=50000),
    limit: int = Query(50, ge=1, le=500),
):
    return network_analysis_controller.get_mention_edges(
        source=source,
        max_edges=max_edges,
        limit=limit,
    )


@router.get("/neo4j/cliques")
def get_sna_neo4j_cliques(
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


@router.get("/neo4j/weight-schema")
def get_sna_neo4j_weight_schema():
    return network_analysis_controller.get_edge_weight_schema()


@router.get("/neo4j/edge-weight-schema")
def get_sna_neo4j_edge_weight_schema():
    return network_analysis_controller.get_edge_weight_schema()


@router.get("/neo4j/export-image-data")
def get_sna_neo4j_export_image_data(
    source: str = Query("app", description="Pilih sumber data: 'app' atau 'instagram'"),
    max_edges: int = Query(25000, ge=100, le=50000),
    limit: int = Query(500, ge=10, le=5000),
):
    return network_analysis_controller.get_graph_png_data(
        source=source,
        max_edges=max_edges,
        limit=limit,
    )


@router.get("/neo4j/visualize")
async def visualize_neo4j_endpoint(
    mode: int = Query(1, description="1: User-User, 2: User-Post"),
    limit: int = Query(1000, ge=1, le=25000),
):
    return await sna_controller.visualize_neo4j_network(
        mode=mode,
        limit=limit,
    )


@router.post("/instagram/sync-neo4j")
async def manual_sync_ig_neo4j_endpoint(
    background_tasks: BackgroundTasks,
    initial_sync: bool = Query(
        False,
        description="True untuk initial sync, False untuk update data terbaru.",
    ),
):
    background_tasks.add_task(
        sna_controller.sync_instagram_to_neo4j,
        initial_sync,
    )

    msg = "Proses initial sync data" if initial_sync else "Proses update data terbaru"

    return {
        "status": "success",
        "message": f"{msg} Instagram ke Neo4j sedang berjalan di background.",
    }