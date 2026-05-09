from unittest.mock import AsyncMock, patch

from fastapi import HTTPException


def test_visualize_graph_from_neo4j_success(api_client):
    expected_html = "<html><body>Graph Visualization</body></html>"

    with patch(
        "app.routers.sna_router.neo4j_graph_controller.visualize_graph_from_neo4j",
        new_callable=AsyncMock,
        return_value=expected_html,
    ) as mock_controller:
        response = api_client.get("/sna/snagraph/visualize?limit=10&mode=1")

    assert response.status_code == 200

    # Endpoint mengembalikan string biasa, sehingga FastAPI serialize sebagai JSON string.
    assert response.json() == expected_html

    mock_controller.assert_awaited_once_with(limit=10, mode=1)


def test_create_app_visualization_graph_success(api_client):
    expected = {
        "message": "Graf visualisasi berhasil dibuat.",
        "graph_info": {
            "nodes_count": 2,
            "edges_count": 1,
            "communities_count": 1,
            "nodes": [],
            "edges": [],
        },
    }

    with patch(
        "app.routers.sna_router.neo4j_graph_controller.create_graph_visualization_from_neo4j",
        new_callable=AsyncMock,
        return_value=expected,
    ) as mock_controller:
        response = api_client.post(
            "/sna/neo4j/visualization/app",
            params={
                "limit": 100,
                "mode": 2,
                "max_edges": 500,
            },
        )

    assert response.status_code == 200
    assert response.json() == expected
    mock_controller.assert_awaited_once_with(
        limit=100,
        mode=2,
        max_edges=500,
    )


def test_create_app_visualization_graph_empty(api_client):
    with patch(
        "app.routers.sna_router.neo4j_graph_controller.create_graph_visualization_from_neo4j",
        new_callable=AsyncMock,
        side_effect=HTTPException(
            status_code=404,
            detail="Tidak ada relasi data yang ditemukan di Neo4j.",
        ),
    ):
        response = api_client.post("/sna/neo4j/visualization/app")

    assert response.status_code == 404
    assert "Tidak ada relasi data" in response.json()["detail"]