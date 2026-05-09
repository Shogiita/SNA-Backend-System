from unittest.mock import AsyncMock, patch


def test_get_sna_dashboard_metrics_success(api_client):
    expected = {
        "status": "success",
        "data": {
            "top_10_posts": [],
            "top_10_hashtags": [],
        },
    }

    with patch(
        "app.routers.sna_router.sna_controller.get_instagram_metrics",
        return_value=expected,
    ) as mock_controller:
        response = api_client.get(
            "/sna/metrics",
            params={
                "start_date": "2026-01-01",
                "end_date": "2026-01-31",
            },
        )

    assert response.status_code == 200
    assert response.json() == expected
    mock_controller.assert_called_once_with(
        start_date="2026-01-01",
        end_date="2026-01-31",
    )


def test_run_ingestion_endpoint_success(api_client):
    with patch(
        "app.routers.sna_router.sna_controller.background_ingestion_task",
        return_value=None,
    ) as mock_controller:
        response = api_client.get("/sna/ingest")

    assert response.status_code == 200
    assert response.json()["status"] == "success"
    assert "sinkronisasi data Instagram" in response.json()["message"]
    assert mock_controller.called


def test_get_sna_dataset_success(api_client):
    expected = {
        "status": "success",
        "data": [
            {
                "id": "post_1",
                "caption": "Test post",
            }
        ],
    }

    with patch(
        "app.routers.sna_router.sna_controller.get_dataset_flat",
        return_value=expected,
    ) as mock_controller:
        response = api_client.get("/sna/dataset")

    assert response.status_code == 200
    assert response.json() == expected
    mock_controller.assert_called_once()


def test_create_instagram_visualization_graph_success(api_client):
    expected = {
        "message": "Graf visualisasi Instagram berhasil dibuat.",
        "graph_info": {
            "nodes_count": 3,
            "edges_count": 2,
            "communities_count": 1,
            "nodes": [],
            "edges": [],
        },
    }

    with patch(
        "app.routers.sna_router.sna_controller.create_instagram_graph_visualization_from_neo4j",
        new_callable=AsyncMock,
        return_value=expected,
    ) as mock_controller:
        response = api_client.post(
            "/sna/neo4j/visualization/instagram",
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


def test_visualize_neo4j_network_success(api_client):
    expected_html = "<html><body>Instagram Neo4j Network</body></html>"

    with patch(
        "app.routers.sna_router.sna_controller.visualize_neo4j_network",
        new_callable=AsyncMock,
        return_value=expected_html,
        create=True,
    ) as mock_controller:
        response = api_client.get(
            "/sna/neo4j/visualize",
            params={
                "mode": 1,
                "limit": 50,
            },
        )

    assert response.status_code == 200

    assert response.json() == expected_html

    mock_controller.assert_awaited_once_with(mode=1, limit=50)

def test_manual_sync_instagram_to_neo4j_success(api_client):
    with patch(
        "app.routers.sna_router.sna_controller.sync_instagram_to_neo4j",
        return_value=None,
    ) as mock_controller:
        response = api_client.post(
            "/sna/instagram/sync-neo4j",
            params={"initial_sync": True},
        )

    assert response.status_code == 200
    assert response.json()["status"] == "success"
    assert "initial sync" in response.json()["message"]
    assert mock_controller.called