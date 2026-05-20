from unittest.mock import patch


def test_get_dashboard_network_analysis_summary_success(api_client):
    expected = {
        "status": "success",
        "source": "app",
        "summary": "Network analysis summary",
    }

    with patch(
        "app.routers.report_router.report_controller.get_network_analysis_summary",
        return_value=expected,
    ) as mock_controller:
        response = api_client.get(
            "/report/dashboard/network-analysis-summary",
            params={"source": "app"},
        )

    assert response.status_code == 200
    assert response.json() == expected
    mock_controller.assert_called_once_with("app")


def test_get_dashboard_google_analytics_success(api_client):
    expected = {
        "status": "success",
        "data": {
            "active_users": 10,
            "screen_page_views": 100,
        },
    }

    with patch(
        "app.routers.report_router.report_controller.get_google_analytics_summary",
        return_value=expected,
    ) as mock_controller:
        response = api_client.get(
            "/report/dashboard/google-analytics",
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


def test_get_dashboard_monthly_report_history_success(api_client):
    expected = {
        "status": "success",
        "data": [],
    }

    with patch(
        "app.routers.report_router.network_analysis_controller.list_monthly_report_history",
        return_value=expected,
    ) as mock_controller:
        response = api_client.get(
            "/report/dashboard/monthly-report/history",
            params={"limit": 20},
        )

    assert response.status_code == 200
    assert response.json() == expected
    mock_controller.assert_called_once_with(limit=20)


def test_get_network_nodes_success(api_client):
    expected = {
        "status": "success",
        "data": [],
    }

    with patch(
        "app.routers.report_router.network_analysis_controller.list_available_nodes",
        return_value=expected,
    ) as mock_controller:
        response = api_client.get(
            "/report/network/nodes",
            params={
                "source": "app",
                "keyword": "budi",
                "max_edges": 25000,
                "limit": 20,
            },
        )

    assert response.status_code == 200
    assert response.json() == expected
    mock_controller.assert_called_once_with(
        source="app",
        keyword="budi",
        max_edges=25000,
        limit=20,
    )


def test_get_network_neighbors_success(api_client):
    expected = {
        "status": "success",
        "data": [],
    }

    with patch(
        "app.routers.report_router.network_analysis_controller.get_node_neighbors",
        return_value=expected,
    ) as mock_controller:
        response = api_client.get(
            "/report/network/neighbors",
            params={
                "source": "app",
                "node": "user_1",
                "max_edges": 25000,
                "limit": 20,
            },
        )

    assert response.status_code == 200
    assert response.json() == expected
    mock_controller.assert_called_once_with(
        source="app",
        node="user_1",
        max_edges=25000,
        limit=20,
    )


def test_get_network_mentions_success(api_client):
    expected = {
        "status": "success",
        "data": [],
    }

    with patch(
        "app.routers.report_router.network_analysis_controller.get_mention_edges",
        return_value=expected,
    ) as mock_controller:
        response = api_client.get(
            "/report/network/mentions",
            params={
                "source": "instagram",
                "max_edges": 25000,
                "limit": 50,
            },
        )

    assert response.status_code == 200
    assert response.json() == expected
    mock_controller.assert_called_once_with(
        source="instagram",
        max_edges=25000,
        limit=50,
    )


def test_get_network_shortest_path_success(api_client):
    expected = {
        "status": "success",
        "path": ["user_1", "user_2"],
    }

    with patch(
        "app.routers.report_router.network_analysis_controller.get_shortest_path",
        return_value=expected,
    ) as mock_controller:
        response = api_client.get(
            "/report/network/shortest-path",
            params={
                "source": "app",
                "source_node": "user_1",
                "target_node": "user_2",
                "max_edges": 25000,
            },
        )

    assert response.status_code == 200
    assert response.json() == expected
    mock_controller.assert_called_once_with(
        source="app",
        source_node="user_1",
        target_node="user_2",
        max_edges=25000,
    )


def test_get_network_cliques_success(api_client):
    expected = {
        "status": "success",
        "data": [],
    }

    with patch(
        "app.routers.report_router.network_analysis_controller.get_cliques",
        return_value=expected,
    ) as mock_controller:
        response = api_client.get(
            "/report/network/cliques",
            params={
                "source": "app",
                "max_edges": 25000,
                "min_size": 3,
                "limit": 10,
            },
        )

    assert response.status_code == 200
    assert response.json() == expected
    mock_controller.assert_called_once_with(
        source="app",
        max_edges=25000,
        min_size=3,
        limit=10,
    )


def test_get_network_weight_schema_success(api_client):
    expected = {
        "status": "success",
        "schema": {},
    }

    with patch(
        "app.routers.report_router.network_analysis_controller.get_edge_weight_schema",
        return_value=expected,
    ) as mock_controller:
        response = api_client.get("/report/network/weight-schema")

    assert response.status_code == 200
    assert response.json() == expected
    mock_controller.assert_called_once()


def test_get_legacy_network_edge_weight_schema_success(api_client):
    expected = {
        "status": "success",
        "schema": {},
    }

    with patch(
        "app.routers.report_router.network_analysis_controller.get_edge_weight_schema",
        return_value=expected,
    ) as mock_controller:
        response = api_client.get("/report/network/edge-weight-schema")

    assert response.status_code == 200
    assert response.json() == expected
    mock_controller.assert_called_once()


def test_get_network_export_image_data_success(api_client):
    expected = {
        "status": "success",
        "data": {
            "nodes": [],
            "edges": [],
        },
    }

    with patch(
        "app.routers.report_router.network_analysis_controller.get_graph_png_data",
        return_value=expected,
    ) as mock_controller:
        response = api_client.get(
            "/report/network/export-image-data",
            params={
                "source": "app",
                "max_edges": 25000,
                "limit": 500,
            },
        )

    assert response.status_code == 200
    assert response.json() == expected
    mock_controller.assert_called_once_with(
        source="app",
        max_edges=25000,
        limit=500,
    )