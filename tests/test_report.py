from unittest.mock import patch


def test_get_dashboard_stats_success(api_client):
    expected = {
        "status": "success",
        "data": {
            "users": {"total": 100},
            "posts": {"total": 50},
        },
    }

    with patch(
        "app.routers.report_router.report_controller.get_stats_summary",
        return_value=expected,
    ) as mock_controller:
        response = api_client.get("/report/dashboard/stats")

    assert response.status_code == 200
    assert response.json() == expected
    mock_controller.assert_called_once()


def test_get_dashboard_top_content_success(api_client):
    expected = {
        "status": "success",
        "source_active": "app",
        "data": {
            "top_content": [
                {"id": "post_1", "judul": "Berita 1", "jumlahView": 100}
            ],
            "top_10_hashtags": [
                {"hashtag": "#surabaya", "count": 2}
            ],
        },
    }

    with patch(
        "app.routers.report_router.report_controller.get_top_content_summary",
        return_value=expected,
    ) as mock_controller:
        response = api_client.get(
            "/report/dashboard/top-content",
            params={
                "source": "app",
                "start_date": "2026-01-01",
                "end_date": "2026-01-31",
            },
        )

    assert response.status_code == 200
    assert response.json() == expected
    mock_controller.assert_called_once_with(
        source="app",
        start_date="2026-01-01",
        end_date="2026-01-31",
    )


def test_get_dashboard_network_metrics_success(api_client):
    expected = {
        "status": "success",
        "source": "instagram",
        "data": {
            "top_10_centrality": {
                "degree": [],
                "betweenness": [],
                "closeness": [],
                "eigenvector": [],
            }
        },
    }

    with patch(
        "app.routers.report_router.report_controller.get_network_metrics_summary",
        return_value=expected,
    ) as mock_controller:
        response = api_client.get(
            "/report/dashboard/network-metrics",
            params={"source": "instagram"},
        )

    assert response.status_code == 200
    assert response.json() == expected
    mock_controller.assert_called_once_with("instagram")


def test_get_dashboard_live_analytics_success(api_client):
    expected = {
        "status": "success",
        "data": {
            "active_users_last_30_min": 10,
            "active_users_last_5_min": 3,
        },
    }

    with patch(
        "app.routers.report_router.report_controller.get_live_analytics_summary",
        return_value=expected,
    ) as mock_controller:
        response = api_client.get("/report/dashboard/live-analytics")

    assert response.status_code == 200
    assert response.json() == expected
    mock_controller.assert_called_once()