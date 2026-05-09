from unittest.mock import AsyncMock, patch


def test_start_migration_success(api_client):
    with patch(
        "app.routers.neo4j_router.neo4j_migration_controller.start_migration",
        return_value=True,
    ) as mock_controller:
        response = api_client.post("/neo4j/migrate")

    assert response.status_code == 200
    assert response.json()["status"] == "success"
    assert "Migrasi Firebase ke Neo4j dimulai" in response.json()["message"]
    mock_controller.assert_called_once()


def test_start_migration_already_running(api_client):
    with patch(
        "app.routers.neo4j_router.neo4j_migration_controller.start_migration",
        return_value=False,
    ) as mock_controller:
        response = api_client.post("/neo4j/migrate")

    assert response.status_code == 200
    assert response.json()["status"] == "running"
    assert "Migrasi masih dianggap berjalan" in response.json()["message"]
    mock_controller.assert_called_once()


def test_get_migration_status(api_client):
    expected = {
        "status": "idle",
        "is_running": False,
        "progress": 0,
    }

    with patch(
        "app.routers.neo4j_router.neo4j_migration_controller.get_migration_status",
        return_value=expected,
    ) as mock_controller:
        response = api_client.get("/neo4j/migrate/status")

    assert response.status_code == 200
    assert response.json() == expected
    mock_controller.assert_called_once()


def test_unlock_migration(api_client):
    expected = {
        "status": "success",
        "message": "Migration lock berhasil dibuka.",
    }

    with patch(
        "app.routers.neo4j_router.neo4j_migration_controller.unlock_migration",
        return_value=expected,
    ) as mock_controller:
        response = api_client.post("/neo4j/migrate/unlock")

    assert response.status_code == 200
    assert response.json() == expected
    mock_controller.assert_called_once()


def test_clear_all_neo4j_data_success(api_client):
    expected = {
        "status": "success",
        "message": "Semua data Neo4j berhasil dihapus.",
    }

    with patch(
        "app.routers.neo4j_router.neo4j_migration_controller.delete_all_neo4j_data",
        new_callable=AsyncMock,
        return_value=expected,
    ) as mock_controller:
        response = api_client.delete("/neo4j/clear-all")

    assert response.status_code == 200
    assert response.json() == expected
    mock_controller.assert_awaited_once()