import pytest
from unittest.mock import patch, MagicMock

# 1. Test Clear Data Neo4j
@pytest.mark.asyncio
@patch("app.controllers.neo4j_migration_controller.check_neo4j_connection")
@patch("app.controllers.neo4j_migration_controller.neo4j_driver")
async def test_clear_all_neo4j_data_success(mock_driver, mock_check, api_client):
    # Arrange
    mock_check.return_value = True
    
    mock_session = MagicMock()
    mock_driver.session.return_value.__enter__.return_value = mock_session

    # Act
    response = api_client.delete("/neo4j/clear-all")

    # Assert
    assert response.status_code == 200
    assert "berhasil dihapus" in response.json()["message"]
    # Memastikan query DETACH DELETE dipanggil
    mock_session.run.assert_called_with("MATCH (n) DETACH DELETE n")

@pytest.mark.asyncio
@patch("app.controllers.neo4j_migration_controller.check_neo4j_connection")
async def test_clear_all_neo4j_data_failed(mock_check, api_client):
    # Arrange: Simulasi koneksi Neo4j putus
    mock_check.return_value = False

    # Act
    response = api_client.delete("/neo4j/clear-all")

    # Assert
    assert response.status_code == 200
    assert response.json()["status"] == "error"
    assert "Gagal terhubung" in response.json()["message"]