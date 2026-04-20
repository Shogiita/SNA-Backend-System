import pytest
from unittest.mock import patch, MagicMock
from app.controllers.neo4j_migration_controller import make_serializable, prepare_for_neo4j

# =====================================================================
# 1. HELPER FUNCTIONS
# =====================================================================
def test_make_serializable():
    class DummyDate:
        def isoformat(self): return "2024-01-01"
    
    res = make_serializable({"date": DummyDate(), "list": [DummyDate()]})
    assert res == {"date": "2024-01-01", "list": ["2024-01-01"]}

def test_prepare_for_neo4j():
    res = prepare_for_neo4j({"a": None, "b": {"x": 1}, "c": True, "d": "str"})
    assert "a" not in res
    assert res["c"] is True
    assert res["d"] == "str"
    assert "x" in res["b"] # Memastikan json.dumps bekerja

# =====================================================================
# 2. DELETE ALL DATA
# =====================================================================
@pytest.mark.asyncio
@patch("app.controllers.neo4j_migration_controller.check_neo4j_connection")
@patch("app.controllers.neo4j_migration_controller.neo4j_driver")
async def test_clear_all_neo4j_data_success(mock_driver, mock_check, api_client):
    mock_check.return_value = True
    mock_session = MagicMock()
    mock_driver.session.return_value.__enter__.return_value = mock_session

    response = api_client.delete("/neo4j/clear-all")

    assert response.status_code == 200
    assert "berhasil dihapus" in response.json()["message"]
    mock_session.run.assert_called_with("MATCH (n) DETACH DELETE n")

@pytest.mark.asyncio
@patch("app.controllers.neo4j_migration_controller.check_neo4j_connection")
async def test_clear_all_neo4j_data_failed(mock_check, api_client):
    mock_check.return_value = False
    response = api_client.delete("/neo4j/clear-all")
    assert response.status_code == 200
    assert response.json()["status"] == "error"

# =====================================================================
# 3. BACKGROUND MIGRATION PROCESS
# =====================================================================
@pytest.mark.asyncio
@patch("app.controllers.neo4j_migration_controller.check_neo4j_connection", return_value=True)
@patch("app.controllers.neo4j_migration_controller.db")
@patch("app.controllers.neo4j_migration_controller.neo4j_driver")
async def test_run_migration_background(mock_driver, mock_db, mock_check):
    from app.controllers.neo4j_migration_controller import run_migration_background
    
    # Mock firestore count (Mencegah total == 0)
    mock_count = MagicMock()
    mock_count.value = 1
    mock_query = MagicMock()
    mock_query.get.return_value = [[mock_count]]
    mock_db.collection.return_value.count.return_value = mock_query

    # Mock stream Firebase Collection
    mock_doc = MagicMock()
    mock_doc.id = "1"
    mock_doc.to_dict.return_value = {"field": "value"}
    
    mock_stream = MagicMock()
    # Side effect: Pertama kali return list 1 dokumen, kedua kali return list kosong (berhenti)
    mock_stream.stream.side_effect = [[mock_doc], []] 
    mock_db.collection.return_value.order_by.return_value.limit.return_value = mock_stream
    mock_db.collection.return_value.order_by.return_value.start_after.return_value.limit.return_value.stream.return_value = []

    mock_session = MagicMock()
    mock_driver.session.return_value.__enter__.return_value = mock_session

    # Jalankan background task
    await run_migration_background()
    
    # Pastikan data di-insert (session.run terpanggil)
    assert mock_session.run.called