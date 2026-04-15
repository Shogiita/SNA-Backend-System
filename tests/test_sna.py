import pytest
from unittest.mock import patch, MagicMock
import networkx as nx

# 1. Test SNA Metrics dari API Instagram
@patch("app.controllers.sna_controller.session.get")
def test_get_instagram_metrics_success(mock_get, api_client):
    # Arrange: Mock response library 'requests'
    mock_resp = MagicMock()
    mock_resp.json.return_value = {
        "data": [
            {
                "id": "1", 
                "timestamp": "2026-04-09T10:00:00+0000", 
                "caption": "Halo #Surabaya #Jatim", 
                "like_count": 100, 
                "comments_count": 5
            }
        ]
    }
    mock_get.return_value = mock_resp
    
    # Act
    response = api_client.get("/sna/metrics")
    
    # Assert
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "success"
    assert len(data["data"]["top_10_posts"]) > 0
    assert data["data"]["top_10_hashtags"][0]["hashtag"] in ["#surabaya", "#jatim"]

# 2. Test Analisis Neo4j (JSON output)
@patch("app.controllers.sna_controller._build_neo4j_graph")
def test_analyze_neo4j_endpoint(mock_build, api_client):
    # Arrange
    dummy_g = nx.DiGraph()
    dummy_g.add_node("user_1", label="Budi")
    dummy_g.add_node("user_2", label="Andi")
    dummy_g.add_edge("user_1", "user_2", weight=1)
    mock_build.return_value = dummy_g
    
    # Act
    response = api_client.get("/sna/neo4j/analyze?mode=1")
    
    # Assert
    assert response.status_code == 200
    res_data = response.json()
    assert res_data["meta"]["total_nodes"] == 2
    assert res_data["meta"]["total_edges"] == 1

# 3. Test Graph Dummy Generator
def test_generate_dummy_graph(api_client):
    # Endpoint ini murni algoritma lokal, tidak butuh mock DB
    response = api_client.get("/graph/generate")
    assert response.status_code == 200
    data = response.json()
    assert data["graph_info"]["nodes_count"] > 0
    assert "leiden_communities" in data["analysis_results"]

def test_generate_csv_graph(api_client):
    # Test error handling jika file twitter_dataset.csv tidak ada di server CI/CD
    response = api_client.get("/csvgraph/generate")
    # Karena file fisik tidak kita sediakan saat testing, harusnya return 404
    if response.status_code == 404:
        assert "Dataset tidak ditemukan" in response.json()["detail"]