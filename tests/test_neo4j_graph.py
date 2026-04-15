import pytest
import networkx as nx
from unittest.mock import patch

@patch("app.controllers.neo4j_graph_controller._build_neo4j_graph_internal")
def test_visualize_graph_endpoint_success(mock_build_graph, api_client):
    dummy_G = nx.DiGraph()
    dummy_G.add_node("user_1", name="Budi", type="user", community=0)
    dummy_G.add_node("user_2", name="Andi", type="user", community=1)
    dummy_G.add_edge("user_1", "user_2", weight=5, relation="COMMENT")
    
    mock_build_graph.return_value = dummy_G

    # Act: Hit endpoint visualisasi
    # response = api_client.get("/ssgraph/snagraph/visualize?limit=10&mode=1")
    response = api_client.get("/snagraph/visualize?limit=10&mode=1")

    # Assert
    assert response.status_code == 200
    assert response.headers["content-type"] == "text/html; charset=utf-8"
    
    html_content = response.text
    assert "Budi" in html_content
    assert "Andi" in html_content
    assert "vis.js" in html_content

@patch("app.controllers.neo4j_graph_controller._build_neo4j_graph_internal")
def test_visualize_graph_empty(mock_build_graph, api_client):
    mock_build_graph.return_value = nx.DiGraph()

    # Act
    # response = api_client.get("/ssgraph/snagraph/visualize?limit=10&mode=1")
    response = api_client.get("/snagraph/visualize?limit=10&mode=1")

    # Assert
    assert response.status_code == 200
    assert "Graf Kosong" in response.text