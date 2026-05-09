import networkx as nx

from app.utils.leiden_utils import (
    detect_leiden_communities,
    apply_leiden_communities,
    get_leiden_communities,
    _fallback_greedy_modularity,
)


def test_detect_leiden_communities_empty_graph():
    graph = nx.Graph()

    result = detect_leiden_communities(graph)

    assert result == {}


def test_detect_leiden_communities_nodes_without_edges():
    graph = nx.Graph()
    graph.add_node("a")
    graph.add_node("b")

    result = detect_leiden_communities(graph)

    assert result == {
        "a": 0,
        "b": 1,
    }


def test_detect_leiden_communities_weight_parse_error():
    graph = nx.Graph()
    graph.add_edge("a", "b", weight="invalid")

    result = detect_leiden_communities(graph)

    assert set(result.keys()) == {"a", "b"}


def test_detect_leiden_communities_directed_graph():
    graph = nx.DiGraph()
    graph.add_edge("a", "b", weight=1)
    graph.add_edge("b", "c", weight=2)

    result = detect_leiden_communities(graph)

    assert set(result.keys()) == {"a", "b", "c"}


def test_apply_leiden_communities_sets_node_attribute():
    graph = nx.Graph()
    graph.add_edge("user_1", "user_2", weight=1)

    result = apply_leiden_communities(
        graph,
        weight_attr="weight",
        community_attr="community",
    )

    assert isinstance(result, dict)
    assert "community" in graph.nodes["user_1"]
    assert "community" in graph.nodes["user_2"]


def test_get_leiden_communities_groups_nodes():
    graph = nx.Graph()
    graph.add_edge("user_1", "user_2", weight=1)
    graph.add_edge("user_3", "user_4", weight=1)

    result = get_leiden_communities(graph)

    assert isinstance(result, dict)
    assert sum(len(nodes) for nodes in result.values()) == 4


def test_fallback_greedy_modularity_empty_graph():
    graph = nx.Graph()

    result = _fallback_greedy_modularity(graph)

    assert result == {}


def test_fallback_greedy_modularity_nodes_without_edges():
    graph = nx.Graph()
    graph.add_node("a")
    graph.add_node("b")

    result = _fallback_greedy_modularity(graph)

    assert result == {
        "a": 0,
        "b": 1,
    }


def test_fallback_greedy_modularity_with_edges():
    graph = nx.Graph()
    graph.add_edge("a", "b", weight=1)
    graph.add_edge("c", "d", weight=1)

    result = _fallback_greedy_modularity(graph)

    assert set(result.keys()) == {"a", "b", "c", "d"}


def test_fallback_greedy_modularity_directed_graph():
    graph = nx.DiGraph()
    graph.add_edge("a", "b", weight=1)

    result = _fallback_greedy_modularity(graph)

    assert set(result.keys()) == {"a", "b"}