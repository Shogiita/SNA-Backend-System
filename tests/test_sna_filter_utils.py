import networkx as nx

from app.utils.sna_filter_utils import (
    normalize_text,
    normalize_hashtag,
    is_ignored_hashtag,
    is_ignored_app_user,
    is_ignored_instagram_user,
    is_ignored_node,
    clean_graph_nodes,
    prepare_graph_for_centrality,
    calculate_centrality,
)


def test_normalize_text():
    assert normalize_text(None) == ""
    assert normalize_text("  Budi  ") == "budi"


def test_normalize_hashtag_lowercase_remove_hash_and_symbols():
    assert normalize_hashtag("#Surabaya!") == "surabaya"
    assert normalize_hashtag("Jatim_2026") == "jatim_2026"


def test_is_ignored_hashtag():
    assert is_ignored_hashtag("") is True
    assert is_ignored_hashtag(None) is True
    assert is_ignored_hashtag("a") is True
    assert is_ignored_hashtag("suarasurabaya") is True
    assert is_ignored_hashtag("surabaya") is False


def test_is_ignored_app_user():
    assert is_ignored_app_user("") is True
    assert is_ignored_app_user(None) is True
    assert is_ignored_app_user("unknown") is True
    assert is_ignored_app_user("unknown_user_123") is True
    assert is_ignored_app_user("user_unknown_123") is True
    assert is_ignored_app_user("@Budi") is False


def test_is_ignored_instagram_user():
    assert is_ignored_instagram_user("") is True
    assert is_ignored_instagram_user(None) is True
    assert is_ignored_instagram_user("@suarasurabayamedia") is True
    assert is_ignored_instagram_user("warga_surabaya") is False


def test_is_ignored_node_non_user_false():
    assert is_ignored_node(
        "post_1",
        {"type": "post", "label": "Post"},
        "app",
    ) is False


def test_is_ignored_node_app_user_true():
    assert is_ignored_node(
        "user_unknown",
        {"type": "user", "label": "unknown"},
        "app",
    ) is True


def test_is_ignored_node_instagram_user_true():
    assert is_ignored_node(
        "user_ss",
        {"type": "user", "label": "suarasurabayamedia"},
        "instagram",
    ) is True


def test_is_ignored_node_unknown_source_false():
    assert is_ignored_node(
        "user_1",
        {"type": "user", "label": "Budi"},
        "twitter",
    ) is False


def test_clean_graph_nodes_removes_ignored_users_and_isolates():
    graph = nx.Graph()
    graph.add_node("user_unknown", type="user", label="unknown")
    graph.add_node("user_budi", type="user", label="Budi")
    graph.add_node("post_1", type="post", label="Post")
    graph.add_node("isolated", type="post", label="Isolated")
    graph.add_edge("user_budi", "post_1", weight=1)

    result = clean_graph_nodes(graph, source="app")

    assert "user_unknown" not in result.nodes
    assert "isolated" not in result.nodes
    assert "user_budi" in result.nodes
    assert "post_1" in result.nodes


def test_prepare_graph_for_centrality_normalizes_weight_and_distance():
    graph = nx.Graph()
    graph.add_edge("a", "b", weight="2")
    graph.add_edge("b", "c", weight="invalid")
    graph.add_edge("c", "d", weight=0)

    result = prepare_graph_for_centrality(graph)

    assert result["a"]["b"]["weight"] == 2.0
    assert result["a"]["b"]["distance"] == 0.5

    assert result["b"]["c"]["weight"] == 1.0
    assert result["b"]["c"]["distance"] == 1.0

    assert result["c"]["d"]["weight"] == 1.0
    assert result["c"]["d"]["distance"] == 1.0


def test_calculate_centrality_empty_graph():
    result = calculate_centrality(nx.Graph())

    assert result["degree"] == {}
    assert result["pagerank"] == {}


def test_calculate_centrality_undirected_graph():
    graph = nx.Graph()
    graph.add_edge("a", "b", weight=2)
    graph.add_edge("b", "c", weight=1)

    result = calculate_centrality(graph)

    assert set(result.keys()) == {
        "degree",
        "in_degree",
        "out_degree",
        "betweenness",
        "closeness",
        "eigenvector",
        "pagerank",
    }
    assert result["in_degree"] == {}
    assert result["out_degree"] == {}
    assert "a" in result["degree"]
    assert "b" in result["pagerank"]


def test_calculate_centrality_directed_graph():
    graph = nx.DiGraph()
    graph.add_edge("a", "b", weight=2)
    graph.add_edge("b", "c", weight=1)

    result = calculate_centrality(graph)

    assert "a" in result["degree"]
    assert "a" in result["out_degree"]
    assert "b" in result["in_degree"]


def test_calculate_centrality_fallback_when_eigenvector_and_pagerank_fail(monkeypatch):
    graph = nx.Graph()
    graph.add_edge("a", "b", weight=1)

    def raise_error(*args, **kwargs):
        raise Exception("centrality failed")

    monkeypatch.setattr(nx, "eigenvector_centrality", raise_error)
    monkeypatch.setattr(nx, "eigenvector_centrality_numpy", raise_error)
    monkeypatch.setattr(nx, "pagerank", raise_error)

    result = calculate_centrality(graph)

    assert result["eigenvector"] == {"a": 0.0, "b": 0.0}
    assert result["pagerank"] == {"a": 0.0, "b": 0.0}