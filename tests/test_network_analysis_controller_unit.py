from unittest.mock import MagicMock

import networkx as nx
import pytest
from fastapi import HTTPException

from app.controllers import network_analysis_controller as controller


class FakeResult:
    def __init__(self, records):
        self._records = records

    def data(self):
        return self._records


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.run_calls = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def run(self, query, **kwargs):
        self.run_calls.append((query, kwargs))

        if self.responses:
            return FakeResult(self.responses.pop(0))

        return FakeResult([])


class FakeDriver:
    def __init__(self, responses):
        self.session_instance = FakeSession(responses)

    def session(self):
        return self.session_instance


class FakeDoc:
    def __init__(self, doc_id, data):
        self.id = doc_id
        self._data = data

    def to_dict(self):
        return self._data


def build_sample_graph():
    graph = nx.DiGraph()

    graph.add_node(
        "user_1",
        type="user",
        source="app",
        raw_id="1",
        name="Budi",
        username="budi",
        label="Budi",
        community=0,
    )

    graph.add_node(
        "user_2",
        type="user",
        source="app",
        raw_id="2",
        name="Siti",
        username="siti",
        label="Siti",
        community=0,
    )

    graph.add_node(
        "user_3",
        type="user",
        source="app",
        raw_id="3",
        name="Andi",
        username="andi",
        label="Andi",
        community=1,
    )

    graph.add_edge("user_1", "user_2", weight=3, relation="COMMENT")
    graph.add_edge("user_2", "user_3", weight=2, relation="MENTION")

    return graph


def test_safe_int():
    assert controller._safe_int("10", default=5, minimum=1, maximum=20) == 10
    assert controller._safe_int("bad", default=5, minimum=1, maximum=20) == 5
    assert controller._safe_int(100, default=5, minimum=1, maximum=20) == 20
    assert controller._safe_int(-10, default=5, minimum=1, maximum=20) == 1


def test_normalize_source():
    assert controller._normalize_source("app") == "app"
    assert controller._normalize_source(" instagram ") == "instagram"
    assert controller._normalize_source(None) == "app"

    with pytest.raises(HTTPException) as excinfo:
        controller._normalize_source("twitter")

    assert excinfo.value.status_code == 400


def test_extract_mentions():
    assert controller._extract_mentions("") == []
    assert controller._extract_mentions("Halo @Budi dan @siti.") == ["budi", "siti"]
    assert controller._extract_mentions("@Budi @budi @Siti") == ["budi", "siti"]


def test_safe_section_success_and_error():
    assert controller._safe_section("ok", lambda: {"status": "success"}) == {
        "status": "success"
    }

    result = controller._safe_section("bad", lambda: (_ for _ in ()).throw(Exception("boom")))

    assert result["status"] == "error"
    assert result["section"] == "bad"
    assert "boom" in result["message"]


def test_normalize_node_key_and_possible_node_ids():
    assert controller._normalize_node_key(" user_1 ") == "user_1"
    assert controller._normalize_node_key(None) == ""

    assert controller._possible_node_ids("") == []
    assert controller._possible_node_ids("1") == ["1", "user_1"]
    assert controller._possible_node_ids("user_1") == ["user_1"]


def test_resolve_node_id():
    graph = build_sample_graph()

    assert controller._resolve_node_id(graph, "user_1") == "user_1"
    assert controller._resolve_node_id(graph, "1") == "user_1"
    assert controller._resolve_node_id(graph, "Budi") == "user_1"
    assert controller._resolve_node_id(graph, "not-found") is None


def test_node_to_response():
    graph = build_sample_graph()

    result = controller._node_to_response(graph, "user_1")

    assert result["id"] == "user_1"
    assert result["raw_id"] == "1"
    assert result["label"] == "Budi"
    assert result["type"] == "user"


def test_edge_to_response():
    graph = build_sample_graph()

    result = controller._edge_to_response(graph, "user_1", "user_2")

    assert result["source"] == "user_1"
    assert result["target"] == "user_2"
    assert result["weight"] == 3
    assert result["relation"] == "COMMENT"


def test_add_or_update_edge():
    graph = nx.DiGraph()

    controller._add_or_update_edge(graph, "a", "b", 1, "LIKE")
    controller._add_or_update_edge(graph, "a", "b", 2, "COMMENT")
    controller._add_or_update_edge(graph, "a", "b", 3, "LIKE")

    assert graph["a"]["b"]["weight"] == 6
    assert graph["a"]["b"]["relation"] == "LIKE, COMMENT"


def test_apply_communities_to_graph_empty():
    graph = nx.Graph()

    assert controller._apply_communities_to_graph(graph) == {}


def test_apply_communities_to_graph_success(monkeypatch):
    graph = build_sample_graph()

    monkeypatch.setattr(
        controller,
        "apply_leiden_communities",
        lambda graph, weight_attr="weight": {
            "user_1": 0,
            "user_2": 0,
            "user_3": 1,
        },
    )

    result = controller._apply_communities_to_graph(graph)

    assert result["user_1"] == 0
    assert graph.nodes["user_1"]["community"] == 0


def test_apply_communities_to_graph_fallback(monkeypatch):
    graph = build_sample_graph()

    def raise_error(*args, **kwargs):
        raise Exception("leiden failed")

    monkeypatch.setattr(controller, "apply_leiden_communities", raise_error)

    result = controller._apply_communities_to_graph(graph)

    assert isinstance(result, dict)
    assert "user_1" in result


def test_build_app_user_graph(monkeypatch):
    responses = [
        [
            {
                "source_id": "1",
                "source_name": "Budi",
                "source_username": "budi",
                "target_id": "2",
                "target_name": "Siti",
                "target_username": "siti",
                "weight": 3,
                "relations": ["COMMENT"],
            },
            {
                "source_id": "",
                "target_id": "3",
            },
        ],
        [
            {
                "source_id": "1",
                "source_name": "Budi",
                "source_username": "budi",
                "text": "Halo @andi @budi",
            }
        ],
    ]

    fake_driver = FakeDriver(responses)
    monkeypatch.setattr(controller, "neo4j_driver", fake_driver)
    monkeypatch.setattr(controller, "_apply_communities_to_graph", lambda graph: {})

    graph = controller._build_app_user_graph(max_edges=100)

    assert "user_1" in graph.nodes
    assert "user_2" in graph.nodes
    assert "user_andi" in graph.nodes
    assert graph.has_edge("user_1", "user_2")
    assert graph.has_edge("user_1", "user_andi")


def test_build_instagram_user_graph(monkeypatch):
    responses = [
        [
            {
                "source_id": "user_a",
                "source_name": "user_a",
                "source_username": "user_a",
                "target_id": "user_b",
                "target_name": "user_b",
                "target_username": "user_b",
                "weight": 4,
                "relations": ["REPLY"],
            },
            {
                "source_id": "suarasurabayamedia",
                "target_id": "user_x",
            },
        ],
        [
            {
                "source_id": "user_a",
                "source_name": "user_a",
                "text": "Halo @user_c @user_a",
            }
        ],
    ]

    fake_driver = FakeDriver(responses)
    monkeypatch.setattr(controller, "neo4j_driver", fake_driver)
    monkeypatch.setattr(controller, "_apply_communities_to_graph", lambda graph: {})

    graph = controller._build_instagram_user_graph(max_edges=100)

    assert "user_user_a" in graph.nodes
    assert "user_user_b" in graph.nodes
    assert "user_user_c" in graph.nodes
    assert graph.has_edge("user_user_a", "user_user_b")
    assert graph.has_edge("user_user_a", "user_user_c")


def test_build_user_graph_app_success(monkeypatch):
    graph = build_sample_graph()

    monkeypatch.setattr(controller, "_build_app_user_graph", lambda max_edges=25000: graph)

    result = controller._build_user_graph("app", max_edges=100)

    assert result.number_of_nodes() == 3


def test_build_user_graph_instagram_success(monkeypatch):
    graph = build_sample_graph()

    monkeypatch.setattr(controller, "_build_instagram_user_graph", lambda max_edges=25000: graph)

    result = controller._build_user_graph("instagram", max_edges=100)

    assert result.number_of_edges() == 2


def test_build_user_graph_empty_raises_404(monkeypatch):
    monkeypatch.setattr(controller, "_build_app_user_graph", lambda max_edges=25000: nx.DiGraph())

    with pytest.raises(HTTPException) as excinfo:
        controller._build_user_graph("app")

    assert excinfo.value.status_code == 404


def test_build_user_graph_connection_error(monkeypatch):
    def raise_error(max_edges=25000):
        raise Exception("neo4j error")

    monkeypatch.setattr(controller, "_build_app_user_graph", raise_error)

    with pytest.raises(HTTPException) as excinfo:
        controller._build_user_graph("app")

    assert excinfo.value.status_code == 503


def test_list_available_nodes(monkeypatch):
    graph = build_sample_graph()

    monkeypatch.setattr(controller, "_build_user_graph", lambda source="app", max_edges=25000: graph)

    result = controller.list_available_nodes(
        source="app",
        keyword="budi",
        max_edges=100,
        limit=10,
    )

    assert result["status"] == "success"
    assert result["total_returned"] == 1
    assert result["nodes"][0]["id"] == "user_1"


def test_get_node_neighbors_success(monkeypatch):
    graph = build_sample_graph()

    monkeypatch.setattr(controller, "_build_user_graph", lambda source="app", max_edges=25000: graph)

    result = controller.get_node_neighbors(
        source="app",
        node="user_1",
        max_edges=100,
        limit=10,
    )

    assert result["status"] == "success"
    assert result["node"]["id"] == "user_1"
    assert result["total_neighbors"] == 1


def test_get_node_neighbors_missing_node_argument():
    with pytest.raises(HTTPException) as excinfo:
        controller.get_node_neighbors(node="")

    assert excinfo.value.status_code == 400


def test_get_node_neighbors_not_found(monkeypatch):
    graph = build_sample_graph()

    monkeypatch.setattr(controller, "_build_user_graph", lambda source="app", max_edges=25000: graph)

    with pytest.raises(HTTPException) as excinfo:
        controller.get_node_neighbors(node="missing")

    assert excinfo.value.status_code == 404


def test_get_mention_edges(monkeypatch):
    graph = build_sample_graph()

    monkeypatch.setattr(controller, "_build_user_graph", lambda source="app", max_edges=25000: graph)

    result = controller.get_mention_edges(
        source="app",
        max_edges=100,
        limit=10,
    )

    assert result["status"] == "success"
    assert result["total_mentions"] == 1


def test_get_shortest_path_missing_arguments():
    with pytest.raises(HTTPException) as excinfo:
        controller.get_shortest_path(source_node="", target_node="")

    assert excinfo.value.status_code == 400


def test_get_shortest_path_same_node(monkeypatch):
    graph = build_sample_graph()

    monkeypatch.setattr(controller, "_build_user_graph", lambda source="app", max_edges=25000: graph)

    result = controller.get_shortest_path(
        source="app",
        source_node="user_1",
        target_node="user_1",
    )

    assert result["status"] == "success"
    assert result["path_length"] == 0
    assert result["edges"] == []


def test_get_shortest_path_success(monkeypatch):
    graph = build_sample_graph()

    monkeypatch.setattr(controller, "_build_user_graph", lambda source="app", max_edges=25000: graph)

    result = controller.get_shortest_path(
        source="app",
        source_node="user_1",
        target_node="user_3",
    )

    assert result["status"] == "success"
    assert result["path_length"] == 2
    assert len(result["nodes"]) == 3
    assert len(result["edges"]) == 2


def test_get_shortest_path_source_not_found(monkeypatch):
    graph = build_sample_graph()

    monkeypatch.setattr(controller, "_build_user_graph", lambda source="app", max_edges=25000: graph)

    with pytest.raises(HTTPException) as excinfo:
        controller.get_shortest_path(
            source="app",
            source_node="missing",
            target_node="user_3",
        )

    assert excinfo.value.status_code == 404


def test_get_shortest_path_no_path(monkeypatch):
    graph = build_sample_graph()
    graph.add_node("user_99", label="Lonely", type="user")

    monkeypatch.setattr(controller, "_build_user_graph", lambda source="app", max_edges=25000: graph)

    result = controller.get_shortest_path(
        source="app",
        source_node="user_1",
        target_node="user_99",
    )

    assert result["status"] == "not_found"
    assert result["path_length"] is None


def test_get_cliques(monkeypatch):
    graph = nx.DiGraph()

    for node in ["user_1", "user_2", "user_3"]:
        graph.add_node(
            node,
            type="user",
            source="app",
            raw_id=node.replace("user_", ""),
            label=node,
        )

    graph.add_edge("user_1", "user_2", weight=2, relation="COMMENT")
    graph.add_edge("user_2", "user_3", weight=2, relation="COMMENT")
    graph.add_edge("user_1", "user_3", weight=2, relation="COMMENT")

    monkeypatch.setattr(controller, "_build_user_graph", lambda source="app", max_edges=25000: graph)

    result = controller.get_cliques(
        source="app",
        max_edges=100,
        min_size=3,
        limit=10,
    )

    assert result["status"] == "success"
    assert result["total_cliques_found"] == 1
    assert result["top_cliques"][0]["size"] == 3


def test_community_summary():
    graph = build_sample_graph()

    result = controller._community_summary(graph, limit=10)

    assert result["total_communities"] == 2
    assert len(result["top_communities"]) == 2


def test_get_edge_weight_schema():
    result = controller.get_edge_weight_schema()

    assert result["status"] == "success"
    assert "data" in result


def test_get_graph_analysis_summary(monkeypatch):
    graph = build_sample_graph()

    monkeypatch.setattr(controller, "_build_user_graph", lambda source="app", max_edges=25000: graph)
    monkeypatch.setattr(
        controller,
        "get_cliques",
        lambda source="app", min_size=3, limit=10: {
            "status": "success",
            "top_cliques": [],
        },
    )

    result = controller._get_graph_analysis_summary(source="app")

    assert result["status"] == "success"
    assert result["graph_info"]["nodes_count"] == 3
    assert result["mention_summary"]["total_mentions"] == 1


def test_get_network_metrics_full_summary(monkeypatch):
    monkeypatch.setattr(
        controller.report_controller,
        "get_network_metrics_summary",
        lambda source: {
            "status": "success",
            "source": source,
        },
    )

    monkeypatch.setattr(
        controller,
        "_get_graph_analysis_summary",
        lambda source="app": {
            "status": "success",
            "graph_info": {},
        },
    )

    result = controller.get_network_metrics_full_summary(source="app")

    assert result["status"] == "success"
    assert result["source_active"] == "app"
    assert result["data"]["features"]["clique_detection"]["available"] is True


def test_get_graph_png_data(monkeypatch):
    graph = build_sample_graph()

    monkeypatch.setattr(controller, "_build_user_graph", lambda source="app", max_edges=25000: graph)

    result = controller.get_graph_png_data(
        source="app",
        max_edges=100,
        limit=10,
    )

    assert result["status"] == "success"
    assert result["graph"]["nodes_count"] == 3
    assert result["graph"]["edges_count"] == 2


def test_save_monthly_report_history_success(monkeypatch):
    fake_db = MagicMock()

    monkeypatch.setattr(controller, "db", fake_db)

    result = controller.save_monthly_report_history(
        {
            "source_active": "app",
            "period": {
                "year": 2026,
                "month": 5,
            },
        }
    )

    assert result["status"] == "success"
    assert result["doc_id"] == "app_2026_5"


def test_save_monthly_report_history_error(monkeypatch):
    fake_db = MagicMock()
    fake_db.collection.side_effect = Exception("firestore error")

    monkeypatch.setattr(controller, "db", fake_db)

    result = controller.save_monthly_report_history(
        {
            "source_active": "app",
            "period": {
                "year": 2026,
                "month": 5,
            },
        }
    )

    assert result["status"] == "error"


def test_list_monthly_report_history_success(monkeypatch):
    doc = FakeDoc(
        "app_2026_5",
        {
            "source_active": "app",
            "report_type": "monthly",
            "period": {
                "year": 2026,
                "month": 5,
            },
            "saved_at": "2026-05-15T00:00:00",
        },
    )

    fake_db = MagicMock()
    fake_db.collection.return_value.order_by.return_value.limit.return_value.stream.return_value = [doc]

    monkeypatch.setattr(controller, "db", fake_db)

    result = controller.list_monthly_report_history(limit=20)

    assert result["status"] == "success"
    assert result["total_returned"] == 1
    assert result["reports"][0]["doc_id"] == "app_2026_5"


def test_list_monthly_report_history_error(monkeypatch):
    fake_db = MagicMock()
    fake_db.collection.side_effect = Exception("firestore error")

    monkeypatch.setattr(controller, "db", fake_db)

    result = controller.list_monthly_report_history(limit=20)

    assert result["status"] == "error"
    assert result["reports"] == []


def test_generate_monthly_report_without_save(monkeypatch):
    monkeypatch.setattr(
        controller.report_controller,
        "get_stats_summary",
        lambda: {
            "status": "success",
        },
    )

    monkeypatch.setattr(
        controller.report_controller,
        "get_top_content_summary",
        lambda source, start_date, end_date: {
            "status": "success",
            "source": source,
            "start_date": start_date,
            "end_date": end_date,
        },
    )

    monkeypatch.setattr(
        controller,
        "get_network_metrics_full_summary",
        lambda source="app": {
            "status": "success",
            "source": source,
        },
    )

    result = controller.generate_monthly_report(
        source="app",
        year=2026,
        month=2,
        save_history=False,
    )

    assert result["status"] == "success"
    assert result["period"]["start_date"] == "2026-02-01"
    assert result["period"]["end_date"] == "2026-02-28"
    assert "history" not in result


def test_generate_monthly_report_with_save(monkeypatch):
    monkeypatch.setattr(
        controller.report_controller,
        "get_stats_summary",
        lambda: {
            "status": "success",
        },
    )

    monkeypatch.setattr(
        controller.report_controller,
        "get_top_content_summary",
        lambda source, start_date, end_date: {
            "status": "success",
        },
    )

    monkeypatch.setattr(
        controller,
        "get_network_metrics_full_summary",
        lambda source="app": {
            "status": "success",
        },
    )

    monkeypatch.setattr(
        controller,
        "save_monthly_report_history",
        lambda report_data: {
            "status": "success",
            "doc_id": "app_2026_5",
        },
    )

    result = controller.generate_monthly_report(
        source="app",
        year=2026,
        month=5,
        save_history=True,
    )

    assert result["status"] == "success"
    assert result["history"]["status"] == "success"

def test_build_app_user_graph_skips_invalid_mentions_and_creates_missing_source(monkeypatch):
    responses = [
        [],
        [
            {
                "source_id": "",
                "source_name": "",
                "source_username": "",
                "text": "invalid @target",
            },
            {
                "source_id": "9",
                "source_name": "Rina",
                "source_username": "rina",
                "text": "Halo @rina @unknown_user_123 @target",
            },
        ],
    ]

    fake_driver = FakeDriver(responses)
    monkeypatch.setattr(controller, "neo4j_driver", fake_driver)
    monkeypatch.setattr(controller, "_apply_communities_to_graph", lambda graph: {})

    graph = controller._build_app_user_graph(max_edges=100)

    assert "user_9" in graph.nodes
    assert "user_target" in graph.nodes
    assert graph.has_edge("user_9", "user_target")
    assert not graph.has_node("user_rina")
    assert not graph.has_node("user_unknown_user_123")


def test_build_instagram_user_graph_skips_invalid_mentions_and_creates_missing_source(monkeypatch):
    responses = [
        [],
        [
            {
                "source_id": "",
                "source_name": "",
                "text": "invalid @target",
            },
            {
                "source_id": "user_z",
                "source_name": "User Z",
                "text": "Halo @user_z @suarasurabayamedia @user_target",
            },
        ],
    ]

    fake_driver = FakeDriver(responses)
    monkeypatch.setattr(controller, "neo4j_driver", fake_driver)
    monkeypatch.setattr(controller, "_apply_communities_to_graph", lambda graph: {})

    graph = controller._build_instagram_user_graph(max_edges=100)

    assert "user_user_z" in graph.nodes
    assert "user_user_target" in graph.nodes
    assert graph.has_edge("user_user_z", "user_user_target")
    assert not graph.has_node("user_suarasurabayamedia")


def test_build_user_graph_reraises_http_exception(monkeypatch):
    def raise_http_exception(max_edges=25000):
        raise HTTPException(status_code=418, detail="custom error")

    monkeypatch.setattr(controller, "_build_app_user_graph", raise_http_exception)

    with pytest.raises(HTTPException) as excinfo:
        controller._build_user_graph("app")

    assert excinfo.value.status_code == 418
    assert excinfo.value.detail == "custom error"


def test_get_shortest_path_target_not_found(monkeypatch):
    graph = build_sample_graph()

    monkeypatch.setattr(
        controller,
        "_build_user_graph",
        lambda source="app", max_edges=25000: graph,
    )

    with pytest.raises(HTTPException) as excinfo:
        controller.get_shortest_path(
            source="app",
            source_node="user_1",
            target_node="missing",
        )

    assert excinfo.value.status_code == 404
    assert "Node target" in excinfo.value.detail


def test_get_shortest_path_uses_fallback_undirected_edge(monkeypatch):
    graph = nx.DiGraph()

    graph.add_node(
        "user_1",
        type="user",
        source="app",
        raw_id="1",
        name="Budi",
        username="budi",
        label="Budi",
    )

    graph.add_node(
        "user_2",
        type="user",
        source="app",
        raw_id="2",
        name="Siti",
        username="siti",
        label="Siti",
    )

    graph.add_node(
        "user_3",
        type="user",
        source="app",
        raw_id="3",
        name="Andi",
        username="andi",
        label="Andi",
    )

    # Ini sengaja dibalik supaya shortest path dari user_1 ke user_3
    # masuk ke branch fallback edge_data dari undirected graph.
    graph.add_edge("user_2", "user_1", weight=5, relation="REVERSE_COMMENT")
    graph.add_edge("user_3", "user_2", weight=4, relation="REVERSE_MENTION")

    monkeypatch.setattr(
        controller,
        "_build_user_graph",
        lambda source="app", max_edges=25000: graph,
    )

    result = controller.get_shortest_path(
        source="app",
        source_node="user_1",
        target_node="user_3",
    )

    assert result["status"] == "success"
    assert result["path_length"] == 2
    assert len(result["edges"]) == 2


def test_get_cliques_skips_small_cliques(monkeypatch):
    graph = nx.DiGraph()

    graph.add_node("user_1", type="user", source="app", raw_id="1", label="user_1")
    graph.add_node("user_2", type="user", source="app", raw_id="2", label="user_2")
    graph.add_edge("user_1", "user_2", weight=1, relation="COMMENT")

    monkeypatch.setattr(
        controller,
        "_build_user_graph",
        lambda source="app", max_edges=25000: graph,
    )

    result = controller.get_cliques(
        source="app",
        max_edges=100,
        min_size=3,
        limit=10,
    )

    assert result["status"] == "success"
    assert result["total_cliques_found"] == 0


def test_get_cliques_scan_limited(monkeypatch):
    graph = nx.Graph()

    for index in range(1, 8):
        graph.add_node(
            f"user_{index}",
            type="user",
            source="app",
            raw_id=str(index),
            label=f"user_{index}",
        )

    graph.add_edge("user_1", "user_2", weight=1, relation="COMMENT")
    graph.add_edge("user_2", "user_3", weight=1, relation="COMMENT")

    monkeypatch.setattr(
        controller,
        "_build_user_graph",
        lambda source="app", max_edges=25000: graph,
    )

    def fake_find_cliques(graph):
        for index in range(5002):
            yield ["user_1", "user_2", "user_3"]

    monkeypatch.setattr(controller.nx, "find_cliques", fake_find_cliques)

    result = controller.get_cliques(
        source="app",
        max_edges=100,
        min_size=3,
        limit=1,
    )

    assert result["status"] == "success"
    assert result["is_scan_limited"] is True


def test_generate_monthly_report_uses_default_year_month(monkeypatch):
    monkeypatch.setattr(
        controller.report_controller,
        "get_stats_summary",
        lambda: {
            "status": "success",
        },
    )

    monkeypatch.setattr(
        controller.report_controller,
        "get_top_content_summary",
        lambda source, start_date, end_date: {
            "status": "success",
            "source": source,
            "start_date": start_date,
            "end_date": end_date,
        },
    )

    monkeypatch.setattr(
        controller,
        "get_network_metrics_full_summary",
        lambda source="app": {
            "status": "success",
            "source": source,
        },
    )

    result = controller.generate_monthly_report(
        source="app",
        year=None,
        month=None,
        save_history=False,
    )

    assert result["status"] == "success"
    assert result["period"]["year"] is not None
    assert result["period"]["month"] is not None


def test_get_shortest_path_uses_final_else_fallback_edge(monkeypatch):
    class FakeUndirectedGraph:
        def __init__(self):
            self._nodes = {
                "user_1": {
                    "type": "user",
                    "source": "app",
                    "raw_id": "1",
                    "name": "Budi",
                    "username": "budi",
                    "label": "Budi",
                },
                "user_2": {
                    "type": "user",
                    "source": "app",
                    "raw_id": "2",
                    "name": "Siti",
                    "username": "siti",
                    "label": "Siti",
                },
            }

        @property
        def nodes(self):
            return self

        def __contains__(self, node_id):
            return node_id in self._nodes

        def __getitem__(self, node_id):
            return self._nodes[node_id]

        def data(self, data=True):
            return self._nodes.items()

        def number_of_nodes(self):
            return 2

        def number_of_edges(self):
            return 1

        def get_edge_data(self, source, target, default=None):
            if {source, target} == {"user_1", "user_2"}:
                return {
                    "weight": 9,
                    "relation": "FALLBACK_EDGE",
                }

            return default or {}

    class FakeDirectedGraph:
        def to_undirected(self):
            return FakeUndirectedGraph()

        def has_edge(self, source, target):
            return False

    fake_directed_graph = FakeDirectedGraph()

    monkeypatch.setattr(
        controller,
        "_build_user_graph",
        lambda source="app", max_edges=25000: fake_directed_graph,
    )

    monkeypatch.setattr(
        controller.nx,
        "shortest_path",
        lambda graph, source, target: ["user_1", "user_2"],
    )

    result = controller.get_shortest_path(
        source="app",
        source_node="user_1",
        target_node="user_2",
        max_edges=100,
    )

    assert result["status"] == "success"
    assert result["source_active"] == "app"
    assert result["path_length"] == 1

    assert len(result["nodes"]) == 2
    assert result["nodes"][0]["id"] == "user_1"
    assert result["nodes"][1]["id"] == "user_2"

    assert len(result["edges"]) == 1
    assert result["edges"][0]["source"] == "user_1"
    assert result["edges"][0]["target"] == "user_2"
    assert result["edges"][0]["weight"] == 9
    assert result["edges"][0]["relation"] == "FALLBACK_EDGE"