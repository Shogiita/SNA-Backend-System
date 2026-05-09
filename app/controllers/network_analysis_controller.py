import calendar
import re
import traceback
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

import networkx as nx
from fastapi import HTTPException

from app.controllers import report_controller
from app.database import db, neo4j_driver
from app.schema.network_analysis_schema import EDGE_WEIGHT_SCHEMA
from app.utils.leiden_utils import apply_leiden_communities
from app.utils.sna_filter_utils import (
    is_ignored_app_user,
    is_ignored_instagram_user,
)


MENTION_REGEX = re.compile(r"@([A-Za-z0-9._]+)")


def _safe_int(value, default: int, minimum: int, maximum: int) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError):
        number = default

    return max(minimum, min(number, maximum))


def _normalize_source(source: str) -> str:
    source = (source or "app").strip().lower()

    if source not in ["app", "instagram"]:
        raise HTTPException(
            status_code=400,
            detail="source harus bernilai 'app' atau 'instagram'."
        )

    return source


def _extract_mentions(text: str) -> List[str]:
    if not text:
        return []

    mentions = MENTION_REGEX.findall(str(text))
    cleaned_mentions = []

    for mention in mentions:
        mention = mention.strip().lower()

        if mention:
            cleaned_mentions.append(mention)

    return list(dict.fromkeys(cleaned_mentions))


def _safe_section(section_name: str, callback: Callable[[], Any]):
    try:
        return callback()
    except Exception as error:
        traceback.print_exc()
        return {
            "status": "error",
            "section": section_name,
            "message": str(error),
        }


def _normalize_node_key(value: str) -> str:
    return str(value or "").strip()


def _possible_node_ids(raw_node: str) -> List[str]:
    node = _normalize_node_key(raw_node)

    if not node:
        return []

    candidates = [node]

    if not node.startswith("user_"):
        candidates.append(f"user_{node}")

    return list(dict.fromkeys(candidates))


def _resolve_node_id(G: nx.Graph, raw_node: str) -> Optional[str]:
    candidates = _possible_node_ids(raw_node)

    for candidate in candidates:
        if candidate in G:
            return candidate

    raw_lower = _normalize_node_key(raw_node).lower()

    for node_id, attrs in G.nodes(data=True):
        searchable_values = [
            str(node_id),
            str(attrs.get("name", "")),
            str(attrs.get("label", "")),
            str(attrs.get("username", "")),
            str(attrs.get("raw_id", "")),
        ]

        for value in searchable_values:
            if value.strip().lower() == raw_lower:
                return node_id

    return None


def _node_to_response(G: nx.Graph, node_id: str) -> Dict:
    attrs = G.nodes[node_id]

    return {
        "id": node_id,
        "raw_id": attrs.get("raw_id", node_id.replace("user_", "")),
        "label": attrs.get("label") or attrs.get("name") or attrs.get("username") or node_id,
        "name": attrs.get("name"),
        "username": attrs.get("username"),
        "type": attrs.get("type", "user"),
        "source": attrs.get("source"),
        "community": attrs.get("community"),
    }


def _edge_to_response(G: nx.Graph, source: str, target: str) -> Dict:
    data = G.get_edge_data(source, target, default={})

    return {
        "source": source,
        "target": target,
        "weight": data.get("weight", 1),
        "relation": data.get("relation", "INTERACTION"),
    }


def _add_or_update_edge(
    G: nx.DiGraph,
    source_node: str,
    target_node: str,
    weight: float,
    relation: str,
):
    if G.has_edge(source_node, target_node):
        current_relation = str(G[source_node][target_node].get("relation", "INTERACTION"))

        if relation and relation not in current_relation:
            G[source_node][target_node]["relation"] = f"{current_relation}, {relation}"

        G[source_node][target_node]["weight"] += weight
    else:
        G.add_edge(
            source_node,
            target_node,
            weight=weight,
            relation=relation or "INTERACTION",
        )


def _apply_communities_to_graph(G: nx.Graph) -> Dict[str, int]:
    if G.number_of_nodes() == 0:
        return {}

    try:
        community_map = apply_leiden_communities(G, weight_attr="weight")
    except Exception:
        community_map = {}

        for index, component in enumerate(nx.connected_components(G.to_undirected())):
            for node in component:
                community_map[node] = index

    for node_id, community_id in community_map.items():
        if node_id in G:
            G.nodes[node_id]["community"] = community_id

    return community_map


def _build_app_user_graph(max_edges: int = 25000) -> nx.DiGraph:
    G = nx.DiGraph()

    query = """
    CALL () {
        MATCH (u1:FirebaseUser)-[:LIKES_KAWAN_FB]->(p:FirebaseKawanSS)<-[:POSTED_FB]-(u2:FirebaseUser)
        WHERE u1.id <> u2.id
        RETURN u1.id AS source_id,
               coalesce(u1.nama, u1.username, u1.id, '') AS source_name,
               coalesce(u1.username, u1.nama, u1.id, '') AS source_username,
               u2.id AS target_id,
               coalesce(u2.nama, u2.username, u2.id, '') AS target_name,
               coalesce(u2.username, u2.nama, u2.id, '') AS target_username,
               1 AS weight,
               'LIKE' AS relation

        UNION ALL

        MATCH (u1:FirebaseUser)-[:LIKES_INFO_FB]->(p:FirebaseInfoss)<-[:POSTED_FB]-(u2:FirebaseUser)
        WHERE u1.id <> u2.id
        RETURN u1.id AS source_id,
               coalesce(u1.nama, u1.username, u1.id, '') AS source_name,
               coalesce(u1.username, u1.nama, u1.id, '') AS source_username,
               u2.id AS target_id,
               coalesce(u2.nama, u2.username, u2.id, '') AS target_name,
               coalesce(u2.username, u2.nama, u2.id, '') AS target_username,
               1 AS weight,
               'LIKE' AS relation

        UNION ALL

        MATCH (u1:FirebaseUser)-[:WROTE_FB]->(c:FirebaseKawanSSComment)-[:COMMENTED_ON_FB]->(p:FirebaseKawanSS)<-[:POSTED_FB]-(u2:FirebaseUser)
        WHERE u1.id <> u2.id
        RETURN u1.id AS source_id,
               coalesce(u1.nama, u1.username, u1.id, '') AS source_name,
               coalesce(u1.username, u1.nama, u1.id, '') AS source_username,
               u2.id AS target_id,
               coalesce(u2.nama, u2.username, u2.id, '') AS target_name,
               coalesce(u2.username, u2.nama, u2.id, '') AS target_username,
               3 AS weight,
               'COMMENT' AS relation

        UNION ALL

        MATCH (u1:FirebaseUser)-[:WROTE_FB]->(c:FirebaseInfossComment)-[:COMMENTED_ON_FB]->(p:FirebaseInfoss)<-[:POSTED_FB]-(u2:FirebaseUser)
        WHERE u1.id <> u2.id
        RETURN u1.id AS source_id,
               coalesce(u1.nama, u1.username, u1.id, '') AS source_name,
               coalesce(u1.username, u1.nama, u1.id, '') AS source_username,
               u2.id AS target_id,
               coalesce(u2.nama, u2.username, u2.id, '') AS target_name,
               coalesce(u2.username, u2.nama, u2.id, '') AS target_username,
               3 AS weight,
               'COMMENT' AS relation
    }
    WITH source_id,
         source_name,
         source_username,
         target_id,
         target_name,
         target_username,
         relation,
         sum(weight) AS total_weight
    RETURN source_id,
           source_name,
           source_username,
           target_id,
           target_name,
           target_username,
           total_weight AS weight,
           collect(DISTINCT relation) AS relations
    ORDER BY weight DESC
    LIMIT $max_edges
    """

    with neo4j_driver.session() as session:
        records = session.run(query, max_edges=max_edges).data()

    for record in records:
        source_id = str(record.get("source_id", "")).strip()
        target_id = str(record.get("target_id", "")).strip()
        source_name = str(record.get("source_name", "")).strip()
        target_name = str(record.get("target_name", "")).strip()
        source_username = str(record.get("source_username", "")).strip()
        target_username = str(record.get("target_username", "")).strip()

        if (
            not source_id
            or not target_id
            or is_ignored_app_user(source_id)
            or is_ignored_app_user(target_id)
            or is_ignored_app_user(source_name)
            or is_ignored_app_user(target_name)
            or is_ignored_app_user(f"user_{source_id}")
            or is_ignored_app_user(f"user_{target_id}")
        ):
            continue

        source_node = f"user_{source_id}"
        target_node = f"user_{target_id}"

        G.add_node(
            source_node,
            type="user",
            source="app",
            raw_id=source_id,
            name=source_name,
            username=source_username,
            label=source_name or source_username or source_id,
        )

        G.add_node(
            target_node,
            type="user",
            source="app",
            raw_id=target_id,
            name=target_name,
            username=target_username,
            label=target_name or target_username or target_id,
        )

        relations = record.get("relations", [])
        relation_text = ", ".join(relations) if isinstance(relations, list) else str(relations)

        _add_or_update_edge(
            G,
            source_node,
            target_node,
            float(record.get("weight", 1)),
            relation_text or "INTERACTION",
        )

    mention_query = """
    CALL () {
        MATCH (u:FirebaseUser)-[:WROTE_FB]->(c:FirebaseKawanSSComment)
        WHERE coalesce(c.komentar, c.text, '') CONTAINS '@'
        RETURN u.id AS source_id,
               coalesce(u.nama, u.username, u.id, '') AS source_name,
               coalesce(u.username, u.nama, u.id, '') AS source_username,
               coalesce(c.komentar, c.text, '') AS text

        UNION ALL

        MATCH (u:FirebaseUser)-[:WROTE_FB]->(c:FirebaseInfossComment)
        WHERE coalesce(c.komentar, c.text, '') CONTAINS '@'
        RETURN u.id AS source_id,
               coalesce(u.nama, u.username, u.id, '') AS source_name,
               coalesce(u.username, u.nama, u.id, '') AS source_username,
               coalesce(c.komentar, c.text, '') AS text
    }
    RETURN source_id,
           source_name,
           source_username,
           text
    LIMIT $max_edges
    """

    with neo4j_driver.session() as session:
        mention_records = session.run(mention_query, max_edges=max_edges).data()

    for record in mention_records:
        source_id = str(record.get("source_id", "")).strip()
        source_name = str(record.get("source_name", "")).strip()
        source_username = str(record.get("source_username", "")).strip()
        text = str(record.get("text", "") or "")

        if (
            not source_id
            or is_ignored_app_user(source_id)
            or is_ignored_app_user(source_name)
            or is_ignored_app_user(source_username)
            or is_ignored_app_user(f"user_{source_id}")
        ):
            continue

        source_node = f"user_{source_id}"

        if not G.has_node(source_node):
            G.add_node(
                source_node,
                type="user",
                source="app",
                raw_id=source_id,
                name=source_name,
                username=source_username,
                label=source_name or source_username or source_id,
            )

        for mentioned_username in _extract_mentions(text):
            if is_ignored_app_user(mentioned_username):
                continue

            if mentioned_username in [source_id.lower(), source_username.lower()]:
                continue

            target_node = f"user_{mentioned_username}"

            if not G.has_node(target_node):
                G.add_node(
                    target_node,
                    type="mentioned_user",
                    source="app",
                    raw_id=mentioned_username,
                    name=mentioned_username,
                    username=mentioned_username,
                    label=mentioned_username,
                )

            _add_or_update_edge(G, source_node, target_node, 2, "MENTION")

    _apply_communities_to_graph(G)

    return G


def _build_instagram_user_graph(max_edges: int = 25000) -> nx.DiGraph:
    G = nx.DiGraph()

    query = """
    CALL () {
        MATCH (u1:InstagramUser)-[:WROTE_IG]->(c:InstagramComment)-[:COMMENTED_ON_IG]->(p:InstagramPost)<-[:POSTED_IG]-(u2:InstagramUser)
        WHERE u1.username <> u2.username
          AND toLower(coalesce(u1.username, '')) <> 'suarasurabayamedia'
          AND toLower(coalesce(u2.username, '')) <> 'suarasurabayamedia'
        RETURN u1.username AS source_id,
               u1.username AS source_name,
               u1.username AS source_username,
               u2.username AS target_id,
               u2.username AS target_name,
               u2.username AS target_username,
               3 AS weight,
               'COMMENT' AS relation

        UNION ALL

        MATCH (u1:InstagramUser)-[:WROTE_IG]->(r:InstagramComment)-[:REPLIED_TO_IG]->(c:InstagramComment)<-[:WROTE_IG]-(u2:InstagramUser)
        WHERE u1.username <> u2.username
          AND toLower(coalesce(u1.username, '')) <> 'suarasurabayamedia'
          AND toLower(coalesce(u2.username, '')) <> 'suarasurabayamedia'
        RETURN u1.username AS source_id,
               u1.username AS source_name,
               u1.username AS source_username,
               u2.username AS target_id,
               u2.username AS target_name,
               u2.username AS target_username,
               4 AS weight,
               'REPLY' AS relation
    }
    WITH source_id,
         source_name,
         source_username,
         target_id,
         target_name,
         target_username,
         relation,
         sum(weight) AS total_weight
    RETURN source_id,
           source_name,
           source_username,
           target_id,
           target_name,
           target_username,
           total_weight AS weight,
           collect(DISTINCT relation) AS relations
    ORDER BY weight DESC
    LIMIT $max_edges
    """

    with neo4j_driver.session() as session:
        records = session.run(query, max_edges=max_edges).data()

    for record in records:
        source_id = str(record.get("source_id", "")).strip()
        target_id = str(record.get("target_id", "")).strip()

        if (
            not source_id
            or not target_id
            or is_ignored_instagram_user(source_id)
            or is_ignored_instagram_user(target_id)
        ):
            continue

        source_node = f"user_{source_id}"
        target_node = f"user_{target_id}"

        G.add_node(
            source_node,
            type="user",
            source="instagram",
            raw_id=source_id,
            name=source_id,
            username=source_id,
            label=source_id,
        )

        G.add_node(
            target_node,
            type="user",
            source="instagram",
            raw_id=target_id,
            name=target_id,
            username=target_id,
            label=target_id,
        )

        relations = record.get("relations", [])
        relation_text = ", ".join(relations) if isinstance(relations, list) else str(relations)

        _add_or_update_edge(
            G,
            source_node,
            target_node,
            float(record.get("weight", 1)),
            relation_text or "INTERACTION",
        )

    mention_query = """
    MATCH (u:InstagramUser)-[:WROTE_IG]->(c:InstagramComment)
    WHERE u.username IS NOT NULL
      AND c.text IS NOT NULL
      AND c.text CONTAINS '@'
    RETURN u.username AS source_id,
           u.username AS source_name,
           c.text AS text
    LIMIT $max_edges
    """

    with neo4j_driver.session() as session:
        mention_records = session.run(mention_query, max_edges=max_edges).data()

    for record in mention_records:
        source_id = str(record.get("source_id", "")).strip()
        source_name = str(record.get("source_name", "")).strip()
        text = str(record.get("text", "") or "")

        if (
            not source_id
            or is_ignored_instagram_user(source_id)
            or is_ignored_instagram_user(source_name)
        ):
            continue

        source_node = f"user_{source_id}"

        if not G.has_node(source_node):
            G.add_node(
                source_node,
                type="user",
                source="instagram",
                raw_id=source_id,
                name=source_name or source_id,
                username=source_id,
                label=source_name or source_id,
            )

        for mentioned_username in _extract_mentions(text):
            if (
                not mentioned_username
                or is_ignored_instagram_user(mentioned_username)
                or mentioned_username == source_id.lower()
            ):
                continue

            target_node = f"user_{mentioned_username}"

            if not G.has_node(target_node):
                G.add_node(
                    target_node,
                    type="mentioned_user",
                    source="instagram",
                    raw_id=mentioned_username,
                    name=mentioned_username,
                    username=mentioned_username,
                    label=mentioned_username,
                )

            _add_or_update_edge(G, source_node, target_node, 2, "MENTION")

    _apply_communities_to_graph(G)

    return G


def _build_user_graph(source: str, max_edges: int = 25000) -> nx.DiGraph:
    source = _normalize_source(source)
    max_edges = _safe_int(max_edges, default=25000, minimum=100, maximum=50000)

    try:
        if source == "instagram":
            G = _build_instagram_user_graph(max_edges=max_edges)
        else:
            G = _build_app_user_graph(max_edges=max_edges)
    except HTTPException:
        raise
    except Exception as error:
        traceback.print_exc()
        raise HTTPException(
            status_code=503,
            detail={
                "status": "error",
                "message": f"Gagal membangun graf dari Neo4j: {str(error)}",
                "hint": (
                    "Cek koneksi Neo4j Aura, konfigurasi NEO4J_URI, "
                    "credential, dan koneksi jaringan."
                ),
            },
        )

    if G.number_of_nodes() == 0 or G.number_of_edges() == 0:
        raise HTTPException(
            status_code=404,
            detail=f"Tidak ada data graf user-user untuk source '{source}'."
        )

    return G


def list_available_nodes(
    source: str = "app",
    keyword: str = "",
    max_edges: int = 25000,
    limit: int = 20,
):
    source = _normalize_source(source)
    limit = _safe_int(limit, default=20, minimum=1, maximum=100)

    G = _build_user_graph(source=source, max_edges=max_edges)

    keyword = str(keyword or "").strip().lower()
    result = []

    for node_id, attrs in G.nodes(data=True):
        label = str(attrs.get("label") or "")
        name = str(attrs.get("name") or "")
        username = str(attrs.get("username") or "")
        raw_id = str(attrs.get("raw_id") or "")

        searchable = " ".join([node_id, label, name, username, raw_id]).lower()

        if keyword and keyword not in searchable:
            continue

        result.append({
            "id": node_id,
            "raw_id": raw_id,
            "label": label,
            "name": name,
            "username": username,
            "type": attrs.get("type", "user"),
            "community": attrs.get("community"),
            "degree": G.degree(node_id, weight="weight"),
        })

    result.sort(key=lambda item: item["degree"], reverse=True)

    return {
        "status": "success",
        "source_active": source,
        "keyword": keyword,
        "total_returned": len(result[:limit]),
        "nodes": result[:limit],
    }


def get_node_neighbors(
    source: str = "app",
    node: str = "",
    max_edges: int = 25000,
    limit: int = 20,
):
    source = _normalize_source(source)

    if not node:
        raise HTTPException(status_code=400, detail="node wajib diisi.")

    limit = _safe_int(limit, default=20, minimum=1, maximum=100)

    G_directed = _build_user_graph(source=source, max_edges=max_edges)
    G = G_directed.to_undirected()

    resolved_node = _resolve_node_id(G, node)

    if not resolved_node:
        raise HTTPException(
            status_code=404,
            detail=f"Node '{node}' tidak ditemukan pada graf."
        )

    neighbors = []

    for neighbor_id in G.neighbors(resolved_node):
        edge_data = G.get_edge_data(resolved_node, neighbor_id, default={})

        neighbors.append({
            "node": _node_to_response(G, neighbor_id),
            "weight": edge_data.get("weight", 1),
            "relation": edge_data.get("relation", "INTERACTION"),
            "degree": G.degree(neighbor_id, weight="weight"),
        })

    neighbors.sort(key=lambda item: item["degree"], reverse=True)

    return {
        "status": "success",
        "source_active": source,
        "node": _node_to_response(G, resolved_node),
        "total_neighbors": len(neighbors),
        "neighbors": neighbors[:limit],
    }


def get_mention_edges(
    source: str = "instagram",
    max_edges: int = 25000,
    limit: int = 50,
):
    source = _normalize_source(source)
    limit = _safe_int(limit, default=50, minimum=1, maximum=500)

    G = _build_user_graph(source=source, max_edges=max_edges)

    mention_edges = []

    for source_node, target_node, data in G.edges(data=True):
        relation = str(data.get("relation", ""))

        if "MENTION" not in relation:
            continue

        mention_edges.append({
            "source": _node_to_response(G, source_node),
            "target": _node_to_response(G, target_node),
            "weight": data.get("weight", 1),
            "relation": relation,
        })

    mention_edges.sort(key=lambda item: item["weight"], reverse=True)

    return {
        "status": "success",
        "source_active": source,
        "total_mentions": len(mention_edges),
        "mentions": mention_edges[:limit],
    }


def get_shortest_path(
    source: str = "app",
    source_node: str = "",
    target_node: str = "",
    max_edges: int = 25000,
):
    source = _normalize_source(source)

    if not source_node or not target_node:
        raise HTTPException(
            status_code=400,
            detail="source_node dan target_node wajib diisi."
        )

    G_directed = _build_user_graph(source=source, max_edges=max_edges)
    G = G_directed.to_undirected()

    resolved_source = _resolve_node_id(G, source_node)
    resolved_target = _resolve_node_id(G, target_node)

    if not resolved_source:
        raise HTTPException(
            status_code=404,
            detail=f"Node sumber '{source_node}' tidak ditemukan pada graf."
        )

    if not resolved_target:
        raise HTTPException(
            status_code=404,
            detail=f"Node target '{target_node}' tidak ditemukan pada graf."
        )

    if resolved_source == resolved_target:
        return {
            "status": "success",
            "source_active": source,
            "path_length": 0,
            "nodes": [_node_to_response(G, resolved_source)],
            "edges": [],
        }

    try:
        path_nodes = nx.shortest_path(G, source=resolved_source, target=resolved_target)
    except nx.NetworkXNoPath:
        return {
            "status": "not_found",
            "source_active": source,
            "message": "Tidak ditemukan jalur yang menghubungkan kedua node.",
            "path_length": None,
            "nodes": [],
            "edges": [],
        }

    path_edges = []

    for index in range(len(path_nodes) - 1):
        current_node = path_nodes[index]
        next_node = path_nodes[index + 1]

        if G_directed.has_edge(current_node, next_node):
            path_edges.append(_edge_to_response(G_directed, current_node, next_node))
        elif G_directed.has_edge(next_node, current_node):
            path_edges.append(_edge_to_response(G_directed, next_node, current_node))
        else:
            path_edges.append({
                "source": current_node,
                "target": next_node,
                "weight": 1,
                "relation": "INTERACTION",
            })

    return {
        "status": "success",
        "source_active": source,
        "requested": {
            "source_node": source_node,
            "target_node": target_node,
        },
        "resolved": {
            "source_node": resolved_source,
            "target_node": resolved_target,
        },
        "path_length": len(path_nodes) - 1,
        "nodes": [_node_to_response(G, node_id) for node_id in path_nodes],
        "edges": path_edges,
    }


def get_cliques(
    source: str = "app",
    max_edges: int = 25000,
    min_size: int = 3,
    limit: int = 10,
):
    source = _normalize_source(source)
    min_size = _safe_int(min_size, default=3, minimum=2, maximum=20)
    limit = _safe_int(limit, default=10, minimum=1, maximum=100)

    G_directed = _build_user_graph(source=source, max_edges=max_edges)
    G = G_directed.to_undirected()

    cliques = []

    for clique_nodes in nx.find_cliques(G):
        if len(clique_nodes) < min_size:
            continue

        total_weight = 0.0

        for node_a, node_b in nx.Graph(G.subgraph(clique_nodes)).edges():
            edge_data = G.get_edge_data(node_a, node_b, default={})
            total_weight += float(edge_data.get("weight", 1))

        cliques.append({
            "size": len(clique_nodes),
            "total_weight": total_weight,
            "nodes": [_node_to_response(G, node_id) for node_id in clique_nodes],
        })

    cliques.sort(key=lambda item: (item["size"], item["total_weight"]), reverse=True)

    return {
        "status": "success",
        "source_active": source,
        "graph_info": {
            "nodes_count": G.number_of_nodes(),
            "edges_count": G.number_of_edges(),
        },
        "filter": {
            "min_size": min_size,
            "limit": limit,
        },
        "total_cliques_found": len(cliques),
        "top_cliques": cliques[:limit],
    }


def _community_summary(G: nx.Graph, limit: int = 10):
    community_groups: Dict[str, List[str]] = {}

    for node_id, attrs in G.nodes(data=True):
        community_id = attrs.get("community", "unknown")
        community_groups.setdefault(str(community_id), []).append(node_id)

    communities = []

    for community_id, nodes in community_groups.items():
        sample_nodes = nodes[:10]

        communities.append({
            "community_id": community_id,
            "size": len(nodes),
            "sample_nodes": [_node_to_response(G, node_id) for node_id in sample_nodes],
        })

    communities.sort(key=lambda item: item["size"], reverse=True)

    return {
        "total_communities": len(communities),
        "top_communities": communities[:limit],
    }


def get_edge_weight_schema():
    return {
        "status": "success",
        "data": EDGE_WEIGHT_SCHEMA,
        "note": (
            "Skema bobot ini digunakan untuk membangun weighted graph. "
            "Mention dideteksi dari teks komentar/reply yang mengandung @username."
        ),
    }


def _get_graph_analysis_summary(source: str = "app"):
    G_directed = _build_user_graph(source=source, max_edges=25000)
    G = G_directed.to_undirected()

    mentions = []

    for source_node, target_node, data in G_directed.edges(data=True):
        if "MENTION" in str(data.get("relation", "")):
            mentions.append({
                "source": _node_to_response(G_directed, source_node),
                "target": _node_to_response(G_directed, target_node),
                "weight": data.get("weight", 1),
                "relation": data.get("relation", "MENTION"),
            })

    mentions.sort(key=lambda item: item["weight"], reverse=True)

    clique_summary = get_cliques(source=source, min_size=3, limit=10)

    return {
        "status": "success",
        "graph_info": {
            "nodes_count": G.number_of_nodes(),
            "edges_count": G.number_of_edges(),
        },
        "community_summary": _community_summary(G),
        "mention_summary": {
            "total_mentions": len(mentions),
            "top_mentions": mentions[:20],
        },
        "clique_summary": clique_summary,
    }


def get_network_metrics_full_summary(source: str = "app"):
    source = _normalize_source(source)

    centrality_summary = _safe_section(
        "centrality_and_legacy_network_metrics",
        lambda: report_controller.get_network_metrics_summary(source),
    )

    graph_summary = _safe_section(
        "graph_summary",
        lambda: _get_graph_analysis_summary(source=source),
    )

    return {
        "status": "success",
        "source_active": source,
        "data": {
            "centrality": centrality_summary,
            "graph_analysis": graph_summary,
            "features": {
                "centrality": {
                    "degree": True,
                    "betweenness": True,
                    "closeness": True,
                    "eigenvector": True,
                },
                "community_detection": {
                    "leiden": True,
                    "included_in_response": True,
                },
                "geodesic_path": {
                    "available": True,
                    "endpoint": "/report/network/shortest-path",
                },
                "clique_detection": {
                    "available": True,
                    "endpoint": "/report/network/cliques",
                },
                "mention_detection": {
                    "available": True,
                    "endpoint": "/report/network/mentions",
                },
            },
            "weight_schema": EDGE_WEIGHT_SCHEMA,
        },
    }


def get_graph_png_data(
    source: str = "app",
    max_edges: int = 25000,
    limit: int = 500,
):
    source = _normalize_source(source)
    limit = _safe_int(limit, default=500, minimum=10, maximum=5000)

    G = _build_user_graph(source=source, max_edges=max_edges)

    degree_map = dict(G.degree(weight="weight"))
    sorted_nodes = sorted(
        G.nodes(),
        key=lambda node_id: degree_map.get(node_id, 0),
        reverse=True,
    )[:limit]

    H = G.subgraph(sorted_nodes).copy()

    nodes = []

    for node_id in H.nodes():
        node_data = _node_to_response(H, node_id)
        node_data["degree"] = H.degree(node_id, weight="weight")
        nodes.append(node_data)

    edges = []

    for source_node, target_node, data in H.edges(data=True):
        edges.append({
            "source": source_node,
            "target": target_node,
            "weight": data.get("weight", 1),
            "relation": data.get("relation", "INTERACTION"),
        })

    return {
        "status": "success",
        "source_active": source,
        "export_mode": "frontend_snapshot",
        "recommended_formats": ["png", "jpeg"],
        "note": "Gunakan data nodes dan edges ini untuk render graph di Flutter, lalu export widget sebagai PNG/JPEG.",
        "graph": {
            "nodes_count": len(nodes),
            "edges_count": len(edges),
            "nodes": nodes,
            "edges": edges,
        },
    }


def save_monthly_report_history(report_data: Dict):
    try:
        period = report_data.get("period", {})
        source = report_data.get("source_active", "app")
        year = period.get("year")
        month = period.get("month")

        doc_id = f"{source}_{year}_{month}"

        payload = {
            **report_data,
            "saved_at": datetime.now().isoformat(),
        }

        db.collection("monthly_reports").document(doc_id).set(payload)

        return {
            "status": "success",
            "message": "Monthly report berhasil disimpan.",
            "doc_id": doc_id,
        }

    except Exception as error:
        traceback.print_exc()
        return {
            "status": "error",
            "message": str(error),
        }


def list_monthly_report_history(limit: int = 20):
    limit = _safe_int(limit, default=20, minimum=1, maximum=100)

    try:
        docs = (
            db.collection("monthly_reports")
            .order_by("saved_at", direction="DESCENDING")
            .limit(limit)
            .stream()
        )

        reports = []

        for doc in docs:
            data = doc.to_dict()
            reports.append({
                "doc_id": doc.id,
                "source_active": data.get("source_active"),
                "report_type": data.get("report_type"),
                "period": data.get("period"),
                "saved_at": data.get("saved_at"),
            })

        return {
            "status": "success",
            "total_returned": len(reports),
            "reports": reports,
        }

    except Exception as error:
        traceback.print_exc()
        return {
            "status": "error",
            "message": str(error),
            "reports": [],
        }


def get_monthly_report(
    source: str = "app",
    year: Optional[int] = None,
    month: Optional[int] = None,
    save_history: bool = False,
):
    source = _normalize_source(source)

    now = datetime.now()

    if year is None:
        year = now.year

    if month is None:
        month = now.month

    year = _safe_int(year, default=now.year, minimum=2000, maximum=2100)
    month = _safe_int(month, default=now.month, minimum=1, maximum=12)

    last_day = calendar.monthrange(year, month)[1]

    start_date = f"{year:04d}-{month:02d}-01"
    end_date = f"{year:04d}-{month:02d}-{last_day:02d}"

    stats = _safe_section(
        "stats_summary",
        lambda: report_controller.get_stats_summary(),
    )

    top_content = _safe_section(
        "top_content_summary",
        lambda: report_controller.get_top_content_summary(
            source=source,
            start_date=start_date,
            end_date=end_date,
        ),
    )

    network_metrics = _safe_section(
        "network_metrics_summary",
        lambda: get_network_metrics_full_summary(source=source),
    )

    report_data = {
        "status": "success",
        "source_active": source,
        "report_type": "monthly",
        "period": {
            "year": year,
            "month": month,
            "start_date": start_date,
            "end_date": end_date,
        },
        "sections": {
            "stats_summary": stats,
            "top_content_summary": top_content,
            "network_metrics_summary": network_metrics,
        },
    }

    if save_history:
        report_data["history"] = save_monthly_report_history(report_data)

    return report_data