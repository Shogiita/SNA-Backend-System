import os
import re
import networkx as nx
from fastapi import HTTPException
from fastapi.responses import HTMLResponse
from app.database import neo4j_driver
from app.utils.sna_filter_utils import (
    is_ignored_app_user,
    clean_graph_nodes,
    calculate_centrality,
)
from app.utils.leiden_utils import apply_leiden_communities


OUTPUT_HTML_DIR = "generated_graphs"
os.makedirs(OUTPUT_HTML_DIR, exist_ok=True)

HASHTAG_REGEX = re.compile(r"#\w+")

async def create_graph_visualization_from_neo4j(
    limit: int = 25000,
    mode: int = 2,
    max_edges: int = 25000
):
    try:
        G = nx.DiGraph()

        safe_max_edges = min(max_edges, 25000)

        with neo4j_driver.session() as session:
            if mode == 2:
                query = """
                MATCH (u:FirebaseUser)-[:POSTED_FB]->(p)
                WHERE p:FirebaseKawanSS OR p:FirebaseInfoss
                RETURN u.id AS uid,
                       coalesce(u.nama, u.username, u.id, '') AS uname,
                       p.id AS pid,
                       coalesce(p.deskripsi, p.judul, p.detail, p.title, '') AS text,
                       labels(p) AS p_labels,
                       coalesce(toInteger(p.jumlahLike), 0) AS likes
                LIMIT $max_edges
                """

                records = session.run(query, max_edges=safe_max_edges).data()

                for r in records:
                    uid = str(r.get("uid", "")).strip()
                    uname = str(r.get("uname", "")).strip()
                    pid = str(r.get("pid", "")).strip()

                    if (
                        not uid
                        or not pid
                        or is_ignored_app_user(uid)
                        or is_ignored_app_user(uname)
                        or is_ignored_app_user(f"user_{uid}")
                    ):
                        continue

                    user_node = f"user_{uid}"
                    post_node = f"post_{pid}"
                    text = r.get("text") or ""
                    labels = r.get("p_labels", [])

                    post_type = (
                        "post_infoss"
                        if "FirebaseInfoss" in labels
                        else "post_kawanss"
                    )

                    if not G.has_node(user_node):
                        G.add_node(
                            user_node,
                            type="user",
                            label=uname,
                            name=uname,
                        )

                    if not G.has_node(post_node):
                        G.add_node(
                            post_node,
                            type=post_type,
                            label=text[:20] + "..." if len(text) > 20 else text,
                            full_text=text[:300],
                            likes=r.get("likes", 0),
                        )

                    G.add_edge(
                        user_node,
                        post_node,
                        relation="AUTHORED",
                        weight=5,
                    )

            else:
                query = """
                CALL {
                    MATCH (u1:FirebaseUser)-[:LIKES_KAWAN_FB]->(p:FirebaseKawanSS)<-[:POSTED_FB]-(u2:FirebaseUser)
                    WHERE u1.id <> u2.id
                    RETURN u1.id AS source_id,
                           coalesce(u1.nama, u1.username, u1.id, '') AS source_name,
                           u2.id AS target_id,
                           coalesce(u2.nama, u2.username, u2.id, '') AS target_name,
                           1 AS weight,
                           'LIKE' AS relation

                    UNION ALL

                    MATCH (u1:FirebaseUser)-[:WROTE_FB]->(c:FirebaseKawanSSComment)-[:COMMENTED_ON_FB]->(p:FirebaseKawanSS)<-[:POSTED_FB]-(u2:FirebaseUser)
                    WHERE u1.id <> u2.id
                    RETURN u1.id AS source_id,
                           coalesce(u1.nama, u1.username, u1.id, '') AS source_name,
                           u2.id AS target_id,
                           coalesce(u2.nama, u2.username, u2.id, '') AS target_name,
                           3 AS weight,
                           'COMMENT' AS relation
                }
                LIMIT $max_edges
                """

                records = session.run(query, max_edges=safe_max_edges).data()

                for r in records:
                    source_id = str(r.get("source_id", "")).strip()
                    target_id = str(r.get("target_id", "")).strip()
                    source_name = str(r.get("source_name", "")).strip()
                    target_name = str(r.get("target_name", "")).strip()

                    if (
                        is_ignored_app_user(source_id)
                        or is_ignored_app_user(target_id)
                        or is_ignored_app_user(source_name)
                        or is_ignored_app_user(target_name)
                        or is_ignored_app_user(f"user_{source_id}")
                        or is_ignored_app_user(f"user_{target_id}")
                    ):
                        continue

                    source_node = f"user_{source_id}"
                    target_node = f"user_{target_id}"

                    if not G.has_node(source_node):
                        G.add_node(
                            source_node,
                            type="user",
                            name=source_name,
                            label=source_name,
                        )

                    if not G.has_node(target_node):
                        G.add_node(
                            target_node,
                            type="user",
                            name=target_name,
                            label=target_name,
                        )

                    G.add_edge(
                        source_node,
                        target_node,
                        relation=r.get("relation", "INTERACTION"),
                        weight=r.get("weight", 1),
                    )

        clean_graph_nodes(G, source="app")
        G.remove_nodes_from(list(nx.isolates(G)))

        if G.number_of_nodes() == 0:
            raise HTTPException(
                status_code=404,
                detail="Tidak ada relasi data yang ditemukan di Neo4j.",
            )

        apply_leiden_communities(G, weight_attr="weight")

        degree_map = dict(G.degree(weight="weight"))
        max_degree = max(degree_map.values()) if degree_map else 1

        nodes_output = []
        for node in G.nodes():
            attr = G.nodes[node].copy()
            degree = degree_map.get(node, 0)

            nodes_output.append({
                "id": node,
                "attributes": attr,
                "metrics": {
                    "degree": degree / max_degree if max_degree else 0.0,
                    "raw_degree": degree,
                    "betweenness": 0.0,
                    "closeness": 0.0,
                    "eigenvector": 0.0,
                    "pagerank": 0.0,
                },
            })

        edges_output = []
        for source, target, data in G.edges(data=True):
            edges_output.append({
                "source": source,
                "target": target,
                "weight": data.get("weight", 1),
                "attributes": {
                    "relation": data.get("relation", "INTERACTION"),
                },
            })

        return {
            "message": (
                f"Graf visualisasi berhasil dibuat "
                f"(Nodes: {len(nodes_output)}, Edges: {len(edges_output)}, Mode: {mode})."
            ),
            "graph_info": {
                "nodes_count": len(nodes_output),
                "edges_count": len(edges_output),
                "nodes": nodes_output,
                "edges": edges_output,
            },
        }

    except Exception as e:
        if isinstance(e, HTTPException):
            raise e

        import traceback
        traceback.print_exc()

        raise HTTPException(
            status_code=500,
            detail=f"Gagal membuat graf visualisasi Neo4j: {str(e)}",
        )
        
def _build_neo4j_graph_internal(limit: int, mode: int):
    G = nx.DiGraph()

    with neo4j_driver.session() as session:
        if mode == 1:
            query = """
            CALL {
                MATCH (u1:FirebaseUser)-[:LIKES_KAWAN_FB]->(p:FirebaseKawanSS)<-[:POSTED_FB]-(u2:FirebaseUser)
                WHERE u1.id <> u2.id
                RETURN u1.id AS source_id,
                       coalesce(u1.nama, u1.username, u1.id, 'Unknown') AS source_name,
                       u2.id AS target_id,
                       coalesce(u2.nama, u2.username, u2.id, 'Unknown') AS target_name,
                       1 AS w,
                       'LIKE' AS t

                UNION ALL

                MATCH (u1:FirebaseUser)-[:LIKES_INFO_FB]->(p:FirebaseInfoss)<-[:POSTED_FB]-(u2:FirebaseUser)
                WHERE u1.id <> u2.id
                RETURN u1.id AS source_id,
                       coalesce(u1.nama, u1.username, u1.id, 'Unknown') AS source_name,
                       u2.id AS target_id,
                       coalesce(u2.nama, u2.username, u2.id, 'Unknown') AS target_name,
                       1 AS w,
                       'LIKE' AS t

                UNION ALL

                MATCH (u1:FirebaseUser)-[:WROTE_FB]->(c:FirebaseKawanSSComment)-[:COMMENTED_ON_FB]->(p:FirebaseKawanSS)<-[:POSTED_FB]-(u2:FirebaseUser)
                WHERE u1.id <> u2.id
                RETURN u1.id AS source_id,
                       coalesce(u1.nama, u1.username, u1.id, 'Unknown') AS source_name,
                       u2.id AS target_id,
                       coalesce(u2.nama, u2.username, u2.id, 'Unknown') AS target_name,
                       3 AS w,
                       'COMMENT' AS t

                UNION ALL

                MATCH (u1:FirebaseUser)-[:WROTE_FB]->(c:FirebaseInfossComment)-[:COMMENTED_ON_FB]->(p:FirebaseInfoss)<-[:POSTED_FB]-(u2:FirebaseUser)
                WHERE u1.id <> u2.id
                RETURN u1.id AS source_id,
                       coalesce(u1.nama, u1.username, u1.id, 'Unknown') AS source_name,
                       u2.id AS target_id,
                       coalesce(u2.nama, u2.username, u2.id, 'Unknown') AS target_name,
                       3 AS w,
                       'COMMENT' AS t
            }
            WITH source_id, source_name, target_id, target_name, sum(w) AS total_weight, collect(DISTINCT t) AS rel_types
            ORDER BY total_weight DESC
            LIMIT $limit
            RETURN source_id, source_name, target_id, target_name, total_weight AS weight, rel_types
            """

            records = session.run(query, limit=limit).data()

            for record in records:
                source_id_raw = str(record.get("source_id", "")).strip()
                target_id_raw = str(record.get("target_id", "")).strip()
                source_name = str(record.get("source_name", "")).strip()
                target_name = str(record.get("target_name", "")).strip()

                if (
                    is_ignored_app_user(source_id_raw)
                    or is_ignored_app_user(target_id_raw)
                    or is_ignored_app_user(source_name)
                    or is_ignored_app_user(target_name)
                    or is_ignored_app_user(f"user_{source_id_raw}")
                    or is_ignored_app_user(f"user_{target_id_raw}")
                ):
                    continue

                source_node = f"user_{source_id_raw}"
                target_node = f"user_{target_id_raw}"

                if not G.has_node(source_node):
                    G.add_node(
                        source_node,
                        type="user",
                        name=source_name,
                        label=source_name
                    )

                if not G.has_node(target_node):
                    G.add_node(
                        target_node,
                        type="user",
                        name=target_name,
                        label=target_name
                    )

                G.add_edge(
                    source_node,
                    target_node,
                    relation=", ".join(record.get("rel_types", [])),
                    weight=record.get("weight", 1)
                )

        elif mode == 2:
            query_posts = """
            MATCH (u:FirebaseUser)-[:POSTED_FB]->(p)
            WHERE p:FirebaseKawanSS OR p:FirebaseInfoss
            RETURN u.id AS uid,
                   coalesce(u.nama, u.username, u.id, 'Unknown') AS uname,
                   p.id AS pid,
                   coalesce(p.deskripsi, p.judul, p.detail, p.title, '') AS text,
                   labels(p) AS p_labels,
                   coalesce(toInteger(p.jumlahLike), 0) AS likes
            LIMIT $limit
            """

            query_comments = """
            MATCH (u:FirebaseUser)-[:WROTE_FB]->(c)-[:COMMENTED_ON_FB]->(p)
            WHERE c:FirebaseKawanSSComment OR c:FirebaseInfossComment
            RETURN u.id AS uid,
                   coalesce(u.nama, u.username, u.id, 'Unknown') AS uname,
                   c.id AS cid,
                   coalesce(c.komentar, c.text, '') AS text,
                   labels(c) AS c_labels,
                   p.id AS target_id
            LIMIT $limit
            """

            query_likes = """
            MATCH (u:FirebaseUser)-[r]->(p)
            WHERE type(r) IN ['LIKES_KAWAN_FB', 'LIKES_INFO_FB']
            RETURN u.id AS uid,
                   coalesce(u.nama, u.username, u.id, 'Unknown') AS uname,
                   p.id AS target_id,
                   type(r) AS rel_type
            LIMIT $limit
            """

            posts_data = session.run(query_posts, limit=limit).data()
            comments_data = session.run(query_comments, limit=limit).data()
            likes_data = session.run(query_likes, limit=limit).data()

            for record in posts_data:
                uid = str(record.get("uid", "")).strip()
                uname = str(record.get("uname", "")).strip()

                if (
                    is_ignored_app_user(uid)
                    or is_ignored_app_user(uname)
                    or is_ignored_app_user(f"user_{uid}")
                ):
                    continue

                user_node = f"user_{uid}"
                post_node = f"post_{record.get('pid')}"
                text = record.get("text") or ""
                post_type = "post_infoss" if "FirebaseInfoss" in record.get("p_labels", []) else "post_kawanss"

                if not G.has_node(user_node):
                    G.add_node(
                        user_node,
                        type="user",
                        label=uname,
                        name=uname
                    )

                if not G.has_node(post_node):
                    G.add_node(
                        post_node,
                        type=post_type,
                        label=text[:20] + "..." if len(text) > 20 else text,
                        full_text=text,
                        likes=record.get("likes", 0)
                    )

                G.add_edge(
                    user_node,
                    post_node,
                    relation="AUTHORED",
                    weight=5
                )

                hashtags = set(HASHTAG_REGEX.findall(text.lower()))

                for tag in hashtags:
                    hashtag_node = f"tag_{tag}"

                    if not G.has_node(hashtag_node):
                        G.add_node(
                            hashtag_node,
                            type="hashtag",
                            label=f"#{tag}"
                        )

                    G.add_edge(
                        post_node,
                        hashtag_node,
                        relation="HAS_HASHTAG",
                        weight=2
                    )

            for record in comments_data:
                uid = str(record.get("uid", "")).strip()
                uname = str(record.get("uname", "")).strip()

                if (
                    is_ignored_app_user(uid)
                    or is_ignored_app_user(uname)
                    or is_ignored_app_user(f"user_{uid}")
                ):
                    continue

                user_node = f"user_{uid}"
                comment_node = f"comment_{record.get('cid')}"
                target_node = f"post_{record.get('target_id')}"
                text = record.get("text") or ""
                comment_type = "comment_infoss" if "FirebaseInfossComment" in record.get("c_labels", []) else "comment_kawanss"

                if not G.has_node(user_node):
                    G.add_node(
                        user_node,
                        type="user",
                        label=uname,
                        name=uname
                    )

                if not G.has_node(comment_node):
                    G.add_node(
                        comment_node,
                        type=comment_type,
                        label=text[:20] + "..." if len(text) > 20 else text,
                        full_text=text
                    )

                if G.has_node(target_node):
                    G.add_edge(
                        user_node,
                        comment_node,
                        relation="WROTE",
                        weight=3
                    )

                    G.add_edge(
                        comment_node,
                        target_node,
                        relation="COMMENTED_ON",
                        weight=3
                    )

                    hashtags = set(HASHTAG_REGEX.findall(text.lower()))

                    for tag in hashtags:
                        hashtag_node = f"tag_{tag}"

                        if not G.has_node(hashtag_node):
                            G.add_node(
                                hashtag_node,
                                type="hashtag",
                                label=f"#{tag}"
                            )

                        G.add_edge(
                            comment_node,
                            hashtag_node,
                            relation="HAS_HASHTAG",
                            weight=2
                        )

            for record in likes_data:
                uid = str(record.get("uid", "")).strip()
                uname = str(record.get("uname", "")).strip()

                if (
                    is_ignored_app_user(uid)
                    or is_ignored_app_user(uname)
                    or is_ignored_app_user(f"user_{uid}")
                ):
                    continue

                user_node = f"user_{uid}"
                target_node = f"post_{record.get('target_id')}"

                if G.has_node(user_node) and G.has_node(target_node):
                    G.add_edge(
                        user_node,
                        target_node,
                        relation="LIKED",
                        weight=1
                    )

        else:
            raise HTTPException(
                status_code=400,
                detail="mode harus 1 atau 2"
            )

    clean_graph_nodes(G, source="app")
    apply_leiden_communities(G, weight_attr="weight")

    return G

async def create_graph_visualization_from_neo4j(
    limit: int = 25000,
    mode: int = 2,
    max_edges: int = 25000
):
    try:
        G = nx.DiGraph()

        safe_max_edges = min(max_edges, 25000)

        with neo4j_driver.session() as session:
            if mode == 2:
                query = """
                MATCH (u:FirebaseUser)-[:POSTED_FB]->(p)
                WHERE p:FirebaseKawanSS OR p:FirebaseInfoss
                RETURN u.id AS uid,
                       coalesce(u.nama, u.username, u.id, '') AS uname,
                       p.id AS pid,
                       coalesce(p.deskripsi, p.judul, p.detail, p.title, '') AS text,
                       labels(p) AS p_labels,
                       coalesce(toInteger(p.jumlahLike), 0) AS likes
                LIMIT $max_edges
                """

                records = session.run(query, max_edges=safe_max_edges).data()

                for r in records:
                    uid = str(r.get("uid", "")).strip()
                    uname = str(r.get("uname", "")).strip()
                    pid = str(r.get("pid", "")).strip()

                    if (
                        not uid
                        or not pid
                        or is_ignored_app_user(uid)
                        or is_ignored_app_user(uname)
                        or is_ignored_app_user(f"user_{uid}")
                    ):
                        continue

                    user_node = f"user_{uid}"
                    post_node = f"post_{pid}"
                    text = r.get("text") or ""
                    labels = r.get("p_labels", [])

                    post_type = (
                        "post_infoss"
                        if "FirebaseInfoss" in labels
                        else "post_kawanss"
                    )

                    if not G.has_node(user_node):
                        G.add_node(
                            user_node,
                            type="user",
                            label=uname,
                            name=uname,
                        )

                    if not G.has_node(post_node):
                        G.add_node(
                            post_node,
                            type=post_type,
                            label=text[:20] + "..." if len(text) > 20 else text,
                            full_text=text[:300],
                            likes=r.get("likes", 0),
                        )

                    G.add_edge(
                        user_node,
                        post_node,
                        relation="AUTHORED",
                        weight=5,
                    )

            else:
                query = """
                CALL {
                    MATCH (u1:FirebaseUser)-[:LIKES_KAWAN_FB]->(p:FirebaseKawanSS)<-[:POSTED_FB]-(u2:FirebaseUser)
                    WHERE u1.id <> u2.id
                    RETURN u1.id AS source_id,
                           coalesce(u1.nama, u1.username, u1.id, '') AS source_name,
                           u2.id AS target_id,
                           coalesce(u2.nama, u2.username, u2.id, '') AS target_name,
                           1 AS weight,
                           'LIKE' AS relation

                    UNION ALL

                    MATCH (u1:FirebaseUser)-[:WROTE_FB]->(c:FirebaseKawanSSComment)-[:COMMENTED_ON_FB]->(p:FirebaseKawanSS)<-[:POSTED_FB]-(u2:FirebaseUser)
                    WHERE u1.id <> u2.id
                    RETURN u1.id AS source_id,
                           coalesce(u1.nama, u1.username, u1.id, '') AS source_name,
                           u2.id AS target_id,
                           coalesce(u2.nama, u2.username, u2.id, '') AS target_name,
                           3 AS weight,
                           'COMMENT' AS relation
                }
                LIMIT $max_edges
                """

                records = session.run(query, max_edges=safe_max_edges).data()

                for r in records:
                    source_id = str(r.get("source_id", "")).strip()
                    target_id = str(r.get("target_id", "")).strip()
                    source_name = str(r.get("source_name", "")).strip()
                    target_name = str(r.get("target_name", "")).strip()

                    if (
                        is_ignored_app_user(source_id)
                        or is_ignored_app_user(target_id)
                        or is_ignored_app_user(source_name)
                        or is_ignored_app_user(target_name)
                        or is_ignored_app_user(f"user_{source_id}")
                        or is_ignored_app_user(f"user_{target_id}")
                    ):
                        continue

                    source_node = f"user_{source_id}"
                    target_node = f"user_{target_id}"

                    if not G.has_node(source_node):
                        G.add_node(
                            source_node,
                            type="user",
                            name=source_name,
                            label=source_name,
                        )

                    if not G.has_node(target_node):
                        G.add_node(
                            target_node,
                            type="user",
                            name=target_name,
                            label=target_name,
                        )

                    G.add_edge(
                        source_node,
                        target_node,
                        relation=r.get("relation", "INTERACTION"),
                        weight=r.get("weight", 1),
                    )

        clean_graph_nodes(G, source="app")
        G.remove_nodes_from(list(nx.isolates(G)))

        if G.number_of_nodes() == 0:
            raise HTTPException(
                status_code=404,
                detail="Tidak ada relasi data yang ditemukan di Neo4j.",
            )

        community_map = apply_leiden_communities(G, weight_attr="weight")

        degree_map = dict(G.degree(weight="weight"))
        max_degree = max(degree_map.values()) if degree_map else 1

        nodes_output = []

        for node in G.nodes():
            attr = G.nodes[node].copy()
            community_id = community_map.get(node, attr.get("community", 0))
            attr["community"] = community_id

            degree = degree_map.get(node, 0)

            nodes_output.append({
                "id": node,
                "community": community_id,
                "attributes": attr,
                "metrics": {
                    "degree": degree / max_degree if max_degree else 0.0,
                    "raw_degree": degree,
                    "betweenness": 0.0,
                    "closeness": 0.0,
                    "eigenvector": 0.0,
                    "pagerank": 0.0,
                },
            })

        edges_output = []

        for source, target, data in G.edges(data=True):
            edges_output.append({
                "source": source,
                "target": target,
                "weight": data.get("weight", 1),
                "attributes": {
                    "relation": data.get("relation", "INTERACTION"),
                },
            })

        return {
            "message": (
                f"Graf visualisasi berhasil dibuat "
                f"(Nodes: {len(nodes_output)}, Edges: {len(edges_output)}, Mode: {mode})."
            ),
            "graph_info": {
                "nodes_count": len(nodes_output),
                "edges_count": len(edges_output),
                "communities_count": len(set(community_map.values())),
                "nodes": nodes_output,
                "edges": edges_output,
            },
        }

    except Exception as e:
        if isinstance(e, HTTPException):
            raise e

        import traceback
        traceback.print_exc()

        raise HTTPException(
            status_code=500,
            detail=f"Gagal membuat graf visualisasi Neo4j: {str(e)}",
        )
        
async def visualize_graph_from_neo4j(limit: int = 1000, mode: int = 1):
    try:
        from pyvis.network import Network

        G = _build_neo4j_graph_internal(limit, mode)

        if G.number_of_nodes() == 0:
            return HTMLResponse(
                "<h1>Graf Kosong</h1><p>Belum ada data relasi di Neo4j.</p>"
            )

        community_map = apply_leiden_communities(G, weight_attr="weight")
        degree_centrality = nx.degree_centrality(G)

        net = Network(
            height="100vh",
            width="100%",
            bgcolor="#1e1e1e",
            font_color="white",
            cdn_resources="in_line"
        )

        for node, data in G.nodes(data=True):
            community_id = community_map.get(node, data.get("community", 0))
            data["community"] = community_id

            score = degree_centrality.get(node, 0)
            size = 15 + (score * 60)

            node_type = data.get("type", "user")
            shape = "dot"
            color = None

            if node_type == "user":
                shape = "dot"
            elif node_type in ["post_kawanss", "post_infoss"]:
                shape = "square"
                color = "#FF5733" if node_type == "post_infoss" else "#33C1FF"
            elif node_type in ["comment_kawanss", "comment_infoss"]:
                shape = "triangle"
                color = "#FFC300"
            elif node_type == "hashtag":
                shape = "star"
                color = "#9C33FF"

            label = data.get("label") or data.get("name") or str(node)

            title_html = (
                f"<b>{str(node_type).upper()}:</b> {label}"
                f"<br><b>Node ID:</b> {node}"
                f"<br><b>Leiden Community:</b> {community_id}"
                f"<br><b>Degree Centrality:</b> {score:.4f}"
            )

            if "likes" in data:
                title_html += f"<br><b>Total Likes:</b> {data['likes']}"

            net.add_node(
                node,
                label=str(label)[:15],
                title=title_html,
                group=community_id,
                size=size,
                shape=shape,
                color=color
            )

        for source, target, data in G.edges(data=True):
            weight = data.get("weight", 1)
            relation = data.get("relation", "Interaction")

            net.add_edge(
                source,
                target,
                value=weight,
                title=(
                    f"Relasi: {relation}"
                    f"<br>Bobot Total: {weight}"
                )
            )

        net.toggle_physics(True)

        output_path = f"{OUTPUT_HTML_DIR}/snagraph_mode_{mode}.html"
        html_content = net.generate_html(output_path)

        with open(output_path, "w", encoding="utf-8") as file:
            file.write(html_content)

        return HTMLResponse(
            content=html_content,
            status_code=200
        )

    except Exception as e:
        import traceback
        traceback.print_exc()

        raise HTTPException(
            status_code=500,
            detail=f"Gagal memvisualisasikan graf dengan Leiden Algorithm: {str(e)}"
        )
