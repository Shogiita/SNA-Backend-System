import requests
import json
import time
import os
import pandas as pd
import networkx as nx
import concurrent.futures
import re
import calendar
import traceback

from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
from dateutil import parser
from pyvis.network import Network
from fastapi import HTTPException, BackgroundTasks
from fastapi.responses import HTMLResponse, Response
from collections import Counter, defaultdict
from apscheduler.schedulers.background import BackgroundScheduler

from app import config
from app.database import neo4j_driver
from app.utils.leiden_utils import apply_leiden_communities
from app.utils.sna_filter_utils import (
    normalize_hashtag,
    is_ignored_hashtag,
    is_ignored_instagram_user,
    clean_graph_nodes,
    calculate_centrality,
)


CACHE_FILE = "instagram_data_cache.json"
OUTPUT_HTML_DIR = "generated_graphs"

MAX_POSTS_TO_FETCH = 1000
FETCH_MONTHS_BACK = 12
MAX_WORKERS = 10

HASHTAG_REGEX = re.compile(r"#(\w+)")

os.makedirs(OUTPUT_HTML_DIR, exist_ok=True)

session = requests.Session()

async def create_instagram_graph_visualization_from_neo4j(
    limit: int = 5000,
    mode: int = 2,
    max_edges: int = 25000
):
    try:
        G = _build_neo4j_graph(
            mode=mode,
            limit=limit
        )

        clean_graph_nodes(G, source="instagram")

        if G.number_of_nodes() == 0:
            raise HTTPException(
                status_code=404,
                detail="Tidak ada relasi data Instagram yang ditemukan di Neo4j."
            )

        degree_map = dict(G.degree(weight="weight"))

        sorted_nodes = sorted(
            G.nodes(),
            key=lambda node: degree_map.get(node, 0),
            reverse=True
        )

        selected_nodes = set(sorted_nodes[:limit])

        H = G.subgraph(selected_nodes).copy()
        H.remove_nodes_from(list(nx.isolates(H)))

        degree_map = dict(H.degree(weight="weight"))

        sorted_edges = sorted(
            H.edges(data=True),
            key=lambda edge: edge[2].get("weight", 1),
            reverse=True
        )[:max_edges]

        edge_node_ids = set()

        for source, target, _ in sorted_edges:
            edge_node_ids.add(source)
            edge_node_ids.add(target)

        H = H.subgraph(edge_node_ids).copy()

        community_map = apply_leiden_communities(H, weight_attr="weight")

        degree_map = dict(H.degree(weight="weight"))
        max_degree = max(degree_map.values()) if degree_map else 1

        nodes_output = []

        for node in H.nodes():
            attr = H.nodes[node].copy()
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
                    "pagerank": 0.0
                }
            })

        valid_nodes = {node["id"] for node in nodes_output}

        edges_output = []

        for source, target, data in sorted_edges:
            if source not in valid_nodes or target not in valid_nodes:
                continue

            edges_output.append({
                "source": source,
                "target": target,
                "weight": data.get("weight", 1),
                "attributes": {
                    "relation": data.get("relation", "INTERACTION")
                }
            })

        return {
            "message": (
                f"Graf visualisasi Instagram berhasil dibuat "
                f"(Nodes: {len(nodes_output)}, Edges: {len(edges_output)}, Mode: {mode})."
            ),
            "graph_info": {
                "nodes_count": len(nodes_output),
                "edges_count": len(edges_output),
                "communities_count": len(set(community_map.values())),
                "nodes": nodes_output,
                "edges": edges_output
            }
        }

    except Exception as e:
        if isinstance(e, HTTPException):
            raise e

        import traceback
        traceback.print_exc()

        raise HTTPException(
            status_code=500,
            detail=f"Gagal membuat graf visualisasi Instagram Neo4j: {str(e)}"
        )

def _build_neo4j_graph(mode: int, limit: int = 1000):
    G = nx.DiGraph()

    with neo4j_driver.session() as session_db:
        if mode == 1:
            query = """
            CALL {
                MATCH (u1:InstagramUser)-[:WROTE_IG]->(c:InstagramComment)-[:COMMENTED_ON_IG]->(p:InstagramPost)<-[:POSTED_IG]-(u2:InstagramUser)
                WHERE u1.username <> u2.username
                  AND toLower(coalesce(u1.username, '')) <> 'suarasurabayamedia'
                  AND toLower(coalesce(u2.username, '')) <> 'suarasurabayamedia'
                RETURN u1.username AS s_id, u2.username AS t_id, 3 AS w, 'COMMENT' AS t

                UNION ALL

                MATCH (u1:InstagramUser)-[:WROTE_IG]->(r:InstagramComment)-[:REPLIED_TO_IG]->(c:InstagramComment)<-[:WROTE_IG]-(u2:InstagramUser)
                WHERE u1.username <> u2.username
                  AND toLower(coalesce(u1.username, '')) <> 'suarasurabayamedia'
                  AND toLower(coalesce(u2.username, '')) <> 'suarasurabayamedia'
                RETURN u1.username AS s_id, u2.username AS t_id, 4 AS w, 'REPLY' AS t
            }
            WITH s_id, t_id, sum(w) AS total_weight, collect(DISTINCT t) AS rel_types
            ORDER BY total_weight DESC
            LIMIT $limit
            RETURN s_id, t_id, total_weight AS weight, rel_types
            """

            records = session_db.run(query, limit=limit).data()

            for record in records:
                source_username = record.get("s_id")
                target_username = record.get("t_id")

                if is_ignored_instagram_user(source_username) or is_ignored_instagram_user(target_username):
                    continue

                source_node = f"user_{source_username}"
                target_node = f"user_{target_username}"

                if not G.has_node(source_node):
                    G.add_node(
                        source_node,
                        type="user",
                        label=source_username,
                    )

                if not G.has_node(target_node):
                    G.add_node(
                        target_node,
                        type="user",
                        label=target_username,
                    )

                relation = ", ".join(record.get("rel_types", []))
                weight = record.get("weight", 1)

                if G.has_edge(source_node, target_node):
                    G[source_node][target_node]["weight"] += weight
                else:
                    G.add_edge(
                        source_node,
                        target_node,
                        relation=relation,
                        weight=weight,
                    )

        elif mode == 2:
            query_posts = """
            MATCH (u:InstagramUser)-[:POSTED_IG]->(p:InstagramPost)
            RETURN u.username AS uid,
                   p.id AS pid,
                   coalesce(p.caption, '') AS text,
                   coalesce(p.like_count, 0) AS likes
            LIMIT $limit
            """

            query_comments = """
            MATCH (u:InstagramUser)-[:WROTE_IG]->(c:InstagramComment)-[:COMMENTED_ON_IG]->(p:InstagramPost)
            RETURN u.username AS uid,
                   c.id AS cid,
                   coalesce(c.text, '') AS text,
                   coalesce(c.likes, 0) AS likes,
                   p.id AS target_id
            LIMIT $limit
            """

            query_replies = """
            MATCH (u:InstagramUser)-[:WROTE_IG]->(r:InstagramComment)-[:REPLIED_TO_IG]->(c:InstagramComment)
            RETURN u.username AS uid,
                   r.id AS cid,
                   coalesce(r.text, '') AS text,
                   coalesce(r.likes, 0) AS likes,
                   c.id AS target_id
            LIMIT $limit
            """

            posts_data = session_db.run(query_posts, limit=limit).data()
            comments_data = session_db.run(query_comments, limit=limit).data()
            replies_data = session_db.run(query_replies, limit=limit).data()

            for record in posts_data:
                username = record.get("uid")
                post_id = f"post_{record.get('pid')}"
                text = record.get("text") or ""

                if not G.has_node(post_id):
                    G.add_node(
                        post_id,
                        type="post_ig",
                        label=text[:20] + "..." if len(text) > 20 else text,
                        full_text=text,
                        likes=record.get("likes", 0),
                    )

                if not is_ignored_instagram_user(username):
                    user_node = f"user_{username}"

                    if not G.has_node(user_node):
                        G.add_node(
                            user_node,
                            type="user",
                            label=username,
                        )

                    G.add_edge(
                        user_node,
                        post_id,
                        relation="POSTED_IG",
                        weight=5,
                    )

                raw_tags = HASHTAG_REGEX.findall(text)

                for raw_tag in raw_tags:
                    tag = normalize_hashtag(raw_tag)

                    if is_ignored_hashtag(tag):
                        continue

                    hashtag_node = f"tag_{tag}"

                    if not G.has_node(hashtag_node):
                        G.add_node(
                            hashtag_node,
                            type="hashtag",
                            label=f"#{tag}",
                        )

                    G.add_edge(
                        post_id,
                        hashtag_node,
                        relation="HAS_HASHTAG",
                        weight=2,
                    )

            for record in comments_data:
                username = record.get("uid")

                if is_ignored_instagram_user(username):
                    continue

                user_node = f"user_{username}"
                comment_node = f"comment_{record.get('cid')}"
                target_node = f"post_{record.get('target_id')}"
                text = record.get("text") or ""

                if not G.has_node(user_node):
                    G.add_node(
                        user_node,
                        type="user",
                        label=username,
                    )

                if not G.has_node(comment_node):
                    G.add_node(
                        comment_node,
                        type="comment_ig",
                        label=text[:20] + "..." if len(text) > 20 else text,
                        full_text=text,
                        likes=record.get("likes", 0),
                    )

                if G.has_node(target_node):
                    G.add_edge(
                        user_node,
                        comment_node,
                        relation="WROTE_IG",
                        weight=3,
                    )

                    G.add_edge(
                        comment_node,
                        target_node,
                        relation="COMMENTED_ON_IG",
                        weight=3,
                    )

                    raw_tags = HASHTAG_REGEX.findall(text)

                    for raw_tag in raw_tags:
                        tag = normalize_hashtag(raw_tag)

                        if is_ignored_hashtag(tag):
                            continue

                        hashtag_node = f"tag_{tag}"

                        if not G.has_node(hashtag_node):
                            G.add_node(
                                hashtag_node,
                                type="hashtag",
                                label=f"#{tag}",
                            )

                        G.add_edge(
                            comment_node,
                            hashtag_node,
                            relation="HAS_HASHTAG",
                            weight=2,
                        )

            for record in replies_data:
                username = record.get("uid")

                if is_ignored_instagram_user(username):
                    continue

                user_node = f"user_{username}"
                reply_node = f"reply_{record.get('cid')}"
                target_node = f"comment_{record.get('target_id')}"
                text = record.get("text") or ""

                if not G.has_node(user_node):
                    G.add_node(
                        user_node,
                        type="user",
                        label=username,
                    )

                if not G.has_node(reply_node):
                    G.add_node(
                        reply_node,
                        type="reply_ig",
                        label=text[:20] + "..." if len(text) > 20 else text,
                        full_text=text,
                        likes=record.get("likes", 0),
                    )

                if G.has_node(target_node):
                    G.add_edge(
                        user_node,
                        reply_node,
                        relation="WROTE_IG",
                        weight=4,
                    )

                    G.add_edge(
                        reply_node,
                        target_node,
                        relation="REPLIED_TO_IG",
                        weight=4,
                    )

                    raw_tags = HASHTAG_REGEX.findall(text)

                    for raw_tag in raw_tags:
                        tag = normalize_hashtag(raw_tag)

                        if is_ignored_hashtag(tag):
                            continue

                        hashtag_node = f"tag_{tag}"

                        if not G.has_node(hashtag_node):
                            G.add_node(
                                hashtag_node,
                                type="hashtag",
                                label=f"#{tag}",
                            )

                        G.add_edge(
                            reply_node,
                            hashtag_node,
                            relation="HAS_HASHTAG",
                            weight=2,
                        )

        else:
            raise HTTPException(
                status_code=400,
                detail="mode harus 1 atau 2",
            )

    clean_graph_nodes(G, source="instagram")
    apply_leiden_communities(G, weight_attr="weight")

    return G


async def analyze_instagram_graph_from_neo4j(limit: int = 1000, mode: int = 1):
    try:
        G = _build_neo4j_graph(
            mode=mode,
            limit=limit,
        )

        if G.number_of_nodes() == 0:
            raise HTTPException(
                status_code=404,
                detail="Tidak ada relasi data Instagram yang ditemukan di Neo4j.",
            )

        community_map = apply_leiden_communities(G, weight_attr="weight")
        centrality = calculate_centrality(G)

        nodes_output = []

        for node in G.nodes():
            attr = G.nodes[node].copy()
            community_id = community_map.get(node, attr.get("community", 0))
            attr["community"] = community_id

            nodes_output.append({
                "id": node,
                "community": community_id,
                "attributes": attr,
                "metrics": {
                    "degree": centrality["degree"].get(node, 0.0),
                    "in_degree": centrality["in_degree"].get(node, 0.0),
                    "out_degree": centrality["out_degree"].get(node, 0.0),
                    "betweenness": centrality["betweenness"].get(node, 0.0),
                    "closeness": centrality["closeness"].get(node, 0.0),
                    "eigenvector": centrality["eigenvector"].get(node, 0.0),
                    "pagerank": centrality["pagerank"].get(node, 0.0),
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
                    "distance": data.get("distance"),
                },
            })

        return {
            "message": f"Graf Instagram berhasil dibuat dari database Neo4j (Limit: {limit}, Mode: {mode}).",
            "graph_info": {
                "nodes_count": G.number_of_nodes(),
                "edges_count": G.number_of_edges(),
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
            detail=f"Gagal memproses graf Instagram Neo4j: {str(e)}",
        )

def _process_ig_to_neo4j_batch(posts_batch, comments_batch):
    post_query = """
    UNWIND $posts AS post
    MERGE (p:InstagramPost {id: post.id})
    SET p.caption = post.caption,
        p.permalink = post.permalink,
        p.media_type = post.media_type,
        p.like_count = post.like_count,
        p.comments_count = post.comments_count,
        p.share_count = post.share_count,
        p.view_count = post.view_count,
        p.timestamp = post.timestamp

    MERGE (u:InstagramUser {username: post.username})
    MERGE (u)-[:POSTED_IG]->(p)
    """

    comment_query = """
    UNWIND $comments AS comm
    MERGE (c:InstagramComment {id: comm.id})
    SET c.text = comm.text,
        c.likes = comm.likes,
        c.timestamp = comm.timestamp,
        c.type = comm.type,
        c.replies_count = comm.replies_count

    MERGE (u:InstagramUser {username: comm.username})
    MERGE (u)-[:WROTE_IG]->(c)

    WITH c, comm
    CALL {
        WITH c, comm
        WITH c, comm WHERE comm.type = 'COMMENT'
        MERGE (p:InstagramPost {id: comm.target_id})
        MERGE (c)-[:COMMENTED_ON_IG]->(p)
    }
    CALL {
        WITH c, comm
        WITH c, comm WHERE comm.type = 'REPLY'
        MERGE (parent:InstagramComment {id: comm.target_id})
        MERGE (c)-[:REPLIED_TO_IG]->(parent)
    }
    """

    try:
        with neo4j_driver.session() as session_db:
            if posts_batch:
                session_db.run(post_query, posts=posts_batch)

            if comments_batch:
                session_db.run(comment_query, comments=comments_batch)

    except Exception as e:
        print(f"[NEO4J ERROR] Gagal insert batch: {e}")


def sync_instagram_to_neo4j(is_initial_sync=False):
    print(f"🔄 Memulai Sinkronisasi IG ke Neo4j. Initial Sync: {is_initial_sync}")

    end_date = datetime.now(timezone.utc)
    two_months_ago = end_date - relativedelta(months=2)

    if is_initial_sync:
        start_date = two_months_ago
        max_posts = 5000
    else:
        start_date = end_date - relativedelta(days=2)
        max_posts = 100

    url = f"{config.GRAPH_API_URL}/{config.IG_BUSINESS_ACCOUNT_ID}/media"
    params = {
        "access_token": config.IG_ACCESS_TOKEN,
        "fields": "id,caption,media_type,media_url,permalink,timestamp,like_count,comments_count,username",
        "limit": 50,
    }

    posts_batch = []
    comments_batch = []
    batch_size = 100

    try:
        while url:
            response = session.get(url, params=params)
            data = response.json()

            if "error" in data or "data" not in data:
                break

            stop_fetching = False

            for post in data["data"]:
                post_time = parser.isoparse(post["timestamp"])

                if post_time < start_date:
                    stop_fetching = True
                    break

                posts_batch.append({
                    "id": post.get("id"),
                    "username": post.get("username", "suarasurabayamedia"),
                    "caption": post.get("caption", ""),
                    "permalink": post.get("permalink", ""),
                    "media_type": post.get("media_type", "UNKNOWN"),
                    "like_count": post.get("like_count", 0),
                    "comments_count": post.get("comments_count", 0),
                    "share_count": 0,
                    "view_count": 0,
                    "timestamp": post.get("timestamp"),
                })

                if post.get("comments_count", 0) > 0:
                    comment_url = f"{config.GRAPH_API_URL}/{post['id']}/comments"
                    comment_params = {
                        "access_token": config.IG_ACCESS_TOKEN,
                        "fields": "id,text,username,like_count,timestamp,replies{id,text,username,like_count,timestamp}",
                        "limit": 50,
                    }

                    try:
                        comment_response = session.get(comment_url, params=comment_params)
                        comment_data = comment_response.json().get("data", [])

                        for comment in comment_data:
                            replies_data = comment.get("replies", {}).get("data", [])

                            comments_batch.append({
                                "id": comment["id"],
                                "target_id": post["id"],
                                "type": "COMMENT",
                                "text": comment.get("text", ""),
                                "username": comment.get("username", "Unknown"),
                                "likes": comment.get("like_count", 0),
                                "replies_count": len(replies_data),
                                "timestamp": comment.get("timestamp"),
                            })

                            for reply in replies_data:
                                comments_batch.append({
                                    "id": reply["id"],
                                    "target_id": comment["id"],
                                    "type": "REPLY",
                                    "text": reply.get("text", ""),
                                    "username": reply.get("username", "Unknown"),
                                    "likes": reply.get("like_count", 0),
                                    "replies_count": 0,
                                    "timestamp": reply.get("timestamp"),
                                })

                    except Exception as comment_error:
                        print(f"Error fetching comments for {post['id']}: {comment_error}")

                if len(posts_batch) >= batch_size or len(comments_batch) >= batch_size * 5:
                    _process_ig_to_neo4j_batch(posts_batch, comments_batch)
                    posts_batch.clear()
                    comments_batch.clear()

            if stop_fetching:
                break

            url = data.get("paging", {}).get("next")
            params = {}

            if len(posts_batch) + len(comments_batch) >= max_posts:
                break

        if posts_batch or comments_batch:
            _process_ig_to_neo4j_batch(posts_batch, comments_batch)

        print("✅ Tarikan data IG ke Neo4j Selesai.")

        cutoff_iso_string = two_months_ago.strftime("%Y-%m-%dT%H:%M:%S+0000")
        print(f"🧹 Membersihkan data Instagram yang lebih lama dari {cutoff_iso_string}")

        cleanup_query = """
        MATCH (c:InstagramComment) WHERE c.timestamp < $cutoff
        DETACH DELETE c
        """

        cleanup_posts_query = """
        MATCH (p:InstagramPost) WHERE p.timestamp < $cutoff
        DETACH DELETE p
        """

        cleanup_users_query = """
        MATCH (u:InstagramUser) WHERE NOT (u)--()
        DELETE u
        """

        with neo4j_driver.session() as session_db:
            session_db.run(cleanup_query, cutoff=cutoff_iso_string)
            session_db.run(cleanup_posts_query, cutoff=cutoff_iso_string)
            session_db.run(cleanup_users_query)

        print("✅ Sinkronisasi dan Pembersihan Sliding Window Berhasil!")

    except Exception as e:
        print(f"❌ Sinkronisasi Gagal: {e}")
        traceback.print_exc()


def get_instagram_metrics(start_date: str = None, end_date: str = None):
    start_time = time.perf_counter()
    now = datetime.now(timezone.utc)

    if start_date:
        start_dt = parser.parse(start_date).replace(tzinfo=timezone.utc)
    else:
        start_dt = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    if end_date:
        end_dt = parser.parse(end_date).replace(hour=23, minute=59, second=59, tzinfo=timezone.utc)
    else:
        last_day_of_month = calendar.monthrange(now.year, now.month)[1]
        end_dt = now.replace(day=last_day_of_month, hour=23, minute=59, second=59, microsecond=0)

    str_start_iso = start_dt.strftime("%Y-%m-%dT%H:%M:%S+0000")
    str_end_iso = end_dt.strftime("%Y-%m-%dT%H:%M:%S+0000")

    try:
        with neo4j_driver.session() as session_db:
            query = """
            MATCH (p:InstagramPost)
            WHERE p.timestamp >= $start_iso AND p.timestamp <= $end_iso
            RETURN p.id AS id,
                   p.permalink AS permalink,
                   coalesce(p.caption, '') AS caption,
                   coalesce(toInteger(p.like_count), 0) AS like_count,
                   coalesce(toInteger(p.comments_count), 0) AS comments_count,
                   p.timestamp AS timestamp
            """

            records = session_db.run(
                query,
                start_iso=str_start_iso,
                end_iso=str_end_iso,
            ).data()

    except Exception as e:
        return {
            "status": "error",
            "message": f"Database Error: {str(e)}",
        }

    if not records:
        return {
            "status": "success",
            "message": "Tidak ada postingan di rentang waktu tersebut.",
            "data": {
                "top_10_posts": [],
                "top_10_hashtags": [],
            },
        }

    hashtag_counts = Counter()
    hashtag_to_posts = defaultdict(list)

    for post in records:
        caption = post.get("caption") or ""

        if not caption:
            continue

        raw_tags = HASHTAG_REGEX.findall(caption)

        clean_caption = caption.replace("\n", " ").replace("\r", " ").strip()
        short_caption = clean_caption[:100] + "..." if len(clean_caption) > 100 else clean_caption

        like_count = int(post.get("like_count", 0))
        comments_count = int(post.get("comments_count", 0))
        total_engagement = like_count + comments_count

        post_info = {
            "id": post.get("id"),
            "permalink": post.get("permalink"),
            "caption": short_caption,
            "like_count": like_count,
            "comments_count": comments_count,
            "total_engagement": total_engagement,
            "timestamp": post.get("timestamp"),
        }

        unique_tags = set()

        for raw_tag in raw_tags:
            tag = normalize_hashtag(raw_tag)

            if is_ignored_hashtag(tag):
                continue

            unique_tags.add(tag)

        for tag in unique_tags:
            hashtag_counts[tag] += 1
            hashtag_to_posts[tag].append(post_info)

    top_10_hashtags = []

    for tag, count in hashtag_counts.most_common(10):
        sorted_posts = sorted(
            hashtag_to_posts[tag],
            key=lambda item: (
                item["total_engagement"],
                item["like_count"],
                item["comments_count"],
            ),
            reverse=True,
        )[:3]

        top_10_hashtags.append({
            "hashtag": f"#{tag}",
            "count": count,
            "top_posts": sorted_posts,
        })

    records.sort(
        key=lambda item: (
            int(item.get("like_count", 0)) + int(item.get("comments_count", 0)),
            int(item.get("like_count", 0)),
        ),
        reverse=True,
    )

    top_10_posts = []

    for post in records[:10]:
        caption = (post.get("caption") or "").replace("\n", " ").replace("\r", " ").strip()
        short_caption = caption[:100] + "..." if len(caption) > 100 else caption

        like_count = int(post.get("like_count", 0))
        comments_count = int(post.get("comments_count", 0))

        top_10_posts.append({
            "id": post.get("id"),
            "permalink": post.get("permalink"),
            "caption": short_caption,
            "like_count": like_count,
            "comments_count": comments_count,
            "total_engagement": like_count + comments_count,
            "timestamp": post.get("timestamp"),
        })

    process_time = round(time.perf_counter() - start_time, 4)

    return {
        "status": "success",
        "message": f"Data {len(records)} postingan berhasil dianalisis dalam {process_time} detik.",
        "data": {
            "top_10_posts": top_10_posts,
            "top_10_hashtags": top_10_hashtags,
        },
    }


def _background_sync_ig_to_neo4j():
    print("[IG SYNC] Memulai penarikan 1000 post...")

    max_posts = 1000
    all_posts = []

    url = f"{config.GRAPH_API_URL}/{config.IG_BUSINESS_ACCOUNT_ID}/media"
    params = {
        "access_token": config.IG_ACCESS_TOKEN,
        "fields": "id,caption,permalink,timestamp,like_count,comments_count",
        "limit": 50,
    }

    while url and len(all_posts) < max_posts:
        try:
            response = session.get(url, params=params)
            data = response.json()

            if "error" in data:
                break

            if "data" in data:
                all_posts.extend(data["data"])

            if "paging" in data and "next" in data["paging"]:
                url = data["paging"]["next"]
                params = {}
            else:
                break

        except Exception as e:
            print(f"[IG SYNC ERROR] Gagal mengambil post: {str(e)}")
            break

    all_posts = all_posts[:max_posts]

    if not all_posts:
        print("[IG SYNC] Tidak ada post yang berhasil diambil.")
        return

    sorted_posts = sorted(
        all_posts,
        key=lambda item: int(item.get("like_count", 0)) + int(item.get("comments_count", 0)),
        reverse=True,
    )

    top_10_posts = []

    for post in sorted_posts[:10]:
        clean_caption = (post.get("caption") or "").replace("\n", " ").replace("\r", " ").strip()
        like_count = int(post.get("like_count", 0))
        comments_count = int(post.get("comments_count", 0))

        top_10_posts.append({
            "id": post.get("id"),
            "permalink": post.get("permalink", ""),
            "caption": clean_caption[:100] + "..." if len(clean_caption) > 100 else clean_caption,
            "like_count": like_count,
            "comments_count": comments_count,
            "total_engagement": like_count + comments_count,
            "timestamp": post.get("timestamp", ""),
        })

    hashtag_counts = Counter()
    hashtag_to_posts = defaultdict(list)

    for post in all_posts:
        caption = post.get("caption") or ""

        if not caption:
            continue

        raw_tags = HASHTAG_REGEX.findall(caption)
        clean_caption = caption.replace("\n", " ").replace("\r", " ").strip()

        like_count = int(post.get("like_count", 0))
        comments_count = int(post.get("comments_count", 0))
        total_engagement = like_count + comments_count

        post_info = {
            "id": post.get("id"),
            "permalink": post.get("permalink", ""),
            "caption": clean_caption[:100] + "..." if len(clean_caption) > 100 else clean_caption,
            "like_count": like_count,
            "comments_count": comments_count,
            "total_engagement": total_engagement,
            "timestamp": post.get("timestamp", ""),
        }

        unique_tags = set()

        for raw_tag in raw_tags:
            tag = normalize_hashtag(raw_tag)

            if is_ignored_hashtag(tag):
                continue

            unique_tags.add(tag)

        for tag in unique_tags:
            hashtag_counts[tag] += 1
            hashtag_to_posts[tag].append(post_info)

    top_10_hashtags = []

    for tag, count in hashtag_counts.most_common(10):
        sorted_posts_for_tag = sorted(
            hashtag_to_posts[tag],
            key=lambda item: (
                item["total_engagement"],
                item["like_count"],
                item["comments_count"],
            ),
            reverse=True,
        )

        top_10_hashtags.append({
            "hashtag": f"#{tag}",
            "count": count,
            "top_posts": sorted_posts_for_tag[:3],
        })

    save_query = """
    MERGE (n:InstagramMetrics {id: 'latest_metrics'})
    SET n.top_posts = $top_posts,
        n.top_hashtags = $top_hashtags,
        n.last_updated = $last_updated
    """

    try:
        with neo4j_driver.session() as session_db:
            session_db.run(
                save_query,
                top_posts=json.dumps(top_10_posts),
                top_hashtags=json.dumps(top_10_hashtags),
                last_updated=time.time(),
            )

        print("[IG SYNC] SUKSES memperbarui Neo4j.")

    except Exception as e:
        print(f"[IG SYNC ERROR] {str(e)}")


def _get_posts_recursive(start_date, end_date, max_posts=MAX_POSTS_TO_FETCH):
    all_posts = []

    url = f"{config.GRAPH_API_URL}/{config.IG_BUSINESS_ACCOUNT_ID}/media"
    params = {
        "access_token": config.IG_ACCESS_TOKEN,
        "fields": "id,caption,media_type,media_url,permalink,timestamp,like_count,comments_count",
        "limit": 50,
    }

    while url and len(all_posts) < max_posts:
        try:
            response = session.get(url, params=params)
            data = response.json()

            if "error" in data or "data" not in data:
                break

            stop_fetching = False

            for post in data["data"]:
                post_time = parser.isoparse(post["timestamp"])

                if post_time > end_date:
                    continue

                if post_time < start_date:
                    stop_fetching = True
                    break

                all_posts.append(post)

                if len(all_posts) >= max_posts:
                    stop_fetching = True
                    break

            if stop_fetching:
                break

            if "paging" in data and "next" in data["paging"]:
                url = data["paging"]["next"]
                params = {}
            else:
                break

        except Exception:
            break

    return all_posts


def _fetch_comments_and_replies(post):
    post_id = post["id"]

    post_item = {
        "id": post_id,
        "caption": post.get("caption", ""),
        "media_url": post.get("media_url", ""),
        "permalink": post.get("permalink", ""),
        "like_count": post.get("like_count", 0),
        "comments_count": post.get("comments_count", 0),
        "timestamp": post.get("timestamp"),
        "interactions": [],
    }

    if post_item["comments_count"] == 0:
        return post_item

    interactions = []

    url = f"{config.GRAPH_API_URL}/{post_id}/comments"
    params = {
        "access_token": config.IG_ACCESS_TOKEN,
        "fields": "id,text,username,like_count,timestamp,replies{id,text,username,like_count,timestamp}",
        "limit": 50,
    }

    try:
        response = session.get(url, params=params)
        data = response.json()

        for comment in data.get("data", []):
            comment_username = comment.get("username", "Unknown")

            interactions.append({
                "interaction_type": "COMMENT",
                "source_username": comment_username,
                "target_id": post_id,
                "target_type": "POST",
                "content": comment.get("text", ""),
                "likes": comment.get("like_count", 0),
                "timestamp": comment.get("timestamp"),
            })

            if "replies" in comment:
                for reply in comment["replies"].get("data", []):
                    interactions.append({
                        "interaction_type": "REPLY",
                        "source_username": reply.get("username", "Unknown"),
                        "target_id": comment_username,
                        "target_type": "USER",
                        "content": reply.get("text", ""),
                        "likes": reply.get("like_count", 0),
                        "timestamp": reply.get("timestamp"),
                    })

    except Exception as e:
        print(f"Error fetching comments for {post_id}: {e}")

    post_item["interactions"] = interactions
    return post_item


def background_ingestion_task():
    try:
        print("[INGESTION] Memulai proses penarikan data dari Instagram...")

        end_date = datetime.now(timezone.utc)
        start_date = end_date - relativedelta(months=FETCH_MONTHS_BACK)

        raw_posts = _get_posts_recursive(
            start_date,
            end_date,
            max_posts=MAX_POSTS_TO_FETCH,
        )

        full_dataset = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(_fetch_comments_and_replies, post): post
                for post in raw_posts
            }

            for future in concurrent.futures.as_completed(futures):
                try:
                    full_dataset.append(future.result())
                except Exception as e:
                    print(f"[INGESTION ERROR] {e}")

        with open(CACHE_FILE, "w", encoding="utf-8") as file:
            json.dump(full_dataset, file, ensure_ascii=False, indent=2)

        print(f"[INGESTION] Selesai. Tersimpan {len(full_dataset)} posts ke cache.")

    except Exception as e:
        print(f"[FATAL ERROR] Ingestion gagal: {e}")


def get_dataset_flat():
    if not os.path.exists(CACHE_FILE):
        raise HTTPException(
            status_code=404,
            detail="Cache belum ada.",
        )

    with open(CACHE_FILE, "r", encoding="utf-8") as file:
        posts_data = json.load(file)

    dataset = []

    for post in posts_data:
        post_id = post["id"]
        post_likes = post.get("like_count", 0)
        post_comments_count = post.get("comments_count", 0)

        clean_caption = (
            post.get("caption", "")
            .replace("\n", " ")
            .replace("\r", " ")
            .replace('"', "'")
        )

        dataset.append({
            "Source": "suarasurabayamedia",
            "Target": post_id,
            "Interaction_Type": "POST",
            "Post_Like_Count": post_likes,
            "Post_Comment_Count": post_comments_count,
            "Interaction_Like_Count": 0,
            "Content": clean_caption,
        })

        for interaction in post.get("interactions", []):
            clean_content = (
                interaction.get("content", "")
                .replace("\n", " ")
                .replace("\r", " ")
                .replace('"', "'")
            )

            dataset.append({
                "Source": interaction.get("source_username", "Unknown"),
                "Target": interaction.get("target_id"),
                "Interaction_Type": interaction.get("interaction_type"),
                "Post_Like_Count": post_likes,
                "Post_Comment_Count": post_comments_count,
                "Interaction_Like_Count": interaction.get("likes", 0),
                "Content": clean_content,
            })

    return dataset


def export_dataset_csv():
    dataset = get_dataset_flat()

    if not dataset:
        raise HTTPException(
            status_code=404,
            detail="Dataset kosong.",
        )

    df = pd.DataFrame(dataset)

    csv_content = df.to_csv(index=False, encoding="utf-8-sig")

    return Response(
        content=csv_content,
        media_type="text/csv",
        headers={
            "Content-Disposition": "attachment; filename=instagram_sna_dataset.csv"
        },
    )


def start_instagram_ingestion(background_tasks: BackgroundTasks):
    background_tasks.add_task(background_ingestion_task)

    return {
        "status": "success",
        "message": "Proses ingestion Instagram sedang berjalan di background.",
    }


def start_instagram_sync_to_neo4j(background_tasks: BackgroundTasks):
    background_tasks.add_task(sync_instagram_to_neo4j, True)

    return {
        "status": "success",
        "message": "Proses sinkronisasi Instagram ke Neo4j sedang berjalan di background.",
    }


def start_metrics_sync(background_tasks: BackgroundTasks):
    background_tasks.add_task(_background_sync_ig_to_neo4j)

    return {
        "status": "success",
        "message": "Proses sinkronisasi metrics Instagram sedang berjalan di background.",
    }


def visualize_instagram_graph_from_neo4j(limit: int = 1000, mode: int = 1):
    try:
        G = _build_neo4j_graph(
            mode=mode,
            limit=limit,
        )

        if G.number_of_nodes() == 0:
            return HTMLResponse(
                "<h1>Graf Kosong</h1><p>Belum ada data relasi Instagram di Neo4j.</p>"
            )

        community_map = apply_leiden_communities(G, weight_attr="weight")
        degree_centrality = nx.degree_centrality(G)

        net = Network(
            height="100vh",
            width="100%",
            bgcolor="#1e1e1e",
            font_color="white",
            cdn_resources="in_line",
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
            elif node_type == "post_ig":
                shape = "square"
                color = "#33C1FF"
            elif node_type == "comment_ig":
                shape = "triangle"
                color = "#FFC300"
            elif node_type == "reply_ig":
                shape = "triangle"
                color = "#FFAA00"
            elif node_type == "hashtag":
                shape = "star"
                color = "#9C33FF"

            label = data.get("label") or str(node)

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
                color=color,
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

        output_path = f"{OUTPUT_HTML_DIR}/instagram_graph_mode_{mode}.html"
        html_content = net.generate_html(output_path)

        with open(output_path, "w", encoding="utf-8") as file:
            file.write(html_content)

        return HTMLResponse(
            content=html_content,
            status_code=200,
        )

    except Exception as e:
        traceback.print_exc()

        raise HTTPException(
            status_code=500,
            detail=f"Gagal memvisualisasikan graf Instagram dengan Leiden Algorithm: {str(e)}",
        )

_scheduler = BackgroundScheduler()


def start_scheduler():
    if not _scheduler.running:
        _scheduler.add_job(
            sync_instagram_to_neo4j,
            "interval",
            hours=1,
            args=[False],
            id="instagram_sync_job",
            replace_existing=True,
        )

        _scheduler.add_job(
            _background_sync_ig_to_neo4j,
            "interval",
            hours=1,
            id="instagram_metrics_sync_job",
            replace_existing=True,
        )

        _scheduler.start()

    return {
        "status": "success",
        "message": "Scheduler Instagram berhasil dijalankan.",
    }


def stop_scheduler():
    if _scheduler.running:
        _scheduler.shutdown(wait=False)

    return {
        "status": "success",
        "message": "Scheduler Instagram berhasil dihentikan.",
    }