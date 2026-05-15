import os
import json
import csv
import io
import re
import asyncio
import itertools
import random
import time
import calendar
import traceback
from dateutil import parser
from datetime import datetime, timezone, timedelta
import networkx as nx
from networkx.algorithms import bipartite
from fastapi import HTTPException, Response
from app.database import neo4j_driver
from collections import Counter
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
    RunRealtimeReportRequest,
    FilterExpression,
    Filter,
    OrderBy,
)
from google.oauth2 import service_account
from app import config
from app.utils.sna_filter_utils import is_ignored_app_user, is_ignored_instagram_user, normalize_hashtag, is_ignored_hashtag
from google.analytics.data_v1beta.types import (
    FilterExpression,
    Filter,
)


def _safe_percentage(current_value: int, previous_value: int) -> float:
    """
    Growth user untuk dashboard.

    Dipakai untuk menunjukkan persentase pertambahan user baru.
    Jika bulan ini tidak ada user baru, return 0.0 agar dashboard tidak menampilkan -100%.
    """

    current_value = _safe_int(current_value)
    previous_value = _safe_int(previous_value)

    if current_value <= 0:
        return 0.0

    if previous_value <= 0:
        return 100.0

    return round(((current_value - previous_value) / previous_value) * 100, 2)


def _safe_int(value, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except Exception:
        return default

def get_ga_credentials():
    return service_account.Credentials.from_service_account_info({
        "type": os.getenv("GCP_TYPE"),
        "project_id": os.getenv("GCP_PROJECT_ID"),
        "private_key_id": os.getenv("GCP_PRIVATE_KEY_ID"),
        "private_key": os.getenv("GCP_PRIVATE_KEY", "").replace('\\n', '\n'),
        "client_email": os.getenv("GCP_CLIENT_EMAIL"),
        "client_id": os.getenv("GCP_CLIENT_ID"),
        "auth_uri": os.getenv("GCP_AUTH_URI"),
        "token_uri": os.getenv("GCP_TOKEN_URI"),
        "auth_provider_x509_cert_url": os.getenv("GCP_AUTH_PROVIDER_CERT_URL"),
        "client_x509_cert_url": os.getenv("GCP_CLIENT_CERT_URL")
    })

def _parse_ga_metric_value(value):
    try:
        if value is None:
            return 0

        value = str(value)

        if "." in value:
            return round(float(value), 2)

        return int(value)
    except Exception:
        return value


def _format_ga_date(value: str) -> str:
    try:
        if not value or len(value) != 8:
            return value

        return f"{value[0:4]}-{value[4:6]}-{value[6:8]}"
    except Exception:
        return value


def _run_ga_report(
    dimensions: list[str],
    metrics: list[str],
    start_date: str,
    end_date: str,
    limit: int = 10,
    order_by_metric: str | None = None,
):
    property_id = os.getenv("GA_PROPERTY_ID")

    if not property_id:
        raise ValueError("GA_PROPERTY_ID tidak ditemukan di environment variables.")

    credentials = get_ga_credentials()
    client = BetaAnalyticsDataClient(credentials=credentials)

    request_data = {
        "property": f"properties/{property_id}",
        "date_ranges": [
            DateRange(
                start_date=start_date,
                end_date=end_date,
            )
        ],
        "metrics": [
            Metric(name=metric_name)
            for metric_name in metrics
        ],
        "limit": limit,
    }

    if dimensions:
        request_data["dimensions"] = [
            Dimension(name=dimension_name)
            for dimension_name in dimensions
        ]

    if order_by_metric:
        request_data["order_bys"] = [
            OrderBy(
                metric=OrderBy.MetricOrderBy(
                    metric_name=order_by_metric,
                ),
                desc=True,
            )
        ]

    response = client.run_report(
        RunReportRequest(**request_data)
    )

    rows = []

    for row in response.rows:
        item = {}

        for index, dimension_name in enumerate(dimensions):
            value = row.dimension_values[index].value

            if dimension_name == "date":
                value = _format_ga_date(value)

            item[dimension_name] = value

        for index, metric_name in enumerate(metrics):
            value = row.metric_values[index].value
            item[metric_name] = _parse_ga_metric_value(value)

        rows.append(item)

    return rows
def _format_ga_date(value: str) -> str:
    try:
        if not value or len(value) != 8:
            return value

        return f"{value[0:4]}-{value[4:6]}-{value[6:8]}"
    except Exception:
        return value


def _run_ga_report(
    dimensions: list[str],
    metrics: list[str],
    start_date: str,
    end_date: str,
    limit: int = 10,
    order_by_metric: str | None = None,
):
    property_id = os.getenv("GA_PROPERTY_ID")

    if not property_id:
        raise ValueError("GA_PROPERTY_ID tidak ditemukan di environment variables.")

    credentials = get_ga_credentials()
    client = BetaAnalyticsDataClient(credentials=credentials)

    request_data = {
        "property": f"properties/{property_id}",
        "date_ranges": [
            DateRange(
                start_date=start_date,
                end_date=end_date,
            )
        ],
        "metrics": [
            Metric(name=metric_name)
            for metric_name in metrics
        ],
        "limit": limit,
    }

    if dimensions:
        request_data["dimensions"] = [
            Dimension(name=dimension_name)
            for dimension_name in dimensions
        ]

    if order_by_metric:
        request_data["order_bys"] = [
            OrderBy(
                metric=OrderBy.MetricOrderBy(
                    metric_name=order_by_metric,
                ),
                desc=True,
            )
        ]

    response = client.run_report(
        RunReportRequest(**request_data)
    )

    rows = []

    for row in response.rows:
        item = {}

        for index, dimension_name in enumerate(dimensions):
            value = row.dimension_values[index].value

            if dimension_name == "date":
                value = _format_ga_date(value)

            item[dimension_name] = value

        for index, metric_name in enumerate(metrics):
            value = row.metric_values[index].value
            item[metric_name] = _parse_ga_metric_value(value)

        rows.append(item)

    return rows

def _run_ga_report(
    dimensions: list[str],
    metrics: list[str],
    start_date: str,
    end_date: str,
    limit: int = 10,
    order_by_metric: str | None = None,
):
    property_id = os.getenv("GA_PROPERTY_ID")

    if not property_id:
        raise ValueError("GA_PROPERTY_ID tidak ditemukan di environment variables.")

    credentials = get_ga_credentials()
    client = BetaAnalyticsDataClient(credentials=credentials)

    request_kwargs = {
        "property": f"properties/{property_id}",
        "date_ranges": [
            DateRange(
                start_date=start_date,
                end_date=end_date,
            )
        ],
        "metrics": [Metric(name=name) for name in metrics],
        "limit": limit,
    }

    if dimensions:
        request_kwargs["dimensions"] = [
            Dimension(name=name) for name in dimensions
        ]

    if order_by_metric:
        request_kwargs["order_bys"] = [
            OrderBy(
                metric=OrderBy.MetricOrderBy(metric_name=order_by_metric),
                desc=True,
            )
        ]

    response = client.run_report(RunReportRequest(**request_kwargs))

    rows = []

    for row in response.rows:
        item = {}

        for index, dimension in enumerate(dimensions):
            item[dimension] = row.dimension_values[index].value

        for index, metric in enumerate(metrics):
            raw_value = row.metric_values[index].value

            try:
                if "." in raw_value:
                    item[metric] = round(float(raw_value), 2)
                else:
                    item[metric] = int(raw_value)
            except Exception:
                item[metric] = raw_value

        rows.append(item)

    return rows

def get_first_day_of_last_month(dt):
    first_day_of_current_month = dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    last_day_of_last_month = first_day_of_current_month - timedelta(days=1)
    return last_day_of_last_month.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

def get_top_content_summary(source: str = "app", start_date: str = None, end_date: str = None):
    try:
        now = datetime.now()

        # 1. LOGIKA FILTER TANGGAL
        if start_date:
            start_dt = parser.parse(start_date).replace(
                hour=0,
                minute=0,
                second=0,
                microsecond=0
            )
        else:
            start_dt = now.replace(
                day=1,
                hour=0,
                minute=0,
                second=0,
                microsecond=0
            )

        if end_date:
            end_dt = parser.parse(end_date).replace(
                hour=23,
                minute=59,
                second=59,
                microsecond=0
            )
        else:
            last_day_of_month = calendar.monthrange(now.year, now.month)[1]
            end_dt = now.replace(
                day=last_day_of_month,
                hour=23,
                minute=59,
                second=59,
                microsecond=0
            )

        # 2. FORMATTING TANGGAL UNTUK DATABASE
        ig_iso_start = start_dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+0000")
        ig_iso_end = end_dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+0000")

        iso_start = start_dt.isoformat()
        iso_end = end_dt.isoformat()
        epoch_start = int(start_dt.timestamp() * 1000)
        epoch_end = int(end_dt.timestamp() * 1000)

        # 3. QUERY CYPHER BERDASARKAN SUMBER DATA
        if source == "instagram":
            top_query = """
            MATCH (i:InstagramPost)
            WHERE i.timestamp >= $start_iso AND i.timestamp <= $end_iso
            RETURN i.id AS id,
                   coalesce(substring(i.caption, 0, 150), 'No Caption') AS judul,
                   coalesce(i.media_product_type, i.media_type, 'Instagram') AS kategori,
                   coalesce(i.permalink, '') AS permalink,
                   i.timestamp AS uploadDate,
                   coalesce(toInteger(i.comments_count), 0) AS jumlahComment,
                   coalesce(toInteger(i.like_count), 0) AS jumlahLike
            ORDER BY (jumlahLike + jumlahComment) DESC
            LIMIT 10
            """

            ht_query = """
            MATCH (p:InstagramPost)
            WHERE p.caption IS NOT NULL
              AND p.caption CONTAINS '#'
              AND p.timestamp >= $start_iso
              AND p.timestamp <= $end_iso
            RETURN p.id AS id,
                   p.permalink AS permalink,
                   p.caption AS text,
                   coalesce(toInteger(p.like_count), 0) AS likes,
                   coalesce(toInteger(p.comments_count), 0) AS comments,
                   p.timestamp AS timestamp
            LIMIT 5000
            """

        else:
            top_query = """
            MATCH (i:FirebaseInfoss)
            WHERE (i.isDeleted = false OR i.isDeleted IS NULL)
              AND (i.uploadDate >= $iso_start OR i.createdAt >= $iso_start)
              AND (i.uploadDate <= $iso_end OR i.createdAt <= $iso_end)
            RETURN i.id AS id,
                   coalesce(i.judul, i.title, 'No Title') AS judul,
                   coalesce(toInteger(i.jumlahView), 0) AS jumlahView,
                   coalesce(i.kategori, 'Umum') AS kategori,
                   coalesce(i.gambar, '') AS gambar,
                   coalesce(i.uploadDate, i.createdAt) AS uploadDate,
                   coalesce(toInteger(i.jumlahComment), 0) AS jumlahComment,
                   coalesce(toInteger(i.jumlahLike), 0) AS jumlahLike
            ORDER BY jumlahView DESC
            LIMIT 10
            """

            ht_query = """
            MATCH (p:FirebaseKawanSS)
            WHERE (p.isDeleted = false OR p.isDeleted IS NULL)
              AND p.deskripsi IS NOT NULL
              AND p.deskripsi CONTAINS '#'
              AND p.createdAt >= $epoch_start
              AND p.createdAt <= $epoch_end
            RETURN p.id AS id,
                   '' AS permalink,
                   p.deskripsi AS text,
                   coalesce(toInteger(p.jumlahLike), 0) AS likes,
                   coalesce(toInteger(p.jumlahComment), 0) AS comments,
                   toString(p.createdAt) AS timestamp
            LIMIT 5000

            UNION ALL

            MATCH (p:FirebaseInfoss)
            WHERE (p.isDeleted = false OR p.isDeleted IS NULL)
              AND coalesce(p.detail, p.judul, '') CONTAINS '#'
              AND (p.uploadDate >= $iso_start OR p.createdAt >= $iso_start)
              AND (p.uploadDate <= $iso_end OR p.createdAt <= $iso_end)
            RETURN p.id AS id,
                   '' AS permalink,
                   coalesce(p.detail, p.judul, '') AS text,
                   coalesce(toInteger(p.jumlahLike), 0) AS likes,
                   coalesce(toInteger(p.jumlahComment), 0) AS comments,
                   coalesce(p.uploadDate, p.createdAt) AS timestamp
            LIMIT 5000
            """

        # 4. EKSEKUSI QUERY
        with neo4j_driver.session() as session:
            if source == "instagram":
                top_content = session.run(
                    top_query,
                    start_iso=ig_iso_start,
                    end_iso=ig_iso_end
                ).data()

                ht_records = session.run(
                    ht_query,
                    start_iso=ig_iso_start,
                    end_iso=ig_iso_end
                ).data()

            else:
                top_content = session.run(
                    top_query,
                    iso_start=iso_start,
                    iso_end=iso_end
                ).data()

                ht_records = session.run(
                    ht_query,
                    iso_start=iso_start,
                    iso_end=iso_end,
                    epoch_start=epoch_start,
                    epoch_end=epoch_end
                ).data()

        # 5. PEMROSESAN HASHTAG MENGGUNAKAN sna_filter_utils
        hashtag_pattern = re.compile(r"#(\w+)")
        hashtag_counts = Counter()
        hashtag_posts_map = {}

        for record in ht_records:
            text = record.get("text", "")

            if not text:
                continue

            raw_hashtags = hashtag_pattern.findall(str(text))
            unique_hashtags = set()

            for raw_tag in raw_hashtags:
                tag = normalize_hashtag(raw_tag)

                if is_ignored_hashtag(tag):
                    continue

                unique_hashtags.add(tag)

            if not unique_hashtags:
                continue

            like_count = int(record.get("likes", 0))
            comments_count = int(record.get("comments", 0))
            total_engagement = like_count + comments_count

            clean_text = (
                str(text)
                .replace("\n", " ")
                .replace("\r", " ")
                .strip()
            )

            post_obj = {
                "id": str(record.get("id", "")),
                "permalink": str(record.get("permalink", "")),
                "caption": clean_text[:150] + ("..." if len(clean_text) > 150 else ""),
                "like_count": like_count,
                "comments_count": comments_count,
                "total_engagement": total_engagement,
                "timestamp": str(record.get("timestamp", ""))
            }

            for tag in unique_hashtags:
                hashtag_counts[tag] += 1

                if tag not in hashtag_posts_map:
                    hashtag_posts_map[tag] = []

                hashtag_posts_map[tag].append(post_obj)

        top_10_hashtags = []

        for tag, count in hashtag_counts.most_common(10):
            sorted_posts = sorted(
                hashtag_posts_map[tag],
                key=lambda item: (
                    item["total_engagement"],
                    item["like_count"],
                    item["comments_count"]
                ),
                reverse=True
            )

            top_10_hashtags.append({
                "hashtag": f"#{tag}",
                "count": count,
                "top_posts": sorted_posts[:3]
            })

        date_range_info = {
            "start": start_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "end": end_dt.strftime("%Y-%m-%d %H:%M:%S")
        }

        return {
            "status": "success",
            "source_active": source,
            "date_range": date_range_info,
            "data": {
                "top_content": top_content,
                "top_10_hashtags": top_10_hashtags
            }
        }

    except Exception as e:
        # traceback.print_exc()
        return {
            "status": "error",
            "message": str(e)
        }

def get_network_metrics_summary(source: str = "app"):
    try:
        t_start = time.perf_counter()

        source = (source or "app").strip().lower()

        if source == "instagram":
            sna_query = """
            MATCH (u:InstagramUser)-[:POSTED_IG]->(p:InstagramPost)
            WHERE u.username IS NOT NULL
            RETURN u.username AS uid,
                   u.username AS uname,
                   u.username AS username,
                   p.id AS pid
            LIMIT 500

            UNION ALL

            MATCH (u:InstagramUser)-[:WROTE_IG]->(c:InstagramComment)-[:COMMENTED_ON_IG]->(p:InstagramPost)
            WHERE u.username IS NOT NULL
            RETURN u.username AS uid,
                   u.username AS uname,
                   u.username AS username,
                   p.id AS pid
            LIMIT 500

            UNION ALL

            MATCH (u:InstagramUser)-[:WROTE_IG]->(reply:InstagramComment)-[:REPLIED_TO_IG]->(comment:InstagramComment)-[:COMMENTED_ON_IG]->(p:InstagramPost)
            WHERE u.username IS NOT NULL
            RETURN u.username AS uid,
                   u.username AS uname,
                   u.username AS username,
                   p.id AS pid
            LIMIT 500
            """
        else:
            sna_query = """
            MATCH (u:FirebaseUser)-[:POSTED_FB]->(p:FirebaseKawanSS)
            RETURN u.id AS uid,
                   coalesce(u.nama, u.username, u.id) AS uname,
                   coalesce(u.username, u.nama, u.id) AS username,
                   p.id AS pid
            LIMIT 500

            UNION ALL

            MATCH (u:FirebaseUser)-[:WROTE_FB]->(c:FirebaseKawanSSComment)-[:COMMENTED_ON_FB]->(p:FirebaseKawanSS)
            RETURN u.id AS uid,
                   coalesce(u.nama, u.username, u.id) AS uname,
                   coalesce(u.username, u.nama, u.id) AS username,
                   p.id AS pid
            LIMIT 500

            UNION ALL

            MATCH (u:FirebaseUser)-[:WROTE_FB]->(c:FirebaseInfossComment)-[:COMMENTED_ON_FB]->(p:FirebaseInfoss)
            RETURN u.id AS uid,
                   coalesce(u.nama, u.username, u.id) AS uname,
                   coalesce(u.username, u.nama, u.id) AS username,
                   p.id AS pid
            LIMIT 500
            """

        with neo4j_driver.session() as session:
            sna_records = session.run(sna_query).data()

        top_10_centrality = {
            "degree": [],
            "betweenness": [],
            "closeness": [],
            "eigenvector": []
        }

        top_10_paths = []

        global_metrics = {
            "average_of_average_degrees": 0.0,
            "average_of_network_diameters": 0.0,
            "average_of_connected_users": 0.0
        }

        clique_metrics = {
            "global_metrics": {
                "total_cliques": 0
            },
            "top_10_cliques": []
        }

        descriptive_analysis = {
            "pola_interaksi": "Belum cukup data",
            "aktor_pengaruh": "",
            "komunitas": ""
        }

        G = nx.Graph()

        for record in sna_records:
            uid = str(record.get("uid", "")).strip()
            uname = str(record.get("uname", "")).strip()
            username = str(record.get("username", "")).strip()
            pid = str(record.get("pid", "")).strip()

            if not uid or not pid:
                continue

            if source == "instagram":
                if (
                    is_ignored_instagram_user(uid)
                    or is_ignored_instagram_user(uname)
                    or is_ignored_instagram_user(username)
                ):
                    continue
            else:
                if (
                    is_ignored_app_user(uid)
                    or is_ignored_app_user(uname)
                    or is_ignored_app_user(username)
                    or is_ignored_app_user(f"user_{uid}")
                ):
                    continue

            user_node = f"user_{uid}"
            post_node = f"post_{pid}"

            G.add_node(
                user_node,
                type="user",
                name=uname,
                username=username,
                bipartite=0
            )

            G.add_node(
                post_node,
                type="post",
                bipartite=1
            )

            if G.has_edge(user_node, post_node):
                G[user_node][post_node]["weight"] += 1
            else:
                G.add_edge(
                    user_node,
                    post_node,
                    weight=1
                )

        user_nodes = [
            node
            for node, data in G.nodes(data=True)
            if data.get("type") == "user"
        ]

        if user_nodes:
            # 1. Kalkulasi Global Metrics dari 1-Mode Projection
            G_user = bipartite.projected_graph(G, user_nodes)
            components = sorted(
                nx.connected_components(G_user),
                key=len,
                reverse=True
            )

            total_avg_deg = 0.0
            total_max_reach = 0
            total_reach_count = 0

            for component in components:
                G_sub = G_user.subgraph(component)
                lengths = dict(nx.all_pairs_shortest_path_length(G_sub))

                for user in component:
                    reachable = [
                        distance
                        for target, distance in lengths[user].items()
                        if distance > 0
                    ]

                    if reachable:
                        total_avg_deg += sum(reachable) / len(reachable)
                        total_max_reach += max(reachable)
                        total_reach_count += len(reachable)

            if len(user_nodes) > 0:
                global_metrics = {
                    "average_of_average_degrees": round(total_avg_deg / len(user_nodes), 2),
                    "average_of_network_diameters": round(total_max_reach / len(user_nodes), 2),
                    "average_of_connected_users": round(total_reach_count / len(user_nodes), 2)
                }

            # 2. Kalkulasi Cliques
            all_cliques = []

            if len(G_user.nodes()) < 500:
                all_cliques = [
                    clique
                    for clique in nx.find_cliques(G_user)
                    if len(clique) >= 3
                ]

            if all_cliques:
                all_cliques.sort(key=len, reverse=True)

                clique_metrics["global_metrics"] = {
                    "total_cliques": len(all_cliques),
                    "largest_clique_size": len(all_cliques[0])
                }

                clique_metrics["top_10_cliques"] = [
                    {
                        "rank": index + 1,
                        "size": len(clique),
                        "members": [
                            {
                                "username": G.nodes[user].get("username")
                            }
                            for user in clique
                        ]
                    }
                    for index, clique in enumerate(all_cliques[:10])
                ]

            # 3. Persiapan Weight & Distance untuk Centrality
            for _, _, data in G.edges(data=True):
                weight = data.get("weight", 1)

                try:
                    weight = float(weight)
                except Exception:
                    weight = 1.0

                if weight <= 0:
                    weight = 1.0

                data["weight"] = weight
                data["distance"] = 1.0 / weight

            # 4. Kalkulasi 4 Metrik Centrality
            degree_centrality = nx.degree_centrality(G)

            betweenness_centrality = nx.betweenness_centrality(
                G,
                k=min(20, len(G.nodes())),
                weight="distance",
                normalized=True,
                seed=42
            )

            closeness_centrality = nx.closeness_centrality(
                G,
                distance="distance"
            )

            try:
                eigenvector_centrality = nx.eigenvector_centrality(
                    G,
                    weight="weight",
                    max_iter=1000,
                    tol=1e-03
                )
            except Exception:
                eigenvector_centrality = {
                    node: 0.0
                    for node in G.nodes()
                }

            def _is_valid_user_node(node_id: str) -> bool:
                if not node_id.startswith("user_"):
                    return False

                username_value = G.nodes[node_id].get("username", "")
                name_value = G.nodes[node_id].get("name", "")
                raw_id = node_id.replace("user_", "")

                if source == "instagram":
                    return not (
                        is_ignored_instagram_user(raw_id)
                        or is_ignored_instagram_user(username_value)
                        or is_ignored_instagram_user(name_value)
                    )

                return not (
                    is_ignored_app_user(raw_id)
                    or is_ignored_app_user(username_value)
                    or is_ignored_app_user(name_value)
                    or is_ignored_app_user(node_id)
                )

            top_10_centrality["degree"] = [
                {
                    "id": node.replace("user_", ""),
                    "username": G.nodes[node].get("username", ""),
                    "metrics": {
                        "degree": round(value, 4)
                    }
                }
                for node, value in sorted(
                    degree_centrality.items(),
                    key=lambda item: item[1],
                    reverse=True
                )
                if _is_valid_user_node(node)
            ][:10]

            top_10_centrality["betweenness"] = [
                {
                    "id": node.replace("user_", ""),
                    "username": G.nodes[node].get("username", ""),
                    "metrics": {
                        "betweenness": round(value, 4)
                    }
                }
                for node, value in sorted(
                    betweenness_centrality.items(),
                    key=lambda item: item[1],
                    reverse=True
                )
                if _is_valid_user_node(node)
            ][:10]

            top_10_centrality["closeness"] = [
                {
                    "id": node.replace("user_", ""),
                    "username": G.nodes[node].get("username", ""),
                    "metrics": {
                        "closeness": round(value, 4)
                    }
                }
                for node, value in sorted(
                    closeness_centrality.items(),
                    key=lambda item: item[1],
                    reverse=True
                )
                if _is_valid_user_node(node)
            ][:10]

            top_10_centrality["eigenvector"] = [
                {
                    "id": node.replace("user_", ""),
                    "username": G.nodes[node].get("username", ""),
                    "metrics": {
                        "eigenvector": round(value, 4)
                    }
                }
                for node, value in sorted(
                    eigenvector_centrality.items(),
                    key=lambda item: item[1],
                    reverse=True
                )
                if _is_valid_user_node(node)
            ][:10]

            # 5. Shortest Paths Sample
            if components and len(components[0]) > 2:
                component = [
                    node
                    for node in list(components[0])
                    if _is_valid_user_node(node)
                ]

                sample_pairs = list(itertools.combinations(component[:15], 2))
                random.shuffle(sample_pairs)

                for user_1, user_2 in sample_pairs[:10]:
                    try:
                        path = nx.shortest_path(G, user_1, user_2)

                        path_details = " ➔ ".join([
                            G.nodes[node].get("username", node)
                            if node.startswith("user_")
                            else "[Post]"
                            for node in path
                        ])

                        top_10_paths.append({
                            "source_username": G.nodes[user_1].get("username"),
                            "target_username": G.nodes[user_2].get("username"),
                            "distance_hops": int((len(path) - 1) / 2),
                            "path_details": path_details
                        })

                    except nx.NetworkXNoPath:
                        pass

            # 6. Analisis Deskriptif
            avg_degree = global_metrics.get("average_of_average_degrees", 0)

            if avg_degree == 0:
                descriptive_analysis["pola_interaksi"] = (
                    "Belum ada interaksi yang memadai antar pengguna untuk membentuk sebuah jaringan."
                )
            elif avg_degree <= 2.5:
                descriptive_analysis["pola_interaksi"] = (
                    f"Informasi di dalam jaringan menyebar dengan sangat cepat. "
                    f"Audiens sangat erat dengan rata-rata jarak sosial hanya {avg_degree} langkah."
                )
            else:
                descriptive_analysis["pola_interaksi"] = (
                    f"Audiens terfragmentasi. Rata-rata jarak sosial mencapai {avg_degree} langkah."
                )

            top_degree = (
                top_10_centrality["degree"][0]
                if top_10_centrality["degree"]
                else None
            )

            top_betweenness = (
                top_10_centrality["betweenness"][0]
                if top_10_centrality["betweenness"]
                else None
            )

            actor_text = ""

            if top_degree:
                actor_text += f"'{top_degree['username']}' adalah Key Opinion Leader utama. "

            if top_betweenness:
                actor_text += f"'{top_betweenness['username']}' bertindak sebagai Information Broker krusial."

            descriptive_analysis["aktor_pengaruh"] = (
                actor_text
                if actor_text
                else "Aktor dominan belum teridentifikasi."
            )

            total_cliques = clique_metrics["global_metrics"].get("total_cliques", 0)

            if total_cliques > 0:
                descriptive_analysis["komunitas"] = (
                    f"Terdapat {total_cliques} sub-grup eksklusif (cliques) di dalam jaringan."
                )
            else:
                descriptive_analysis["komunitas"] = "Grup eksklusif belum terdeteksi."

        process_time = round(time.perf_counter() - t_start, 3)

        return {
            "status": "success",
            "source_active": source,
            "process_time_sec": process_time,
            "data": {
                "top_10_centrality": top_10_centrality,
                "shortest_path_metrics": {
                    "top_10_paths": top_10_paths,
                    "global_averages": global_metrics
                },
                "clique_metrics": clique_metrics,
                "descriptive_analysis": descriptive_analysis
            }
        }

    except Exception as e:
        traceback.print_exc()

        return {
            "status": "error",
            "message": str(e)
        }

def get_live_analytics_summary(
    start_date: str | None = None,
    end_date: str | None = None,
):
    try:
        property_id = os.getenv("GA_PROPERTY_ID")

        if not property_id:
            raise ValueError("GA_PROPERTY_ID tidak ditemukan di environment variables.")

        credentials = get_ga_credentials()
        client = BetaAnalyticsDataClient(credentials=credentials)

        request_30 = RunRealtimeReportRequest(
            property=f"properties/{property_id}",
            metrics=[Metric(name="activeUsers")]
        )
        response_30 = client.run_realtime_report(request_30)

        active_users_30_min = 0
        if response_30.rows:
            active_users_30_min = int(response_30.rows[0].metric_values[0].value)

        request_5 = RunRealtimeReportRequest(
            property=f"properties/{property_id}",
            metrics=[Metric(name="activeUsers")],
            dimension_filter=FilterExpression(
                filter=Filter(
                    field_name="minutesAgo",
                    in_list_filter=Filter.InListFilter(
                        values=["00", "01", "02", "03", "04"]
                    )
                )
            )
        )
        response_5 = client.run_realtime_report(request_5)

        active_users_5_min = 0
        if response_5.rows:
            active_users_5_min = int(response_5.rows[0].metric_values[0].value)

        now = datetime.now()

        if not start_date:
            start_date = now.replace(day=1).strftime("%Y-%m-%d")

        if not end_date:
            end_date = now.strftime("%Y-%m-%d")

        overview_rows = _run_ga_report(
            dimensions=[],
            metrics=[
                "activeUsers",
                "newUsers",
                "totalUsers",
                "sessions",
                "engagedSessions",
                "screenPageViews",
                "eventCount",
                "averageSessionDuration",
            ],
            start_date=start_date,
            end_date=end_date,
            limit=1,
        )

        overview = overview_rows[0] if overview_rows else {}

        users_by_date = _run_ga_report(
            dimensions=["date"],
            metrics=[
                "activeUsers",
                "newUsers",
                "sessions",
            ],
            start_date=start_date,
            end_date=end_date,
            limit=100,
        )

        users_by_country = _run_ga_report(
            dimensions=["country"],
            metrics=["activeUsers"],
            start_date=start_date,
            end_date=end_date,
            limit=10,
            order_by_metric="activeUsers",
        )

        users_by_city = _run_ga_report(
            dimensions=["city"],
            metrics=["activeUsers"],
            start_date=start_date,
            end_date=end_date,
            limit=10,
            order_by_metric="activeUsers",
        )

        return {
            "status": "success",
            "data": {
                "integrations": {
                    "google_analytics": {
                        "status": "connected",
                        "property_id": property_id,
                        "realtime": {
                            "active_users_last_30_min": active_users_30_min,
                            "active_users_last_5_min": active_users_5_min,
                        },
                        "date_range": {
                            "start_date": start_date,
                            "end_date": end_date,
                        },
                        "summary": {
                            "monthly_active_users": overview.get("activeUsers", 0),
                            "monthly_new_users": overview.get("newUsers", 0),
                            "monthly_total_users": overview.get("totalUsers", 0),
                            "monthly_sessions": overview.get("sessions", 0),
                            "monthly_engaged_sessions": overview.get("engagedSessions", 0),
                            "monthly_screen_page_views": overview.get("screenPageViews", 0),
                            "monthly_event_count": overview.get("eventCount", 0),
                            "average_session_duration_seconds": overview.get(
                                "averageSessionDuration",
                                0,
                            ),
                        },
                        "users_by_date": users_by_date,
                        "users_by_country": users_by_country,
                        "users_by_city": users_by_city,
                    }
                }
            }
        }

    except Exception as e:
        traceback.print_exc()
        return {
            "status": "error",
            "message": str(e),
        }

def get_google_analytics_summary(
    start_date: str | None = None,
    end_date: str | None = None,
):
    try:
        property_id = os.getenv("GA_PROPERTY_ID")

        if not property_id:
            raise ValueError("GA_PROPERTY_ID tidak ditemukan di environment variables.")

        now = datetime.now()

        if not start_date:
            start_date = now.replace(day=1).strftime("%Y-%m-%d")

        if not end_date:
            end_date = now.strftime("%Y-%m-%d")

        overview_rows = _run_ga_report(
            dimensions=[],
            metrics=[
                "activeUsers",
                "newUsers",
                "totalUsers",
                "sessions",
                "engagedSessions",
                "screenPageViews",
                "eventCount",
                "averageSessionDuration",
            ],
            start_date=start_date,
            end_date=end_date,
            limit=1,
        )

        overview = overview_rows[0] if overview_rows else {}

        users_by_date = _run_ga_report(
            dimensions=["date"],
            metrics=[
                "activeUsers",
                "newUsers",
                "sessions",
            ],
            start_date=start_date,
            end_date=end_date,
            limit=100,
        )

        users_by_country = _run_ga_report(
            dimensions=["country"],
            metrics=["activeUsers"],
            start_date=start_date,
            end_date=end_date,
            limit=10,
            order_by_metric="activeUsers",
        )

        users_by_city = _run_ga_report(
            dimensions=["city"],
            metrics=["activeUsers"],
            start_date=start_date,
            end_date=end_date,
            limit=10,
            order_by_metric="activeUsers",
        )

        return {
            "status": "success",
            "data": {
                "google_analytics": {
                    "status": "connected",
                    "date_range": {
                        "start_date": start_date,
                        "end_date": end_date,
                    },
                    "summary": {
                        "monthly_active_users": overview.get("activeUsers", 0),
                        "monthly_new_users": overview.get("newUsers", 0),
                        "monthly_total_users": overview.get("totalUsers", 0),
                        "monthly_sessions": overview.get("sessions", 0),
                        "monthly_engaged_sessions": overview.get("engagedSessions", 0),
                        "monthly_screen_page_views": overview.get("screenPageViews", 0),
                        "monthly_event_count": overview.get("eventCount", 0),
                        "average_session_duration_seconds": overview.get(
                            "averageSessionDuration",
                            0,
                        ),
                    },
                    "users_by_date": users_by_date,
                    "users_by_country": users_by_country,
                    "users_by_city": users_by_city,
                }
            }
        }

    except Exception as e:
        traceback.print_exc()

        return {
            "status": "error",
            "message": str(e),
        }

def get_google_analytics_report_summary(
    start_date: str | None = None,
    end_date: str | None = None,
):
    try:
        now = datetime.now()

        if not start_date:
            start_date = now.replace(day=1).strftime("%Y-%m-%d")

        if not end_date:
            end_date = now.strftime("%Y-%m-%d")

        overview_rows = _run_ga_report(
            dimensions=[],
            metrics=[
                "activeUsers",
                "newUsers",
                "totalUsers",
                "sessions",
                "engagedSessions",
                "screenPageViews",
                "eventCount",
                "averageSessionDuration",
            ],
            start_date=start_date,
            end_date=end_date,
            limit=1,
        )

        users_by_date = _run_ga_report(
            dimensions=["date"],
            metrics=[
                "activeUsers",
                "newUsers",
                "sessions",
            ],
            start_date=start_date,
            end_date=end_date,
            limit=100,
            order_by_metric="activeUsers",
        )

        users_by_platform = _run_ga_report(
            dimensions=["platform"],
            metrics=[
                "activeUsers",
                "newUsers",
            ],
            start_date=start_date,
            end_date=end_date,
            limit=10,
            order_by_metric="activeUsers",
        )

        users_by_country = _run_ga_report(
            dimensions=["country"],
            metrics=["activeUsers"],
            start_date=start_date,
            end_date=end_date,
            limit=10,
            order_by_metric="activeUsers",
        )

        users_by_city = _run_ga_report(
            dimensions=["city"],
            metrics=["activeUsers"],
            start_date=start_date,
            end_date=end_date,
            limit=10,
            order_by_metric="activeUsers",
        )

        users_by_language = _run_ga_report(
            dimensions=["language"],
            metrics=["activeUsers"],
            start_date=start_date,
            end_date=end_date,
            limit=10,
            order_by_metric="activeUsers",
        )

        users_by_channel = _run_ga_report(
            dimensions=["firstUserPrimaryChannelGroup"],
            metrics=["newUsers"],
            start_date=start_date,
            end_date=end_date,
            limit=10,
            order_by_metric="newUsers",
        )

        sessions_by_channel = _run_ga_report(
            dimensions=["sessionDefaultChannelGroup"],
            metrics=["sessions"],
            start_date=start_date,
            end_date=end_date,
            limit=10,
            order_by_metric="sessions",
        )

        top_events = _run_ga_report(
            dimensions=["eventName"],
            metrics=["eventCount"],
            start_date=start_date,
            end_date=end_date,
            limit=10,
            order_by_metric="eventCount",
        )

        top_screens = _run_ga_report(
            dimensions=["unifiedScreenName"],
            metrics=["screenPageViews"],
            start_date=start_date,
            end_date=end_date,
            limit=10,
            order_by_metric="screenPageViews",
        )

        return {
            "status": "success",
            "date_range": {
                "start_date": start_date,
                "end_date": end_date,
            },
            "data": {
                "overview": overview_rows[0] if overview_rows else {
                    "activeUsers": 0,
                    "newUsers": 0,
                    "totalUsers": 0,
                    "sessions": 0,
                    "engagedSessions": 0,
                    "screenPageViews": 0,
                    "eventCount": 0,
                    "averageSessionDuration": 0,
                },
                "users_by_date": users_by_date,
                "users_by_platform": users_by_platform,
                "users_by_country": users_by_country,
                "users_by_city": users_by_city,
                "users_by_language": users_by_language,
                "users_by_channel": users_by_channel,
                "sessions_by_channel": sessions_by_channel,
                "top_events": top_events,
                "top_screens": top_screens,
            },
        }

    except Exception as e:
        traceback.print_exc()
        return {
            "status": "error",
            "message": str(e),
        }

def get_stats_summary():
    try:
        now = datetime.now()

        this_month_start = now.replace(
            day=1,
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )

        last_month_end = this_month_start - timedelta(seconds=1)
        last_month_start = last_month_end.replace(
            day=1,
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )

        last_30_days_start = now - timedelta(days=30)

        this_month_start_epoch = int(this_month_start.timestamp() * 1000)
        now_epoch = int(now.timestamp() * 1000)

        last_month_start_epoch = int(last_month_start.timestamp() * 1000)
        last_month_end_epoch = int(last_month_end.timestamp() * 1000)

        last_30_days_epoch = int(last_30_days_start.timestamp() * 1000)

        this_month_start_iso = this_month_start.isoformat()
        now_iso = now.isoformat()

        last_month_start_iso = last_month_start.isoformat()
        last_month_end_iso = last_month_end.isoformat()

        last_30_days_iso = last_30_days_start.isoformat()

        this_month_start_ig = this_month_start.astimezone(timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%S+0000"
        )
        now_ig = now.astimezone(timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%S+0000"
        )
        last_30_days_ig = last_30_days_start.astimezone(timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%S+0000"
        )

        query = """
        CALL () {
            MATCH (u:FirebaseUser)
            RETURN count(u) AS total_users
        }

        CALL () {
            MATCH (u:FirebaseUser)
            WHERE
                (
                    u.createdAt IS NOT NULL
                    AND u.createdAt >= $this_month_start_epoch
                    AND u.createdAt <= $now_epoch
                )
                OR
                (
                    u.createdAt IS NOT NULL
                    AND toString(u.createdAt) >= $this_month_start_iso
                    AND toString(u.createdAt) <= $now_iso
                )
                OR
                (
                    u.tglRegister IS NOT NULL
                    AND toString(u.tglRegister) >= $this_month_start_iso
                    AND toString(u.tglRegister) <= $now_iso
                )
                OR
                (
                    u.registerDate IS NOT NULL
                    AND toString(u.registerDate) >= $this_month_start_iso
                    AND toString(u.registerDate) <= $now_iso
                )
            RETURN count(u) AS new_users_this_month
        }

        CALL () {
            MATCH (u:FirebaseUser)
            WHERE
                (
                    u.createdAt IS NOT NULL
                    AND u.createdAt >= $last_month_start_epoch
                    AND u.createdAt <= $last_month_end_epoch
                )
                OR
                (
                    u.createdAt IS NOT NULL
                    AND toString(u.createdAt) >= $last_month_start_iso
                    AND toString(u.createdAt) <= $last_month_end_iso
                )
                OR
                (
                    u.tglRegister IS NOT NULL
                    AND toString(u.tglRegister) >= $last_month_start_iso
                    AND toString(u.tglRegister) <= $last_month_end_iso
                )
                OR
                (
                    u.registerDate IS NOT NULL
                    AND toString(u.registerDate) >= $last_month_start_iso
                    AND toString(u.registerDate) <= $last_month_end_iso
                )
            RETURN count(u) AS new_users_last_month
        }

        CALL () {
            MATCH (p:FirebaseInfoss)
            WHERE p.isDeleted = false OR p.isDeleted IS NULL
            RETURN count(p) AS total_infoss_posts
        }

        CALL () {
            MATCH (p:FirebaseKawanSS)
            WHERE p.isDeleted = false OR p.isDeleted IS NULL
            RETURN count(p) AS total_kawanss_posts
        }

        CALL () {
            MATCH (p:FirebaseInfoss)
            WHERE (p.isDeleted = false OR p.isDeleted IS NULL)
              AND (
                (
                    p.createdAt IS NOT NULL
                    AND p.createdAt >= $last_30_days_epoch
                    AND p.createdAt <= $now_epoch
                )
                OR
                (
                    p.uploadDate IS NOT NULL
                    AND toString(p.uploadDate) >= $last_30_days_iso
                    AND toString(p.uploadDate) <= $now_iso
                )
                OR
                (
                    p.createdAt IS NOT NULL
                    AND toString(p.createdAt) >= $last_30_days_iso
                    AND toString(p.createdAt) <= $now_iso
                )
              )
            RETURN count(p) AS new_30_days_infoss
        }

        CALL () {
            MATCH (p:FirebaseKawanSS)
            WHERE (p.isDeleted = false OR p.isDeleted IS NULL)
              AND (
                (
                    p.createdAt IS NOT NULL
                    AND p.createdAt >= $last_30_days_epoch
                    AND p.createdAt <= $now_epoch
                )
                OR
                (
                    p.createdAt IS NOT NULL
                    AND toString(p.createdAt) >= $last_30_days_iso
                    AND toString(p.createdAt) <= $now_iso
                )
              )
            RETURN count(p) AS new_30_days_kawanss
        }

        CALL () {
            MATCH (c:FirebaseInfossComment)
            RETURN count(c) AS total_infoss_comments
        }

        CALL () {
            MATCH (c:FirebaseKawanSSComment)
            RETURN count(c) AS total_kawanss_comments
        }

        CALL () {
            MATCH (c:FirebaseInfossComment)
            WHERE
                (
                    c.createdAt IS NOT NULL
                    AND c.createdAt >= $this_month_start_epoch
                    AND c.createdAt <= $now_epoch
                )
                OR
                (
                    c.createdAt IS NOT NULL
                    AND toString(c.createdAt) >= $this_month_start_iso
                    AND toString(c.createdAt) <= $now_iso
                )
            RETURN count(c) AS this_month_infoss_comments
        }

        CALL () {
            MATCH (c:FirebaseKawanSSComment)
            WHERE
                (
                    c.createdAt IS NOT NULL
                    AND c.createdAt >= $this_month_start_epoch
                    AND c.createdAt <= $now_epoch
                )
                OR
                (
                    c.createdAt IS NOT NULL
                    AND toString(c.createdAt) >= $this_month_start_iso
                    AND toString(c.createdAt) <= $now_iso
                )
            RETURN count(c) AS this_month_kawanss_comments
        }

        CALL () {
            MATCH (p:InstagramPost)
            RETURN count(p) AS total_instagram_posts
        }

        CALL () {
            MATCH (p:InstagramPost)
            WHERE p.timestamp IS NOT NULL
              AND p.timestamp >= $last_30_days_ig
              AND p.timestamp <= $now_ig
            RETURN count(p) AS new_30_days_instagram_posts
        }

        CALL () {
            MATCH (c:InstagramComment)
            RETURN count(c) AS total_instagram_comments
        }

        CALL () {
            MATCH (c:InstagramComment)
            WHERE c.timestamp IS NOT NULL
              AND c.timestamp >= $this_month_start_ig
              AND c.timestamp <= $now_ig
            RETURN count(c) AS this_month_instagram_comments
        }

        RETURN
            total_users,
            new_users_this_month,
            new_users_last_month,
            total_infoss_posts,
            total_kawanss_posts,
            new_30_days_infoss,
            new_30_days_kawanss,
            total_infoss_comments,
            total_kawanss_comments,
            this_month_infoss_comments,
            this_month_kawanss_comments,
            total_instagram_posts,
            new_30_days_instagram_posts,
            total_instagram_comments,
            this_month_instagram_comments
        """

        with neo4j_driver.session() as session:
            record = session.run(
                query,
                this_month_start_epoch=this_month_start_epoch,
                now_epoch=now_epoch,
                last_month_start_epoch=last_month_start_epoch,
                last_month_end_epoch=last_month_end_epoch,
                last_30_days_epoch=last_30_days_epoch,
                this_month_start_iso=this_month_start_iso,
                now_iso=now_iso,
                last_month_start_iso=last_month_start_iso,
                last_month_end_iso=last_month_end_iso,
                last_30_days_iso=last_30_days_iso,
                this_month_start_ig=this_month_start_ig,
                now_ig=now_ig,
                last_30_days_ig=last_30_days_ig,
            ).single()

        if record is None:
            raise HTTPException(
                status_code=404,
                detail="Data statistik dashboard tidak ditemukan.",
            )

        data = dict(record)

        total_users = _safe_int(data.get("total_users"))
        new_users_this_month = _safe_int(data.get("new_users_this_month"))
        new_users_last_month = _safe_int(data.get("new_users_last_month"))

        total_infoss_posts = _safe_int(data.get("total_infoss_posts"))
        total_kawanss_posts = _safe_int(data.get("total_kawanss_posts"))
        total_app_posts = total_infoss_posts + total_kawanss_posts

        new_30_days_infoss = _safe_int(data.get("new_30_days_infoss"))
        new_30_days_kawanss = _safe_int(data.get("new_30_days_kawanss"))
        new_30_days_app_posts = new_30_days_infoss + new_30_days_kawanss

        total_infoss_comments = _safe_int(data.get("total_infoss_comments"))
        total_kawanss_comments = _safe_int(data.get("total_kawanss_comments"))
        total_app_comments = total_infoss_comments + total_kawanss_comments

        this_month_infoss_comments = _safe_int(
            data.get("this_month_infoss_comments")
        )
        this_month_kawanss_comments = _safe_int(
            data.get("this_month_kawanss_comments")
        )
        this_month_app_comments = (
            this_month_infoss_comments + this_month_kawanss_comments
        )

        total_instagram_posts = _safe_int(data.get("total_instagram_posts"))
        new_30_days_instagram_posts = _safe_int(
            data.get("new_30_days_instagram_posts")
        )

        total_instagram_comments = _safe_int(data.get("total_instagram_comments"))
        this_month_instagram_comments = _safe_int(
            data.get("this_month_instagram_comments")
        )

        user_growth_percentage = _safe_percentage(
            new_users_this_month,
            new_users_last_month,
        )

        total_monthly_comments = (
            this_month_app_comments + this_month_instagram_comments
        )

        total_monthly_interactions = (
            new_users_this_month
            + new_30_days_app_posts
            + new_30_days_instagram_posts
            + total_monthly_comments
        )

        users = {
            "total": total_users,
            "new_this_month": new_users_this_month,
            "new_last_month": new_users_last_month,
            "growth_percentage": user_growth_percentage,
            "comparison_text": (
                f"{new_users_this_month} user baru bulan ini, "
                f"{new_users_last_month} user baru bulan lalu"
            ),

            # legacy key agar frontend lama tetap aman
            "total_post": total_app_posts,
            "total_post_kawanss": total_kawanss_posts,
        }

        posts = {
            "total": total_app_posts,
            "total_infoss": total_infoss_posts,
            "total_kawan_ss": total_kawanss_posts,
            "new_30_days": new_30_days_app_posts,
            "new_30_days_infoss": new_30_days_infoss,
            "new_30_days_kawan_ss": new_30_days_kawanss,

            # info tambahan, bukan source utama total post dashboard
            "instagram_total": total_instagram_posts,
            "instagram_new_30_days": new_30_days_instagram_posts,
        }

        monthly_report = {
            "report_type": "monthly",
            "period": {
                "start": this_month_start.strftime("%Y-%m-%d"),
                "end": now.strftime("%Y-%m-%d"),
                "label": now.strftime("%B %Y"),
            },
            "users": {
                "total": total_users,
                "new_this_month": new_users_this_month,
                "new_last_month": new_users_last_month,
                "growth_percentage": user_growth_percentage,
            },
            "posts": {
                "total": total_app_posts,
                "infoss_total": total_infoss_posts,
                "kawan_ss_total": total_kawanss_posts,
                "new_30_days": new_30_days_app_posts,
                "infoss_new_30_days": new_30_days_infoss,
                "kawan_ss_new_30_days": new_30_days_kawanss,
                "instagram_total": total_instagram_posts,
                "instagram_new_30_days": new_30_days_instagram_posts,
            },
            "comments": {
                "app_total": total_app_comments,
                "app_this_month": this_month_app_comments,
                "instagram_total": total_instagram_comments,
                "instagram_this_month": this_month_instagram_comments,
                "combined_this_month": total_monthly_comments,
            },
            "interactions": {
                "combined_this_month": total_monthly_interactions,
                "description": (
                    "Jumlah interaksi dihitung dari user baru, post baru, "
                    "komentar aplikasi, dan komentar Instagram pada bulan berjalan."
                ),
            },
            "summary_cards": [
                {
                    "title": "Total Users",
                    "value": total_users,
                    "description": "Total pengguna aplikasi Suara Surabaya.",
                },
                {
                    "title": "New Users This Month",
                    "value": new_users_this_month,
                    "description": "Jumlah pengguna baru aplikasi pada bulan berjalan.",
                },
                {
                    "title": "Total App Posts",
                    "value": total_app_posts,
                    "description": "Total post dari data aplikasi, yaitu InfoSS dan KawanSS.",
                },
                {
                    "title": "Monthly Interactions",
                    "value": total_monthly_interactions,
                    "description": "Ringkasan interaksi aplikasi dan Instagram pada bulan berjalan.",
                },
            ],
        }

        return {
            "status": "success",
            "data": {
                "users": users,
                "posts": posts,
                "monthly_report": monthly_report,
            },
        }

    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        return {
            "status": "error",
            "message": str(e),
        }
    """
    Dashboard stats + monthly report summary.

    Struktur utama tetap:
    {
        status,
        data: {
            users: {...},
            posts: {...}
        }
    }

    Query sudah menggunakan CALL () { ... } agar tidak terkena warning deprecated Neo4j.
    """

    try:
        now = datetime.now()

        this_month_start = now.replace(
            day=1,
            hour=0,
            minute=0,
            second=0,
            microsecond=0
        )

        last_month_end = this_month_start - timedelta(seconds=1)
        last_month_start = last_month_end.replace(
            day=1,
            hour=0,
            minute=0,
            second=0,
            microsecond=0
        )

        last_30_days_start = now - timedelta(days=30)

        this_month_start_epoch = int(this_month_start.timestamp() * 1000)
        now_epoch = int(now.timestamp() * 1000)

        last_month_start_epoch = int(last_month_start.timestamp() * 1000)
        last_month_end_epoch = int(last_month_end.timestamp() * 1000)

        last_30_days_epoch = int(last_30_days_start.timestamp() * 1000)

        this_month_start_iso = this_month_start.isoformat()
        now_iso = now.isoformat()

        last_month_start_iso = last_month_start.isoformat()
        last_month_end_iso = last_month_end.isoformat()

        last_30_days_iso = last_30_days_start.isoformat()

        now_ig = now.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+0000")
        last_30_days_ig = last_30_days_start.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+0000")

        query = """
        CALL () {
            MATCH (u:FirebaseUser)
            RETURN count(u) AS total_users
        }

        CALL () {
            MATCH (u:FirebaseUser)
            WHERE
                (
                    u.createdAt IS NOT NULL
                    AND u.createdAt >= $this_month_start_epoch
                    AND u.createdAt <= $now_epoch
                )
                OR
                (
                    u.createdAt IS NOT NULL
                    AND toString(u.createdAt) >= $this_month_start_iso
                    AND toString(u.createdAt) <= $now_iso
                )
                OR
                (
                    u.tglRegister IS NOT NULL
                    AND toString(u.tglRegister) >= $this_month_start_iso
                    AND toString(u.tglRegister) <= $now_iso
                )
                OR
                (
                    u.registerDate IS NOT NULL
                    AND toString(u.registerDate) >= $this_month_start_iso
                    AND toString(u.registerDate) <= $now_iso
                )
            RETURN count(u) AS new_users_this_month
        }

        CALL () {
            MATCH (u:FirebaseUser)
            WHERE
                (
                    u.createdAt IS NOT NULL
                    AND u.createdAt >= $last_month_start_epoch
                    AND u.createdAt <= $last_month_end_epoch
                )
                OR
                (
                    u.createdAt IS NOT NULL
                    AND toString(u.createdAt) >= $last_month_start_iso
                    AND toString(u.createdAt) <= $last_month_end_iso
                )
                OR
                (
                    u.tglRegister IS NOT NULL
                    AND toString(u.tglRegister) >= $last_month_start_iso
                    AND toString(u.tglRegister) <= $last_month_end_iso
                )
                OR
                (
                    u.registerDate IS NOT NULL
                    AND toString(u.registerDate) >= $last_month_start_iso
                    AND toString(u.registerDate) <= $last_month_end_iso
                )
            RETURN count(u) AS new_users_last_month
        }

        CALL () {
            MATCH (p:FirebaseInfoss)
            WHERE p.isDeleted = false OR p.isDeleted IS NULL
            RETURN count(p) AS total_posts
        }

        CALL () {
            MATCH (p:FirebaseInfoss)
            WHERE (p.isDeleted = false OR p.isDeleted IS NULL)
              AND (
                    (
                        p.createdAt IS NOT NULL
                        AND p.createdAt >= $last_30_days_epoch
                        AND p.createdAt <= $now_epoch
                    )
                    OR
                    (
                        p.uploadDate IS NOT NULL
                        AND toString(p.uploadDate) >= $last_30_days_iso
                        AND toString(p.uploadDate) <= $now_iso
                    )
                    OR
                    (
                        p.createdAt IS NOT NULL
                        AND toString(p.createdAt) >= $last_30_days_iso
                        AND toString(p.createdAt) <= $now_iso
                    )
              )
            RETURN count(p) AS new_30_days_posts
        }

        CALL () {
            MATCH (p:FirebaseKawanSS)
            WHERE p.isDeleted = false OR p.isDeleted IS NULL
            RETURN count(p) AS total_kawan_ss
        }

        CALL () {
            MATCH (p:FirebaseKawanSS)
            WHERE (p.isDeleted = false OR p.isDeleted IS NULL)
              AND (
                    (
                        p.createdAt IS NOT NULL
                        AND p.createdAt >= $last_30_days_epoch
                        AND p.createdAt <= $now_epoch
                    )
                    OR
                    (
                        p.createdAt IS NOT NULL
                        AND toString(p.createdAt) >= $last_30_days_iso
                        AND toString(p.createdAt) <= $now_iso
                    )
              )
            RETURN count(p) AS new_30_days_kawanss
        }

        CALL () {
            MATCH (p:InstagramPost)
            RETURN count(p) AS total_instagram_posts
        }

        CALL () {
            MATCH (p:InstagramPost)
            WHERE p.timestamp IS NOT NULL
              AND p.timestamp >= $last_30_days_ig
              AND p.timestamp <= $now_ig
            RETURN count(p) AS new_30_days_instagram_posts
        }

        CALL () {
            MATCH (u:InstagramUser)
            RETURN count(u) AS total_instagram_users
        }

        CALL () {
            MATCH (c)
            WHERE c:FirebaseKawanSSComment OR c:FirebaseInfossComment
            RETURN count(c) AS total_app_comments
        }

        CALL () {
            MATCH (c:InstagramComment)
            RETURN count(c) AS total_instagram_comments
        }

        CALL () {
            MATCH ()-[r]->()
            WHERE type(r) IN ['LIKES_KAWAN_FB', 'LIKES_INFO_FB']
            RETURN count(r) AS total_app_likes
        }

        CALL () {
            MATCH ()-[r]->()
            WHERE type(r) IN [
                'POSTED_FB',
                'WROTE_FB',
                'COMMENTED_ON_FB',
                'LIKES_KAWAN_FB',
                'LIKES_INFO_FB'
            ]
            RETURN count(r) AS total_app_interactions
        }

        CALL () {
            MATCH ()-[r]->()
            WHERE type(r) IN [
                'POSTED_IG',
                'WROTE_IG',
                'COMMENTED_ON_IG',
                'REPLIED_TO_IG'
            ]
            RETURN count(r) AS total_instagram_interactions
        }

        RETURN total_users,
               new_users_this_month,
               new_users_last_month,
               total_posts,
               new_30_days_posts,
               total_kawan_ss,
               new_30_days_kawanss,
               total_instagram_posts,
               new_30_days_instagram_posts,
               total_instagram_users,
               total_app_comments,
               total_instagram_comments,
               total_app_likes,
               total_app_interactions,
               total_instagram_interactions
        """

        with neo4j_driver.session() as session:
            result = session.run(
                query,
                this_month_start_epoch=this_month_start_epoch,
                now_epoch=now_epoch,
                last_month_start_epoch=last_month_start_epoch,
                last_month_end_epoch=last_month_end_epoch,
                last_30_days_epoch=last_30_days_epoch,
                this_month_start_iso=this_month_start_iso,
                now_iso=now_iso,
                last_month_start_iso=last_month_start_iso,
                last_month_end_iso=last_month_end_iso,
                last_30_days_iso=last_30_days_iso,
                now_ig=now_ig,
                last_30_days_ig=last_30_days_ig,
            ).single()

        if not result:
            raise RuntimeError("Query statistik tidak mengembalikan data.")

        total_users = int(result.get("total_users", 0))
        new_users_this_month = int(result.get("new_users_this_month", 0))
        new_users_last_month = int(result.get("new_users_last_month", 0))

        total_posts = int(result.get("total_posts", 0))
        new_30_days_posts = int(result.get("new_30_days_posts", 0))

        total_kawan_ss = int(result.get("total_kawan_ss", 0))
        new_30_days_kawanss = int(result.get("new_30_days_kawanss", 0))

        total_instagram_posts = int(result.get("total_instagram_posts", 0))
        new_30_days_instagram_posts = int(result.get("new_30_days_instagram_posts", 0))
        total_instagram_users = int(result.get("total_instagram_users", 0))

        total_app_comments = int(result.get("total_app_comments", 0))
        total_instagram_comments = int(result.get("total_instagram_comments", 0))
        total_app_likes = int(result.get("total_app_likes", 0))

        total_app_interactions = int(result.get("total_app_interactions", 0))
        total_instagram_interactions = int(result.get("total_instagram_interactions", 0))

        if new_users_last_month > 0:
            growth_percentage = round(
                ((new_users_this_month - new_users_last_month) / new_users_last_month) * 100,
                2
            )
        elif new_users_this_month > 0:
            growth_percentage = 100.0
        else:
            growth_percentage = 0.0

        total_comments = total_app_comments + total_instagram_comments
        total_interactions = total_app_interactions + total_instagram_interactions
        total_all_posts = total_posts + total_kawan_ss + total_instagram_posts

        return {
            "status": "success",
            "data": {
                "users": {
                    "total": total_users,
                    "new_this_month": new_users_this_month,
                    "new_last_month": new_users_last_month,
                    "growth_percentage": growth_percentage,
                },
                "posts": {
                    "total": total_posts,
                    "new_30_days": new_30_days_posts,
                    "total_kawn_ss": total_kawan_ss,
                    "new_30_days_kawanss": new_30_days_kawanss,
                },
                "monthly_report": {
                    "period": {
                        "this_month_start": this_month_start.strftime("%Y-%m-%d"),
                        "last_month_start": last_month_start.strftime("%Y-%m-%d"),
                        "last_month_end": last_month_end.strftime("%Y-%m-%d"),
                        "last_30_days_start": last_30_days_start.strftime("%Y-%m-%d"),
                        "generated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
                    },
                    "users": {
                        "app_total": total_users,
                        "instagram_total": total_instagram_users,
                        "combined_total": total_users + total_instagram_users,
                        "new_this_month": new_users_this_month,
                        "new_last_month": new_users_last_month,
                        "growth_percentage": growth_percentage,
                    },
                    "posts": {
                        "infoss_total": total_posts,
                        "infoss_new_30_days": new_30_days_posts,
                        "kawan_ss_total": total_kawan_ss,
                        "kawan_ss_new_30_days": new_30_days_kawanss,
                        "instagram_total": total_instagram_posts,
                        "instagram_new_30_days": new_30_days_instagram_posts,
                        "combined_total": total_all_posts,
                    },
                    "comments": {
                        "app_total": total_app_comments,
                        "instagram_total": total_instagram_comments,
                        "combined_total": total_comments,
                    },
                    "likes": {
                        "app_total": total_app_likes,
                    },
                    "interactions": {
                        "app_total": total_app_interactions,
                        "instagram_total": total_instagram_interactions,
                        "combined_total": total_interactions,
                    },
                    "summary_cards": [
                        {
                            "title": "Total Users",
                            "value": total_users + total_instagram_users,
                            "description": "Total pengguna aplikasi dan Instagram yang tersimpan dalam Neo4j",
                        },
                        {
                            "title": "New Users This Month",
                            "value": new_users_this_month,
                            "description": "Jumlah pengguna aplikasi baru pada bulan berjalan",
                        },
                        {
                            "title": "Total Posts",
                            "value": total_all_posts,
                            "description": "Total konten Infoss, Kawan SS, dan Instagram",
                        },
                        {
                            "title": "Total Interactions",
                            "value": total_interactions,
                            "description": "Total relasi interaksi aplikasi dan Instagram",
                        },
                    ],
                },
            },
        }

    except Exception as e:
        traceback.print_exc()

        return {
            "status": "error",
            "message": str(e),
            "data": {
                "users": {
                    "total": 0,
                    "new_this_month": 0,
                    "new_last_month": 0,
                    "growth_percentage": 0.0,
                },
                "posts": {
                    "total": 0,
                    "new_30_days": 0,
                    "total_kawn_ss": 0,
                    "new_30_days_kawanss": 0,
                },
                "monthly_report": {},
            },
        }
    """
    Dashboard stats + monthly report summary.

    Struktur utama tetap:
    {
        status,
        data: {
            users: {...},
            posts: {...}
        }
    }

    Tambahan monthly report dimasukkan ke:
    data.monthly_report
    """

    try:
        now = datetime.now()

        # Bulan ini
        this_month_start = now.replace(
            day=1,
            hour=0,
            minute=0,
            second=0,
            microsecond=0
        )

        # Bulan lalu
        last_month_end = this_month_start - timedelta(seconds=1)
        last_month_start = last_month_end.replace(
            day=1,
            hour=0,
            minute=0,
            second=0,
            microsecond=0
        )

        # 30 hari terakhir
        last_30_days_start = now - timedelta(days=30)

        # Format epoch millisecond untuk data Firebase yang pakai createdAt angka
        this_month_start_epoch = int(this_month_start.timestamp() * 1000)
        now_epoch = int(now.timestamp() * 1000)

        last_month_start_epoch = int(last_month_start.timestamp() * 1000)
        last_month_end_epoch = int(last_month_end.timestamp() * 1000)

        last_30_days_epoch = int(last_30_days_start.timestamp() * 1000)

        # Format ISO string untuk data yang pakai uploadDate / createdAt string
        this_month_start_iso = this_month_start.isoformat()
        now_iso = now.isoformat()

        last_month_start_iso = last_month_start.isoformat()
        last_month_end_iso = last_month_end.isoformat()

        last_30_days_iso = last_30_days_start.isoformat()

        # Format Instagram timestamp: 2026-04-29T14:58:55+0000
        this_month_start_ig = this_month_start.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+0000")
        now_ig = now.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+0000")

        last_30_days_ig = last_30_days_start.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+0000")

        query = """
        CALL {
            MATCH (u:FirebaseUser)
            RETURN count(u) AS total_users
        }

        CALL {
            MATCH (u:FirebaseUser)
            WHERE
                (
                    u.createdAt IS NOT NULL
                    AND u.createdAt >= $this_month_start_epoch
                    AND u.createdAt <= $now_epoch
                )
                OR
                (
                    u.createdAt IS NOT NULL
                    AND toString(u.createdAt) >= $this_month_start_iso
                    AND toString(u.createdAt) <= $now_iso
                )
                OR
                (
                    u.tglRegister IS NOT NULL
                    AND toString(u.tglRegister) >= $this_month_start_iso
                    AND toString(u.tglRegister) <= $now_iso
                )
                OR
                (
                    u.registerDate IS NOT NULL
                    AND toString(u.registerDate) >= $this_month_start_iso
                    AND toString(u.registerDate) <= $now_iso
                )
            RETURN count(u) AS new_users_this_month
        }

        CALL {
            MATCH (u:FirebaseUser)
            WHERE
                (
                    u.createdAt IS NOT NULL
                    AND u.createdAt >= $last_month_start_epoch
                    AND u.createdAt <= $last_month_end_epoch
                )
                OR
                (
                    u.createdAt IS NOT NULL
                    AND toString(u.createdAt) >= $last_month_start_iso
                    AND toString(u.createdAt) <= $last_month_end_iso
                )
                OR
                (
                    u.tglRegister IS NOT NULL
                    AND toString(u.tglRegister) >= $last_month_start_iso
                    AND toString(u.tglRegister) <= $last_month_end_iso
                )
                OR
                (
                    u.registerDate IS NOT NULL
                    AND toString(u.registerDate) >= $last_month_start_iso
                    AND toString(u.registerDate) <= $last_month_end_iso
                )
            RETURN count(u) AS new_users_last_month
        }

        CALL {
            MATCH (p:FirebaseInfoss)
            WHERE p.isDeleted = false OR p.isDeleted IS NULL
            RETURN count(p) AS total_posts
        }

        CALL {
            MATCH (p:FirebaseInfoss)
            WHERE (p.isDeleted = false OR p.isDeleted IS NULL)
              AND (
                    (
                        p.createdAt IS NOT NULL
                        AND p.createdAt >= $last_30_days_epoch
                        AND p.createdAt <= $now_epoch
                    )
                    OR
                    (
                        p.uploadDate IS NOT NULL
                        AND toString(p.uploadDate) >= $last_30_days_iso
                        AND toString(p.uploadDate) <= $now_iso
                    )
                    OR
                    (
                        p.createdAt IS NOT NULL
                        AND toString(p.createdAt) >= $last_30_days_iso
                        AND toString(p.createdAt) <= $now_iso
                    )
              )
            RETURN count(p) AS new_30_days_posts
        }

        CALL {
            MATCH (p:FirebaseKawanSS)
            WHERE p.isDeleted = false OR p.isDeleted IS NULL
            RETURN count(p) AS total_kawan_ss
        }

        CALL {
            MATCH (p:FirebaseKawanSS)
            WHERE (p.isDeleted = false OR p.isDeleted IS NULL)
              AND (
                    (
                        p.createdAt IS NOT NULL
                        AND p.createdAt >= $last_30_days_epoch
                        AND p.createdAt <= $now_epoch
                    )
                    OR
                    (
                        p.createdAt IS NOT NULL
                        AND toString(p.createdAt) >= $last_30_days_iso
                        AND toString(p.createdAt) <= $now_iso
                    )
              )
            RETURN count(p) AS new_30_days_kawanss
        }

        CALL {
            MATCH (p:InstagramPost)
            RETURN count(p) AS total_instagram_posts
        }

        CALL {
            MATCH (p:InstagramPost)
            WHERE p.timestamp IS NOT NULL
              AND p.timestamp >= $last_30_days_ig
              AND p.timestamp <= $now_ig
            RETURN count(p) AS new_30_days_instagram_posts
        }

        CALL {
            MATCH (u:InstagramUser)
            RETURN count(u) AS total_instagram_users
        }

        CALL {
            MATCH (c)
            WHERE c:FirebaseKawanSSComment OR c:FirebaseInfossComment
            RETURN count(c) AS total_app_comments
        }

        CALL {
            MATCH (c:InstagramComment)
            RETURN count(c) AS total_instagram_comments
        }

        CALL {
            MATCH ()-[r]->()
            WHERE type(r) IN ['LIKES_KAWAN_FB', 'LIKES_INFO_FB']
            RETURN count(r) AS total_app_likes
        }

        CALL {
            MATCH ()-[r]->()
            WHERE type(r) IN [
                'POSTED_FB',
                'WROTE_FB',
                'COMMENTED_ON_FB',
                'LIKES_KAWAN_FB',
                'LIKES_INFO_FB'
            ]
            RETURN count(r) AS total_app_interactions
        }

        CALL {
            MATCH ()-[r]->()
            WHERE type(r) IN [
                'POSTED_IG',
                'WROTE_IG',
                'COMMENTED_ON_IG',
                'REPLIED_TO_IG'
            ]
            RETURN count(r) AS total_instagram_interactions
        }

        RETURN total_users,
               new_users_this_month,
               new_users_last_month,
               total_posts,
               new_30_days_posts,
               total_kawan_ss,
               new_30_days_kawanss,
               total_instagram_posts,
               new_30_days_instagram_posts,
               total_instagram_users,
               total_app_comments,
               total_instagram_comments,
               total_app_likes,
               total_app_interactions,
               total_instagram_interactions
        """

        with neo4j_driver.session() as session:
            result = session.run(
                query,
                this_month_start_epoch=this_month_start_epoch,
                now_epoch=now_epoch,
                last_month_start_epoch=last_month_start_epoch,
                last_month_end_epoch=last_month_end_epoch,
                last_30_days_epoch=last_30_days_epoch,
                this_month_start_iso=this_month_start_iso,
                now_iso=now_iso,
                last_month_start_iso=last_month_start_iso,
                last_month_end_iso=last_month_end_iso,
                last_30_days_iso=last_30_days_iso,
                this_month_start_ig=this_month_start_ig,
                now_ig=now_ig,
                last_30_days_ig=last_30_days_ig,
            ).single()

        if not result:
            return {
                "status": "success",
                "data": {
                    "users": {
                        "total": 0,
                        "new_this_month": 0,
                        "new_last_month": 0,
                        "growth_percentage": 0.0
                    },
                    "posts": {
                        "total": 0,
                        "new_30_days": 0,
                        "total_kawn_ss": 0,
                        "new_30_days_kawanss": 0
                    },
                    "monthly_report": {}
                }
            }

        total_users = int(result.get("total_users", 0))
        new_users_this_month = int(result.get("new_users_this_month", 0))
        new_users_last_month = int(result.get("new_users_last_month", 0))

        total_posts = int(result.get("total_posts", 0))
        new_30_days_posts = int(result.get("new_30_days_posts", 0))

        total_kawan_ss = int(result.get("total_kawan_ss", 0))
        new_30_days_kawanss = int(result.get("new_30_days_kawanss", 0))

        total_instagram_posts = int(result.get("total_instagram_posts", 0))
        new_30_days_instagram_posts = int(result.get("new_30_days_instagram_posts", 0))

        total_instagram_users = int(result.get("total_instagram_users", 0))

        total_app_comments = int(result.get("total_app_comments", 0))
        total_instagram_comments = int(result.get("total_instagram_comments", 0))

        total_app_likes = int(result.get("total_app_likes", 0))

        total_app_interactions = int(result.get("total_app_interactions", 0))
        total_instagram_interactions = int(result.get("total_instagram_interactions", 0))

        if new_users_last_month > 0:
            growth_percentage = round(
                ((new_users_this_month - new_users_last_month) / new_users_last_month) * 100,
                2
            )
        elif new_users_this_month > 0:
            growth_percentage = 100.0
        else:
            growth_percentage = 0.0

        total_comments = total_app_comments + total_instagram_comments
        total_interactions = total_app_interactions + total_instagram_interactions
        total_all_posts = total_posts + total_kawan_ss + total_instagram_posts

        return {
            "status": "success",
            "data": {
                "users": {
                    "total": total_users,
                    "new_this_month": new_users_this_month,
                    "new_last_month": new_users_last_month,
                    "growth_percentage": growth_percentage
                },
                "posts": {
                    "total": total_posts,
                    "new_30_days": new_30_days_posts,
                    "total_kawn_ss": total_kawan_ss,
                    "new_30_days_kawanss": new_30_days_kawanss
                },
                "monthly_report": {
                    "period": {
                        "this_month_start": this_month_start.strftime("%Y-%m-%d"),
                        "last_month_start": last_month_start.strftime("%Y-%m-%d"),
                        "last_month_end": last_month_end.strftime("%Y-%m-%d"),
                        "last_30_days_start": last_30_days_start.strftime("%Y-%m-%d"),
                        "generated_at": now.strftime("%Y-%m-%d %H:%M:%S")
                    },
                    "users": {
                        "app_total": total_users,
                        "instagram_total": total_instagram_users,
                        "combined_total": total_users + total_instagram_users,
                        "new_this_month": new_users_this_month,
                        "new_last_month": new_users_last_month,
                        "growth_percentage": growth_percentage
                    },
                    "posts": {
                        "infoss_total": total_posts,
                        "infoss_new_30_days": new_30_days_posts,
                        "kawan_ss_total": total_kawan_ss,
                        "kawan_ss_new_30_days": new_30_days_kawanss,
                        "instagram_total": total_instagram_posts,
                        "instagram_new_30_days": new_30_days_instagram_posts,
                        "combined_total": total_all_posts
                    },
                    "comments": {
                        "app_total": total_app_comments,
                        "instagram_total": total_instagram_comments,
                        "combined_total": total_comments
                    },
                    "likes": {
                        "app_total": total_app_likes
                    },
                    "interactions": {
                        "app_total": total_app_interactions,
                        "instagram_total": total_instagram_interactions,
                        "combined_total": total_interactions
                    },
                    "summary_cards": [
                        {
                            "title": "Total Users",
                            "value": total_users + total_instagram_users,
                            "description": "Total pengguna aplikasi dan Instagram yang tersimpan dalam Neo4j"
                        },
                        {
                            "title": "New Users This Month",
                            "value": new_users_this_month,
                            "description": "Jumlah pengguna aplikasi baru pada bulan berjalan"
                        },
                        {
                            "title": "Total Posts",
                            "value": total_all_posts,
                            "description": "Total konten Infoss, Kawan SS, dan Instagram"
                        },
                        {
                            "title": "Total Interactions",
                            "value": total_interactions,
                            "description": "Total relasi interaksi aplikasi dan Instagram"
                        }
                    ]
                }
            }
        }

    except Exception as e:
        traceback.print_exc()

        return {
            "status": "error",
            "message": str(e),
            "data": {
                "users": {
                    "total": 0,
                    "new_this_month": 0,
                    "new_last_month": 0,
                    "growth_percentage": 0.0
                },
                "posts": {
                    "total": 0,
                    "new_30_days": 0,
                    "total_kawn_ss": 0,
                    "new_30_days_kawanss": 0
                },
                "monthly_report": {}
            }
        }

def _get_first_item(items):
    if isinstance(items, list) and len(items) > 0:
        return items[0]

    return None


def _get_metric_value(item, metric_name: str):
    if not item:
        return 0

    metrics = item.get("metrics", {})

    try:
        return float(metrics.get(metric_name, 0))
    except Exception:
        return 0


def _get_actor_name(item):
    if not item:
        return "-"

    return (
        item.get("label")
        or item.get("username")
        or item.get("name")
        or item.get("id")
        or "-"
    )

import traceback
from fastapi import HTTPException

def _build_network_summary_narrative(source: str, metrics_response: dict):
    data = metrics_response.get("data", {})

    top_centrality = data.get("top_10_centrality", {})
    shortest_path_metrics = data.get("shortest_path_metrics", {})
    clique_metrics = data.get("clique_metrics", {})

    degree_top = _get_first_item(top_centrality.get("degree", []))
    betweenness_top = _get_first_item(top_centrality.get("betweenness", []))
    closeness_top = _get_first_item(top_centrality.get("closeness", []))
    eigenvector_top = _get_first_item(top_centrality.get("eigenvector", []))

    degree_name = _get_actor_name(degree_top)
    betweenness_name = _get_actor_name(betweenness_top)
    closeness_name = _get_actor_name(closeness_top)
    eigenvector_name = _get_actor_name(eigenvector_top)

    degree_value = _get_metric_value(degree_top, "degree")
    betweenness_value = _get_metric_value(betweenness_top, "betweenness")
    closeness_value = _get_metric_value(closeness_top, "closeness")
    eigenvector_value = _get_metric_value(eigenvector_top, "eigenvector")

    global_averages = shortest_path_metrics.get("global_averages", {})
    average_distance = global_averages.get("average_of_average_degrees", 0)
    average_diameter = global_averages.get("average_of_network_diameters", 0)
    average_connected_users = global_averages.get("average_of_connected_users", 0)

    clique_global = clique_metrics.get("global_metrics", {})
    total_cliques = clique_global.get("total_cliques", 0)
    largest_clique_size = clique_global.get("largest_clique_size", 0)

    top_paths = shortest_path_metrics.get("top_10_paths", [])

    source_label = "Instagram" if source == "instagram" else "Aplikasi Suara Surabaya"

    # --- KONDISI DATA KOSONG / SEPI ---
    if not degree_top and not betweenness_top and not closeness_top and not eigenvector_top:
        return {
            "overview": f"Aktivitas di {source_label} masih terlalu sepi untuk dianalisis.",
            "degree_centrality": "Pusat Keramaian: Belum ada pengguna yang dominan berinteraksi.",
            "betweenness_centrality": "Jembatan Penghubung: Belum ada tokoh yang menghubungkan antar kelompok.",
            "closeness_centrality": "Penyebar Info: Jarak koneksi masih terlalu jauh untuk penyebaran informasi cepat.",
            "eigenvector_centrality": "Sosok Berpengaruh: Belum terbentuk lingkaran pengguna penting.",
            "geodesic_path": "Rantai Informasi: Jalur komunikasi masih terputus-putus.",
            "clique": "Komunitas (Sirkel): Belum ada kelompok ('sirkel') pengguna yang saling terhubung rapat.",
            "conclusion": "Kesimpulan: Jaringan belum memiliki pola yang jelas karena minimnya interaksi.",
        }

    # --- KONDISI DATA TERSEDIA (SINGKAT & JELAS) ---
    overview = f"Ringkasan pola interaksi pengguna di {source_label}:"

    degree_summary = (
        f"Pusat Keramaian (Degree): {degree_name} (skor: {degree_value}) adalah pengguna paling aktif berinteraksi "
        f"dan paling banyak mendapat respons."
    )

    betweenness_summary = (
        f"Jembatan Penghubung (Betweenness): {betweenness_name} (skor: {betweenness_value}) sangat penting untuk "
        f"menghubungkan berbagai kelompok pengguna yang berbeda agar informasi tidak terputus."
    )

    closeness_summary = (
        f"Penyebar Info Tercepat (Closeness): {closeness_name} (skor: {closeness_value}) memiliki posisi paling strategis "
        f"untuk menyebarkan informasi ke seluruh jaringan dengan rute tersingkat."
    )

    eigenvector_summary = (
        f"Sosok Paling Berpengaruh (Eigenvector): {eigenvector_name} (skor: {eigenvector_value}) adalah tokoh utama "
        f"karena ia terkoneksi langsung dengan pengguna-pengguna penting lainnya."
    )

    if top_paths:
        geodesic_summary = (
            f"Rantai Informasi (Geodesic): Sebuah informasi rata-rata hanya butuh melewati {average_distance} orang "
            f"untuk menyebar luas, dengan batas maksimal {average_diameter} orang."
        )
    else:
        geodesic_summary = "Rantai Informasi (Geodesic): Koneksi belum membentuk rantai yang utuh untuk diukur kecepatannya."

    if total_cliques > 0:
        clique_summary = (
            f"Komunitas (Sirkel): Terdapat {total_cliques} kelompok ('sirkel') akrab, dengan sirkel terbesar "
            f"berisi {largest_clique_size} orang yang saling berinteraksi penuh."
        )
    else:
        clique_summary = "Komunitas (Sirkel): Belum ditemukan komunitas kecil yang anggotanya saling berinteraksi penuh."

    conclusion = (
        f"Kesimpulan: Ekosistem {source_label} digerakkan oleh {degree_name} yang memimpin keramaian, {betweenness_name} "
        f"yang menjaga koneksi antar kelompok, {closeness_name} yang mempercepat arus info, dan {eigenvector_name} sebagai "
        f"pemegang pengaruh terkuat."
    )

    return {
        "overview": overview,
        "degree_centrality": degree_summary,
        "betweenness_centrality": betweenness_summary,
        "closeness_centrality": closeness_summary,
        "eigenvector_centrality": eigenvector_summary,
        "geodesic_path": geodesic_summary,
        "clique": clique_summary,
        "conclusion": conclusion,
        "key_findings": {
            "most_connected_actor": {
                "name": degree_name,
                "metric": "degree_centrality",
                "value": degree_value,
            },
            "main_bridge_actor": {
                "name": betweenness_name,
                "metric": "betweenness_centrality",
                "value": betweenness_value,
            },
            "closest_actor": {
                "name": closeness_name,
                "metric": "closeness_centrality",
                "value": closeness_value,
            },
            "most_influential_actor": {
                "name": eigenvector_name,
                "metric": "eigenvector_centrality",
                "value": eigenvector_value,
            },
            "network_distance": {
                "average_distance": average_distance,
                "average_diameter": average_diameter,
                "average_connected_users": average_connected_users,
            },
            "community_structure": {
                "total_cliques": total_cliques,
                "largest_clique_size": largest_clique_size,
            },
        },
    }
    
def get_network_analysis_summary(source: str = "app"):
    try:
        source = (source or "app").strip().lower()

        if source not in ["app", "instagram"]:
            raise HTTPException(
                status_code=400,
                detail="source harus bernilai 'app' atau 'instagram'."
            )

        metrics_response = get_network_metrics_summary(source)

        if metrics_response.get("status") != "success":
            return {
                "status": "error",
                "source_active": source,
                "message": "Gagal membuat summary karena data metrik jaringan tidak berhasil diproses.",
                "detail": metrics_response.get("message"),
            }

        narrative = _build_network_summary_narrative(
            source=source,
            metrics_response=metrics_response,
        )

        return {
            "status": "success",
            "source_active": source,
            "data": {
                "analysis_summary": narrative,
            },
        }

    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()

        return {
            "status": "error",
            "source_active": source,
            "message": str(e),
        }
    data = metrics_response.get("data", {})

    top_centrality = data.get("top_10_centrality", {})
    shortest_path_metrics = data.get("shortest_path_metrics", {})
    clique_metrics = data.get("clique_metrics", {})

    degree_top = _get_first_item(top_centrality.get("degree", []))
    betweenness_top = _get_first_item(top_centrality.get("betweenness", []))
    closeness_top = _get_first_item(top_centrality.get("closeness", []))
    eigenvector_top = _get_first_item(top_centrality.get("eigenvector", []))

    degree_name = _get_actor_name(degree_top)
    betweenness_name = _get_actor_name(betweenness_top)
    closeness_name = _get_actor_name(closeness_top)
    eigenvector_name = _get_actor_name(eigenvector_top)

    degree_value = _get_metric_value(degree_top, "degree")
    betweenness_value = _get_metric_value(betweenness_top, "betweenness")
    closeness_value = _get_metric_value(closeness_top, "closeness")
    eigenvector_value = _get_metric_value(eigenvector_top, "eigenvector")

    global_averages = shortest_path_metrics.get("global_averages", {})
    average_distance = global_averages.get("average_of_average_degrees", 0)
    average_diameter = global_averages.get("average_of_network_diameters", 0)
    average_connected_users = global_averages.get("average_of_connected_users", 0)

    clique_global = clique_metrics.get("global_metrics", {})
    total_cliques = clique_global.get("total_cliques", 0)
    largest_clique_size = clique_global.get("largest_clique_size", 0)

    top_paths = shortest_path_metrics.get("top_10_paths", [])

    source_label = "Instagram" if source == "instagram" else "aplikasi Suara Surabaya"

    if not degree_top and not betweenness_top and not closeness_top and not eigenvector_top:
        return {
            "overview": (
                f"Jaringan sosial pada {source_label} belum memiliki data interaksi yang cukup "
                f"untuk menghasilkan analisis centrality yang kuat. Hal ini dapat terjadi karena "
                f"jumlah interaksi antar pengguna masih sedikit atau data graf belum membentuk "
                f"koneksi yang signifikan."
            ),
            "degree_centrality": (
                "Degree centrality belum dapat menunjukkan aktor dominan karena belum ditemukan "
                "pengguna dengan koneksi langsung yang cukup kuat."
            ),
            "betweenness_centrality": (
                "Betweenness centrality belum menunjukkan aktor penghubung utama karena struktur "
                "jaringan belum memiliki jalur penghubung yang jelas antar kelompok pengguna."
            ),
            "closeness_centrality": (
                "Closeness centrality belum dapat menunjukkan aktor yang paling dekat dengan aktor "
                "lain karena konektivitas jaringan masih terbatas."
            ),
            "eigenvector_centrality": (
                "Eigenvector centrality belum dapat menunjukkan aktor berpengaruh karena belum "
                "terdapat pola hubungan yang cukup kuat dengan aktor penting lainnya."
            ),
            "geodesic_path": (
                "Geodesic path belum dapat dianalisis secara optimal karena jalur antar aktor dalam "
                "jaringan masih terbatas."
            ),
            "clique": (
                "Clique belum terdeteksi. Artinya, belum ditemukan kelompok pengguna yang saling "
                "terhubung secara langsung dalam bentuk sub-jaringan yang rapat."
            ),
            "conclusion": (
                f"Secara umum, jaringan sosial pada {source_label} masih belum cukup padat untuk "
                f"menunjukkan pola pengaruh, aktor kunci, atau komunitas yang kuat."
            ),
        }

    overview = (
        f"Berdasarkan hasil analisis jaringan sosial pada {source_label}, terlihat bahwa struktur "
        f"jaringan terbentuk dari aktivitas interaksi antar pengguna. Interaksi tersebut membentuk "
        f"pola hubungan yang dapat dianalisis melalui degree centrality, betweenness centrality, "
        f"closeness centrality, eigenvector centrality, geodesic path, dan clique."
    )

    degree_summary = (
        f"Pada metrik degree centrality, aktor dengan nilai tertinggi adalah {degree_name} "
        f"dengan nilai {degree_value}. Hal ini menunjukkan bahwa aktor tersebut memiliki jumlah "
        f"koneksi langsung paling tinggi dibandingkan pengguna lain. Dalam konteks jaringan sosial, "
        f"aktor ini dapat dianggap sebagai pengguna yang aktif atau populer karena banyak terlibat "
        f"dalam interaksi langsung."
    )

    betweenness_summary = (
        f"Pada metrik betweenness centrality, aktor dengan nilai tertinggi adalah {betweenness_name} "
        f"dengan nilai {betweenness_value}. Nilai ini menunjukkan bahwa aktor tersebut memiliki peran "
        f"sebagai penghubung antar pengguna atau antar kelompok dalam jaringan. Semakin tinggi nilai "
        f"betweenness, semakin besar kemungkinan aktor tersebut menjadi perantara dalam penyebaran "
        f"informasi."
    )

    closeness_summary = (
        f"Pada metrik closeness centrality, aktor dengan nilai tertinggi adalah {closeness_name} "
        f"dengan nilai {closeness_value}. Hal ini menunjukkan bahwa aktor tersebut memiliki jarak "
        f"yang relatif lebih dekat ke aktor-aktor lain dalam jaringan. Aktor dengan closeness tinggi "
        f"berpotensi menjangkau pengguna lain secara lebih cepat."
    )

    eigenvector_summary = (
        f"Pada metrik eigenvector centrality, aktor dengan nilai tertinggi adalah {eigenvector_name} "
        f"dengan nilai {eigenvector_value}. Metrik ini menunjukkan bahwa aktor tersebut tidak hanya "
        f"memiliki banyak koneksi, tetapi juga terhubung dengan aktor lain yang memiliki pengaruh "
        f"tinggi. Dengan demikian, aktor ini dapat dipandang sebagai salah satu aktor paling berpengaruh "
        f"dalam struktur jaringan."
    )

    if top_paths:
        geodesic_summary = (
            f"Analisis geodesic path menunjukkan bahwa rata-rata jarak sosial dalam jaringan adalah "
            f"sekitar {average_distance} langkah, dengan rata-rata diameter jaringan sebesar "
            f"{average_diameter}. Nilai ini menggambarkan seberapa jauh pengguna harus melewati "
            f"aktor lain untuk dapat terhubung. Semakin kecil jarak geodesic, semakin cepat informasi "
            f"dapat menyebar di dalam jaringan."
        )
    else:
        geodesic_summary = (
            "Analisis geodesic path belum menemukan sampel jalur yang cukup kuat antar aktor. "
            "Hal ini dapat menunjukkan bahwa jaringan masih terfragmentasi atau sebagian besar "
            "pengguna belum saling terhubung secara langsung maupun tidak langsung."
        )

    if total_cliques > 0:
        clique_summary = (
            f"Pada analisis clique, ditemukan {total_cliques} kelompok clique dalam jaringan. "
            f"Clique terbesar memiliki {largest_clique_size} anggota. Hal ini menunjukkan adanya "
            f"sub-kelompok pengguna yang saling terhubung secara langsung. Kelompok seperti ini "
            f"dapat menjadi indikasi adanya komunitas kecil yang memiliki intensitas interaksi tinggi."
        )
    else:
        clique_summary = (
            "Pada analisis clique, belum ditemukan kelompok pengguna yang membentuk hubungan saling "
            "terhubung secara penuh. Hal ini menunjukkan bahwa interaksi pengguna belum membentuk "
            "sub-komunitas yang benar-benar rapat."
        )

    conclusion = (
        f"Secara keseluruhan, jaringan sosial pada {source_label} menunjukkan bahwa aktor dengan "
        f"koneksi langsung tinggi dapat diidentifikasi melalui degree centrality, aktor penghubung "
        f"dapat dilihat melalui betweenness centrality, aktor yang paling mudah menjangkau jaringan "
        f"dapat dilihat melalui closeness centrality, dan aktor berpengaruh dapat dilihat melalui "
        f"eigenvector centrality. Hasil ini dapat digunakan untuk memahami pola interaksi, menemukan "
        f"aktor kunci, serta melihat potensi terbentuknya komunitas dalam jaringan sosial."
    )

    return {
        "overview": overview,
        "degree_centrality": degree_summary,
        "betweenness_centrality": betweenness_summary,
        "closeness_centrality": closeness_summary,
        "eigenvector_centrality": eigenvector_summary,
        "geodesic_path": geodesic_summary,
        "clique": clique_summary,
        "conclusion": conclusion,
        "key_findings": {
            "most_connected_actor": {
                "name": degree_name,
                "metric": "degree_centrality",
                "value": degree_value,
            },
            "main_bridge_actor": {
                "name": betweenness_name,
                "metric": "betweenness_centrality",
                "value": betweenness_value,
            },
            "closest_actor": {
                "name": closeness_name,
                "metric": "closeness_centrality",
                "value": closeness_value,
            },
            "most_influential_actor": {
                "name": eigenvector_name,
                "metric": "eigenvector_centrality",
                "value": eigenvector_value,
            },
            "network_distance": {
                "average_distance": average_distance,
                "average_diameter": average_diameter,
                "average_connected_users": average_connected_users,
            },
            "community_structure": {
                "total_cliques": total_cliques,
                "largest_clique_size": largest_clique_size,
            },
        },
    }



    try:
        source = (source or "app").strip().lower()

        if source not in ["app", "instagram"]:
            raise HTTPException(
                status_code=400,
                detail="source harus bernilai 'app' atau 'instagram'."
            )

        metrics_response = get_network_metrics_summary(source)

        if metrics_response.get("status") != "success":
            return {
                "status": "error",
                "source_active": source,
                "message": "Gagal membuat summary karena data metrik jaringan tidak berhasil diproses.",
                "detail": metrics_response.get("message"),
            }

        narrative = _build_network_summary_narrative(
            source=source,
            metrics_response=metrics_response,
        )

        return {
            "status": "success",
            "source_active": source,
            "data": {
                "analysis_summary": narrative,
            },
        }

    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()

        return {
            "status": "error",
            "source_active": source,
            "message": str(e),
        }