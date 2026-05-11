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
from google.analytics.data_v1beta.types import Dimension, Metric, RunRealtimeReportRequest, MinuteRange
from google.oauth2 import service_account
from app import config
from app.utils.sna_filter_utils import is_ignored_app_user, is_ignored_instagram_user, normalize_hashtag, is_ignored_hashtag
from google.analytics.data_v1beta.types import (
    RunRealtimeReportRequest, 
    Metric, 
    FilterExpression, 
    Filter
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

def get_live_analytics_summary():
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
                        # Filter data untuk 0, 1, 2, 3, dan 4 menit yang lalu
                        values=["00", "01", "02", "03", "04"] 
                    )
                )
            )
        )
        response_5 = client.run_realtime_report(request_5)

        active_users_5_min = 0
        if response_5.rows:
            active_users_5_min = int(response_5.rows[0].metric_values[0].value)

        # Konversi ke string atau kembalikan "Tidak ada" sesuai instruksi
        val_30_min = str(active_users_30_min) if active_users_30_min > 0 else "Tidak ada"
        val_5_min = str(active_users_5_min) if active_users_5_min > 0 else "Tidak ada"

        return {
            "status": "success",
            "data": {
                "integrations": {
                    "google_analytics": {
                        "status": "connected",
                        "active_users_last_30_min": val_30_min,
                        "active_users_last_5_min": val_5_min
                    }
                }
            }
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

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