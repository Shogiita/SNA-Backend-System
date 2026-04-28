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
        traceback.print_exc()
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
        import traceback
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
        iso_this_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0).isoformat()
        iso_last_month = get_first_day_of_last_month(now).isoformat()
        iso_30_days_ago = (now - timedelta(days=30)).isoformat()
        epoch_30_days_ago = int((now - timedelta(days=30)).timestamp() * 1000)

        # Query mutlak hanya membaca data Firebase (Suara Surabaya Mobile)
        query = """
        CALL { MATCH (u:FirebaseUser) RETURN count(u) AS total_users }
        CALL { MATCH (i:FirebaseInfoss) RETURN count(i) AS total_infoss }
        CALL { MATCH (k:FirebaseKawanSS) RETURN count(k) AS total_kawanss }
        CALL { MATCH (u:FirebaseUser) WHERE u.createdAt >= $iso_this_month OR u.joinDate >= $iso_this_month RETURN count(u) AS new_users_this_month }
        CALL { MATCH (u:FirebaseUser) WHERE (u.createdAt >= $iso_last_month AND u.createdAt < $iso_this_month) OR (u.joinDate >= $iso_last_month AND u.joinDate < $iso_this_month) RETURN count(u) AS new_users_last_month }
        CALL { MATCH (k:FirebaseKawanSS) WHERE (k.isDeleted = false OR k.isDeleted IS NULL) AND k.createdAt >= $epoch_30_days_ago RETURN count(k) AS new_kawanss_30_days }
        CALL { MATCH (i:FirebaseInfoss) WHERE (i.isDeleted = false OR i.isDeleted IS NULL) AND (i.uploadDate >= $iso_30_days_ago OR i.createdAt >= $iso_30_days_ago) RETURN count(i) AS new_infoss_30_days }
        RETURN total_users, total_infoss, total_kawanss, new_users_this_month, new_users_last_month, new_infoss_30_days, new_kawanss_30_days
        """
        
        with neo4j_driver.session() as session:
            res = session.run(
                query, 
                iso_this_month=iso_this_month, 
                iso_last_month=iso_last_month, 
                iso_30_days_ago=iso_30_days_ago, 
                epoch_30_days_ago=epoch_30_days_ago
            ).single()

        new_this_month = res["new_users_this_month"]
        new_last_month = res["new_users_last_month"]
        growth = (new_this_month / new_last_month) * 100 if new_last_month > 0 else (100.0 if new_this_month > 0 else 0.0)

        return {
            "status": "success", 
            "data": {
                "users": {
                    "total": res["total_users"], 
                    "new_this_month": new_this_month, 
                    "new_last_month": new_last_month, 
                    "growth_percentage": round(growth, 2)
                },
                "posts": {
                    "total": res["total_infoss"], 
                    "new_30_days": res["new_infoss_30_days"], 
                    "total_kawn_ss": res["total_kawanss"], 
                    "new_30_days_kawanss": res["new_kawanss_30_days"]
                }
            }
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"status": "error", "message": str(e)}
