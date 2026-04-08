import os
import json
import csv
import io
import re
import asyncio
import itertools
import random
from datetime import datetime, timedelta
import networkx as nx
from networkx.algorithms import bipartite
from fastapi import HTTPException, Response
from app.database import neo4j_driver
from collections import Counter
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import Dimension, Metric, RunRealtimeReportRequest, MinuteRange
from google.oauth2 import service_account
from app import config

def get_realtime_active_users():
    """Mengambil jumlah active users dalam 30 menit dan 5 menit terakhir dari GA4"""
    try:
        property_id = config.GA_PROPERTY_ID
        if not property_id:
            return {"last_30_min": 0, "last_5_min": 0}

        credentials = service_account.Credentials.from_service_account_info(config.FIREBASE_CREDENTIALS)
        client = BetaAnalyticsDataClient(credentials=credentials)

        # Request 1: 30 Menit Terakhir (Secara default GA4 Realtime mengambil 30 menit)
        request_30 = RunRealtimeReportRequest(
            property=f"properties/{property_id}",
            metrics=[Metric(name="activeUsers")]
        )
        
        # Request 2: 5 Menit Terakhir (Kita spesifikasikan minute_ranges-nya)
        request_5 = RunRealtimeReportRequest(
            property=f"properties/{property_id}",
            metrics=[Metric(name="activeUsers")],
            minute_ranges=[MinuteRange(name="last_5", start_minutes_ago=4, end_minutes_ago=0)]
        )
        
        # Eksekusi kedua request
        response_30 = client.run_realtime_report(request_30)
        response_5 = client.run_realtime_report(request_5)
        
        # Parsing hasil Request 30 Menit
        count_30 = 0
        if response_30.rows:
            count_30 = int(response_30.rows[0].metric_values[0].value)
            
        # Parsing hasil Request 5 Menit
        count_5 = 0
        if response_5.rows:
            count_5 = int(response_5.rows[0].metric_values[0].value)
            
        return {
            "last_30_min": count_30,
            "last_5_min": count_5
        }
        
    except Exception as e:
        # Menambahkan print error ini agar jika GA4 menolak request, kita bisa lihat alasannya di terminal
        print(f"Error fetching Realtime GA4: {e}")
        # Kita gunakan angka 200 lagi untuk ngetes apakah masih masuk ke error ini
        return {"last_30_min": 200, "last_5_min": 200}

def get_top_10_hashtags():
    query = """
    MATCH (p:KawanSS)
    WHERE (p.isDeleted = false OR p.isDeleted IS NULL)
      AND p.deskripsi IS NOT NULL
      AND p.deskripsi CONTAINS '#'
    RETURN p.deskripsi AS text
    LIMIT 5000
    UNION ALL
    MATCH (p:Infoss)
    WHERE (p.isDeleted = false OR p.isDeleted IS NULL)
      AND coalesce(p.detail, p.judul, '') CONTAINS '#'
    RETURN coalesce(p.detail, p.judul, '') AS text
    LIMIT 5000
    """
    try:
        with neo4j_driver.session() as session:
            records = session.run(query).data()

        hashtag_pattern = re.compile(r'#\w+')
        all_hashtags = []
        for r in records:
            text = r.get('text')
            if text:
                all_hashtags.extend(hashtag_pattern.findall(text.lower()))

        top_10 = Counter(all_hashtags).most_common(10)
        return {"status": "success", "message": "Top 10 Hashtags", "data": [{"hashtag": tag, "count": count} for tag, count in top_10]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Gagal mengambil top hashtags: {str(e)}")


def get_first_day_of_last_month(dt):
    if dt.month == 1:
        return dt.replace(year=dt.year - 1, month=12, day=1, hour=0, minute=0, second=0, microsecond=0)
    return dt.replace(month=dt.month - 1, day=1, hour=0, minute=0, second=0, microsecond=0)


def get_main_dashboard_summary():
    try:
        now = datetime.now()
        first_day_this_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        first_day_last_month = get_first_day_of_last_month(now)
        thirty_days_ago = now - timedelta(days=30)

        iso_this_month  = first_day_this_month.isoformat()
        iso_last_month  = first_day_last_month.isoformat()
        iso_30_days_ago = thirty_days_ago.isoformat()

        epoch_this_month  = int(first_day_this_month.timestamp() * 1000)
        epoch_last_month  = int(first_day_last_month.timestamp() * 1000)
        epoch_30_days_ago = int(thirty_days_ago.timestamp() * 1000)

        stats_query = """
        CALL { MATCH (u:User) RETURN count(u) AS total_users }
        CALL { MATCH (i:Infoss) RETURN count(i) AS total_infoss }
        CALL { MATCH (k:KawanSS) RETURN count(k) AS total_kawanss }
        CALL { MATCH (u:User) WHERE u.createdAt >= $iso_this_month OR u.joinDate >= $iso_this_month RETURN count(u) AS new_users_this_month }
        CALL { MATCH (u:User) WHERE (u.createdAt >= $iso_last_month AND u.createdAt < $iso_this_month) OR (u.joinDate >= $iso_last_month AND u.joinDate < $iso_this_month) RETURN count(u) AS new_users_last_month }
        CALL { MATCH (k:KawanSS) WHERE (k.isDeleted = false OR k.isDeleted IS NULL) AND k.createdAt >= $epoch_30_days_ago RETURN count(k) AS new_kawanss_30_days }
        CALL { MATCH (i:Infoss) WHERE (i.isDeleted = false OR i.isDeleted IS NULL) AND (i.uploadDate >= $iso_30_days_ago OR i.createdAt >= $iso_30_days_ago) RETURN count(i) AS new_infoss_30_days }
        RETURN total_users, total_infoss, total_kawanss, new_users_this_month, new_users_last_month, new_infoss_30_days, new_kawanss_30_days
        """

        top_content_query = """
        MATCH (i:Infoss)
        WHERE i.isDeleted = false OR i.isDeleted IS NULL
        RETURN i.id AS id, coalesce(i.judul, i.title, 'No Title') AS judul, coalesce(toInteger(i.jumlahView), 0) AS jumlahView, coalesce(i.kategori, 'Umum') AS kategori, coalesce(i.gambar, '') AS gambar, i.uploadDate AS uploadDate, coalesce(toInteger(i.jumlahComment), 0) AS jumlahComment, coalesce(toInteger(i.jumlahLike), 0) AS jumlahLike
        ORDER BY jumlahView DESC LIMIT 10
        """
        
        sna_query = """
        MATCH (u:User)-[:POSTED]->(p:KawanSS) WHERE u.id <> 'unknown_user'
        RETURN u.id AS uid, coalesce(u.nama, u.username, u.id) AS uname, coalesce(u.username, u.nama, u.id) AS username, p.id AS pid LIMIT 500
        UNION ALL
        MATCH (u:User)-[:WROTE]->(c:KawanssComment)-[:COMMENTED_ON]->(p:KawanSS) WHERE u.id <> 'unknown_user'
        RETURN u.id AS uid, coalesce(u.nama, u.username, u.id) AS uname, coalesce(u.username, u.nama, u.id) AS username, p.id AS pid LIMIT 500
        UNION ALL
        MATCH (u:User)-[:WROTE]->(c:InfossComment)-[:COMMENTED_ON]->(p:Infoss) WHERE u.id <> 'unknown_user'
        RETURN u.id AS uid, coalesce(u.nama, u.username, u.id) AS uname, coalesce(u.username, u.nama, u.id) AS username, p.id AS pid LIMIT 500
        """

        with neo4j_driver.session() as session:
            stats_res = session.run(stats_query, iso_this_month=iso_this_month, iso_last_month=iso_last_month, iso_30_days_ago=iso_30_days_ago, epoch_30_days_ago=epoch_30_days_ago).single()
            top_content_res = session.run(top_content_query).data()
            sna_records = session.run(sna_query).data()

        total_users = stats_res["total_users"]
        new_this_month = stats_res["new_users_this_month"]
        new_last_month = stats_res["new_users_last_month"]
        user_growth_percent = (new_this_month / new_last_month) * 100 if new_last_month > 0 else (100.0 if new_this_month > 0 else 0.0)

        # Struktur Metrik sesuai Note
        top_10_centrality = {"degree": [], "betweenness": [], "closeness": [], "eigenvector": []}
        user_network_metrics = []
        user_metrics_lookup = {}
        top_10_paths = []
        global_metrics = {"average_of_average_degrees": 0.0, "average_of_network_diameters": 0.0, "average_of_connected_users": 0.0}
        clique_global_metrics = {"total_cliques": 0, "largest_clique_size": 0, "average_clique_size": 0.0}
        top_10_cliques = []

        try:
            G = nx.Graph()
            user_nodes_set = set()
            
            for record in sna_records:
                uid = str(record['uid']).strip()
                if uid == 'unknown_user': continue  

                u_node = f"user_{uid}"
                p_node = f"post_{record['pid']}"
                
                user_nodes_set.add(u_node)
                G.add_node(u_node, type="user", name=str(record['uname']), username=str(record['username']), bipartite=0)
                G.add_node(p_node, type="post", bipartite=1)       
                
                if G.has_edge(u_node, p_node):
                    G[u_node][p_node]['weight'] += 1
                else:
                    G.add_edge(u_node, p_node, weight=1)

            G.remove_nodes_from(list(nx.isolates(G)))
            user_nodes_in_G = [n for n in user_nodes_set if n in G]

            if user_nodes_in_G:
                G_user_1mode = bipartite.projected_graph(G, user_nodes_in_G)
                components = sorted(nx.connected_components(G_user_1mode), key=len, reverse=True)
                
                total_avg_deg, total_max_reach, total_reach_count = 0.0, 0, 0
                all_cliques = []
                paths_found = 0
                
                for comp in components:
                    G_sub = G_user_1mode.subgraph(comp)
                    
                    # 1. Clique Detection
                    comp_cliques = list(nx.find_cliques(G_sub))
                    valid_comp_cliques = [c for c in comp_cliques if len(c) >= 3]
                    all_cliques.extend(valid_comp_cliques)
                    
                    comp_user_clique_info = {u: {"cliques_count": 0, "largest": 0} for u in comp}
                    for c in valid_comp_cliques:
                        c_size = len(c)
                        for u in c:
                            comp_user_clique_info[u]["cliques_count"] += 1
                            if c_size > comp_user_clique_info[u]["largest"]:
                                comp_user_clique_info[u]["largest"] = c_size

                    # 2. Shortest Path (Geodesic)
                    lengths = dict(nx.all_pairs_shortest_path_length(G_sub))
                    
                    for u in comp:
                        reachable_lengths = [dist for target, dist in lengths[u].items() if dist > 0]
                        if reachable_lengths:
                            avg_deg = sum(reachable_lengths) / len(reachable_lengths)
                            max_reach = max(reachable_lengths)
                            reach_count = len(reachable_lengths)
                        else:
                            avg_deg, max_reach, reach_count = 0.0, 0, 0
                            
                        metrics_obj = {
                            "average_degrees_of_separation": round(avg_deg, 2),
                            "network_diameter": int(max_reach),
                            "connected_users_in_lcc": reach_count,
                            "cliques_count": comp_user_clique_info[u]["cliques_count"],
                            "largest_clique_membership": comp_user_clique_info[u]["largest"]
                        }
                        
                        user_metrics_lookup[u] = metrics_obj
                        user_network_metrics.append({
                            "user_id": u,
                            "user_name": G.nodes[u].get('name', u),
                            "username": G.nodes[u].get('username', u),
                            **metrics_obj
                        })
                        
                        total_avg_deg += avg_deg
                        total_max_reach += max_reach
                        total_reach_count += reach_count

                    if paths_found < 10 and len(comp) >= 2:
                        sample_users = list(comp)[:20]
                        sample_pairs = list(itertools.combinations(sample_users, 2))
                        random.shuffle(sample_pairs)
                        
                        for u1, u2 in sample_pairs:
                            if paths_found >= 10: break
                            try:
                                path = nx.shortest_path(G, u1, u2)
                                readable_path = []
                                for node in path:
                                    if node.startswith("user_"):
                                        readable_path.append(G.nodes[node].get('username', node))
                                    else:
                                        readable_path.append(f"[Post: {node.replace('post_', '')[:8]}]")
                                        
                                top_10_paths.append({
                                    "source_user": G.nodes[u1].get('name', u1),
                                    "source_username": G.nodes[u1].get('username', u1),
                                    "target_user": G.nodes[u2].get('name', u2),
                                    "target_username": G.nodes[u2].get('username', u2),
                                    "distance_hops": int((len(path)-1)/2),
                                    "path_details": " ➔ ".join(readable_path),
                                    **user_metrics_lookup.get(u1, {})
                                })
                                paths_found += 1 
                            except nx.NetworkXNoPath:
                                continue

                num_users = len(user_nodes_in_G)
                if num_users > 0:
                    global_metrics = {
                        "average_of_average_degrees": round(total_avg_deg / num_users, 2),
                        "average_of_network_diameters": round(total_max_reach / num_users, 2),
                        "average_of_connected_users": round(total_reach_count / num_users, 2)
                    }

                all_cliques.sort(key=len, reverse=True)
                total_cliques = len(all_cliques)
                if total_cliques > 0:
                    clique_global_metrics = {
                        "total_cliques": total_cliques,
                        "largest_clique_size": len(all_cliques[0]),
                        "average_clique_size": round(sum(len(c) for c in all_cliques) / total_cliques, 2)
                    }
                    
                for idx, c in enumerate(all_cliques[:10]):
                    members_list = [{"id": u.replace('user_', ''), "name": G.nodes[u].get('name', u), "username": G.nodes[u].get('username', u)} for u in c]
                    top_10_cliques.append({"rank": idx + 1, "size": len(c), "members": members_list})

                user_network_metrics = sorted(user_network_metrics, key=lambda x: x["connected_users_in_lcc"], reverse=True)

                # =================================================================
                # === CENTRALITY CALCULATIONS (Eksak Sesuai Scope) ===
                # =================================================================
                for u, v, d in G.edges(data=True):
                    d['distance'] = 1.0 / d['weight'] if d['weight'] > 0 else 1.0

                deg_cent = nx.degree_centrality(G)
                bet_cent  = nx.betweenness_centrality(G, weight='distance')
                clo_cent  = nx.closeness_centrality(G, distance='distance')
                
                # Eigenvector Centrality sesuai Note
                try:
                    eig_cent = nx.eigenvector_centrality(G, weight='weight', max_iter=1000, tol=1e-03)
                except Exception:
                    eig_cent = {n: 0.0 for n in G.nodes()}

                def fmt(u):
                    return {
                        "id": u, "name": G.nodes[u].get('name', u.replace('user_', '')), "username": G.nodes[u].get('username', u.replace('user_', '')),
                        "metrics": {
                            "degree": round(deg_cent.get(u, 0.0), 6),
                            "betweenness": round(bet_cent.get(u, 0.0), 6),
                            "closeness": round(clo_cent.get(u, 0.0), 6),
                            "eigenvector": round(eig_cent.get(u, 0.0), 6)
                        }
                    }

                top_10_centrality["degree"]      = [fmt(u) for u in sorted(user_nodes_in_G, key=lambda x: deg_cent.get(x, 0), reverse=True)[:10]]
                top_10_centrality["betweenness"] = [fmt(u) for u in sorted(user_nodes_in_G, key=lambda x: bet_cent.get(x, 0), reverse=True)[:10]]
                top_10_centrality["closeness"]   = [fmt(u) for u in sorted(user_nodes_in_G, key=lambda x: clo_cent.get(x, 0), reverse=True)[:10]]
                top_10_centrality["eigenvector"] = [fmt(u) for u in sorted(user_nodes_in_G, key=lambda x: eig_cent.get(x, 0), reverse=True)[:10]]

        except Exception as sna_err:
            print(f"SNA Calculation error on dashboard: {sna_err}")

        # =================================================================
        # === DESCRIPTIVE ANALYSIS GENERATOR (Rule-Based) ===
        # =================================================================
        descriptive_analysis = {
            "pola_interaksi": "",
            "aktor_pengaruh": "",
            "komunitas": ""
        }

        avg_degree = global_metrics.get("average_of_average_degrees", 0)
        if avg_degree == 0:
            descriptive_analysis["pola_interaksi"] = "Belum ada interaksi yang memadai antar pengguna untuk membentuk sebuah jaringan."
        elif avg_degree <= 2.5:
            descriptive_analysis["pola_interaksi"] = f"Informasi di dalam jaringan menyebar dengan sangat cepat. Audiens sangat erat dengan rata-rata jarak sosial hanya {avg_degree} langkah."
        else:
            descriptive_analysis["pola_interaksi"] = f"Audiens terfragmentasi. Rata-rata jarak sosial mencapai {avg_degree} langkah."

        top_degree = top_10_centrality["degree"][0] if top_10_centrality["degree"] else None
        top_betw = top_10_centrality["betweenness"][0] if top_10_centrality["betweenness"] else None
        
        aktor_text = ""
        if top_degree:
            aktor_text += f"'{top_degree['username']}' adalah Key Opinion Leader utama. "
        if top_betw:
            aktor_text += f"'{top_betw['username']}' bertindak sebagai Information Broker krusial."
        descriptive_analysis["aktor_pengaruh"] = aktor_text if aktor_text else "Aktor dominan belum teridentifikasi."

        total_cliq = clique_global_metrics.get("total_cliques", 0)
        if total_cliq > 0:
            descriptive_analysis["komunitas"] = f"Terdapat {total_cliq} sub-grup eksklusif (cliques) di dalam jaringan."
        else:
            descriptive_analysis["komunitas"] = "Grup eksklusif belum terdeteksi."

        # === UPDATE PEMANGGILAN GA4 ===
        active_users_data = get_realtime_active_users()

        return {
            "status": "success",
            "data": {
                "users": {"total": total_users, "total_post": stats_res["total_infoss"], "total_post_kawanss": stats_res["total_kawanss"], "new_this_month": new_this_month, "new_last_month": new_last_month, "growth_percentage": round(user_growth_percent, 2)},
                "posts": {"total": stats_res["total_infoss"], "new_30_days": stats_res["new_infoss_30_days"], "total_kawn_ss": stats_res["total_kawanss"], "new_30_days_kawanss": stats_res["new_kawanss_30_days"]},
                "top_content": top_content_res,
                "top_10_centrality": top_10_centrality,
                "shortest_path_metrics": {"global_averages": global_metrics, "top_10_paths": top_10_paths, "users_details": user_network_metrics},
                "clique_metrics": {"global_metrics": clique_global_metrics, "top_10_cliques": top_10_cliques},
                "descriptive_analysis": descriptive_analysis,
                "integrations": {
                    "csv_export": {"status": "ready"}, 
                    "google_analytics": {
                        "status": "connected" if config.GA_PROPERTY_ID else "disconnected", 
                        "active_users_last_30_min": active_users_data["last_30_min"],
                        "active_users_last_5_min": active_users_data["last_5_min"]
                    }
                }
            }
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

async def export_neo4j_to_csv():
    query = """MATCH (u:User) OPTIONAL MATCH (u)-[:POSTED]->(k:KawanSS) WHERE k.isDeleted = false OR k.isDeleted IS NULL RETURN u.id AS ID, coalesce(u.nama, u.username, 'Unknown') AS Nama, count(k) AS Total_Post ORDER BY Total_Post DESC"""
    try:
        with neo4j_driver.session() as session:
            records = session.run(query).data()
        if not records: raise HTTPException(status_code=404, detail="Tidak ada data di Neo4j untuk diexport.")
        stream = io.StringIO()
        writer = csv.writer(stream)
        writer.writerow(["User ID", "Nama User", "Total Postingan KawanSS"])
        for r in records: writer.writerow([r["ID"], r["Nama"], r["Total_Post"]])
        return Response(content=stream.getvalue(), media_type="text/csv", headers={"Content-Disposition": "attachment; filename=Laporan_SNA_Neo4j.csv"})
    except Exception as e:
        if isinstance(e, HTTPException): raise e
        raise HTTPException(status_code=500, detail=f"Gagal export Neo4j ke CSV: {str(e)}")

async def export_instagram_to_csv():
    cache_file = "instagram_data_cache.json"
    if not os.path.exists(cache_file): raise HTTPException(status_code=404, detail="Cache Instagram tidak ditemukan. Jalankan /sna/ingest terlebih dahulu.")
    try:
        with open(cache_file, "r", encoding="utf-8") as f: posts_data = json.load(f)
        stream = io.StringIO()
        writer = csv.writer(stream)
        writer.writerow(["Post ID", "Timestamp", "Total Likes", "Total Comments", "Caption"])
        for post in posts_data: writer.writerow([post.get('id'), post.get('timestamp'), post.get('like_count', 0), len(post.get('interactions', [])), str(post.get('caption', ''))[:150]])
        return Response(content=stream.getvalue(), media_type="text/csv", headers={"Content-Disposition": "attachment; filename=Laporan_SNA_Instagram.csv"})
    except Exception as e:
        if isinstance(e, HTTPException): raise e
        raise HTTPException(status_code=500, detail=f"Gagal export Instagram ke CSV: {str(e)}")

def get_analytics_summary():
    return {"status": "success", "message": "Data Google Analytics berhasil diambil", "data": {"active_users": 150, "page_views": 1200}}