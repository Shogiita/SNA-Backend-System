import os
import json
import csv
import io
import re
from datetime import datetime, timedelta
import networkx as nx
from fastapi import HTTPException, Response
from app.database import neo4j_driver
from collections import Counter

def get_top_10_hashtags():
    """
    Mengambil Top 10 Hashtag yang digunakan oleh user dari postingan di Neo4j.
    Disempurnakan sesuai dengan skema Infoss (detail, judul, title) 
    dan KawanSS (deskripsi, title).
    """
    
    query = """
    MATCH (u:User)-[:POSTED|AUTHORED]->(p)
    WHERE p:KawanSS OR p:Infoss
    WITH coalesce(p.detail, p.deskripsi, p.judul, p.title, '') AS text
    WHERE text CONTAINS '#'
    RETURN text
    """
    
    try:
        with neo4j_driver.session() as session:
            records = session.run(query).data()

        all_hashtags = []
        hashtag_pattern = re.compile(r'#\w+')

        for r in records:
            text = r['text']
            if text:
                tags = hashtag_pattern.findall(text.lower())
                all_hashtags.extend(tags)

        top_10 = Counter(all_hashtags).most_common(10)

        result = [{"hashtag": tag, "count": count} for tag, count in top_10]

        return {
            "status": "success",
            "message": "Top 10 Hashtags berhasil diambil",
            "data": result
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Gagal mengambil top hashtags: {str(e)}")

def get_first_day_of_last_month(dt):
    if dt.month == 1:
        return dt.replace(year=dt.year-1, month=12, day=1, hour=0, minute=0, second=0, microsecond=0)
    else:
        return dt.replace(month=dt.month-1, day=1, hour=0, minute=0, second=0, microsecond=0)

def get_main_dashboard_summary():
    """
    Mengambil data statistik dari Neo4j.
    Menghitung metrik Centrality dan menampilkannya dalam 4 kategori Top 10 terpisah.
    """
    try:
        now = datetime.now()
        first_day_this_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        first_day_last_month = get_first_day_of_last_month(now)
        thirty_days_ago = now - timedelta(days=30)

        iso_this_month = first_day_this_month.isoformat()
        iso_last_month = first_day_last_month.isoformat()
        iso_30_days_ago = thirty_days_ago.isoformat()

        stats_query = """
        CALL { MATCH (u:User) RETURN count(u) AS total_users }
        CALL { MATCH (i:Infoss) RETURN count(i) AS total_infoss }
        CALL { MATCH (k:KawanSS) RETURN count(k) AS total_kawanss }
        CALL { MATCH (u_this:User) WHERE u_this.createdAt >= $iso_this_month OR u_this.joinDate >= $iso_this_month RETURN count(u_this) AS new_users_this_month }
        CALL { MATCH (u_last:User) WHERE (u_last.createdAt >= $iso_last_month AND u_last.createdAt < $iso_this_month) OR (u_last.joinDate >= $iso_last_month AND u_last.joinDate < $iso_this_month) RETURN count(u_last) AS new_users_last_month }
        CALL { MATCH (i_30:Infoss) WHERE i_30.uploadDate >= $iso_30_days_ago OR i_30.createdAt >= $iso_30_days_ago RETURN count(i_30) AS new_infoss_30_days }
        CALL { MATCH (k_30:KawanSS) WHERE k_30.uploadDate >= $iso_30_days_ago OR k_30.createdAt >= $iso_30_days_ago RETURN count(k_30) AS new_kawanss_30_days }
        RETURN total_users, total_infoss, total_kawanss, new_users_this_month, new_users_last_month, new_infoss_30_days, new_kawanss_30_days
        """

        top_content_query = """
        MATCH (i:Infoss)
        RETURN i.id AS id, 
               coalesce(i.judul, i.title, 'No Title') AS judul, 
               coalesce(toInteger(i.jumlahView), 0) AS jumlahView, 
               coalesce(i.kategori, 'Umum') AS kategori, 
               coalesce(i.gambar, '') AS gambar, 
               i.uploadDate AS uploadDate, 
               coalesce(toInteger(i.jumlahComment), 0) AS jumlahComment, 
               coalesce(toInteger(i.jumlahLike), 0) AS jumlahLike
        ORDER BY jumlahView DESC
        LIMIT 10
        """

        sna_query = """
        MATCH (u:User)-[r:POSTED|AUTHORED|LIKES_KAWAN|LIKES_INFOSS|COMMENTED_ON|WROTE]->(p)
        RETURN u.id AS uid, coalesce(u.nama, u.username, u.id) AS uname, p.id AS pid
        LIMIT 1000
        """

        with neo4j_driver.session() as session:
            stats_res = session.run(stats_query, 
                iso_this_month=iso_this_month, 
                iso_last_month=iso_last_month, 
                iso_30_days_ago=iso_30_days_ago
            ).single()
            
            top_content_res = session.run(top_content_query).data()
            sna_records = session.run(sna_query).data()

        total_users = stats_res["total_users"]
        new_this_month = stats_res["new_users_this_month"]
        new_last_month = stats_res["new_users_last_month"]
        
        user_growth_percent = 0.0
        if new_last_month > 0:
            user_growth_percent = (new_this_month / new_last_month) * 100
        else:
            user_growth_percent = 100.0 if new_this_month > 0 else 0.0

        top_10_centrality = {
            "degree": [],
            "betweenness": [],
            "closeness": [],
            "eigenvector": []
        }
        
        try:
            G = nx.Graph()
            
            for record in sna_records:
                u_node = f"user_{record['uid']}"
                p_node = f"post_{record['pid']}"
                
                G.add_node(u_node, type="user", name=record['uname'])
                G.add_node(p_node, type="post")
                
                if G.has_edge(u_node, p_node):
                    G[u_node][p_node]['weight'] += 1
                else:
                    G.add_edge(u_node, p_node, weight=1)

            G.remove_nodes_from(list(nx.isolates(G)))

            if G.number_of_nodes() > 0:
                deg_cent = nx.degree_centrality(G)
                
                k_samples = min(100, G.number_of_nodes())
                bet_cent = nx.betweenness_centrality(G, weight='weight', k=k_samples)
                
                clo_cent = nx.closeness_centrality(G)
                
                try:
                    eig_cent = nx.eigenvector_centrality(G, weight='weight', max_iter=500)
                except Exception:
                    eig_cent = nx.pagerank(G, weight='weight')

                user_nodes = [n for n, attr in G.nodes(data=True) if attr.get('type') == 'user']
                
                def format_node(u):
                    return {
                        "id": u,
                        "name": G.nodes[u].get('name', str(u).replace('user_', '')),
                        "metrics": {
                            "degree": deg_cent.get(u, 0.0),
                            "betweenness": bet_cent.get(u, 0.0),
                            "closeness": clo_cent.get(u, 0.0),
                            "eigenvector": eig_cent.get(u, 0.0) 
                        }
                    }
                
                sorted_by_degree = sorted(user_nodes, key=lambda x: (deg_cent.get(x, 0.0), bet_cent.get(x, 0.0)), reverse=True)[:10]
                sorted_by_betweenness = sorted(user_nodes, key=lambda x: (bet_cent.get(x, 0.0), deg_cent.get(x, 0.0)), reverse=True)[:10]
                sorted_by_closeness = sorted(user_nodes, key=lambda x: (clo_cent.get(x, 0.0), deg_cent.get(x, 0.0)), reverse=True)[:10]
                sorted_by_eigenvector = sorted(user_nodes, key=lambda x: (eig_cent.get(x, 0.0), deg_cent.get(x, 0.0)), reverse=True)[:10]

                top_10_centrality["degree"] = [format_node(u) for u in sorted_by_degree]
                top_10_centrality["betweenness"] = [format_node(u) for u in sorted_by_betweenness]
                top_10_centrality["closeness"] = [format_node(u) for u in sorted_by_closeness]
                top_10_centrality["eigenvector"] = [format_node(u) for u in sorted_by_eigenvector]

        except Exception as sna_err:
            print(f"SNA Calculation error on dashboard: {sna_err}")

        return {
            "status": "success",
            "data": {
                "users": {
                    "total": total_users,
                    "total_post": stats_res["total_infoss"],
                    "total_post_kawanss": stats_res["total_kawanss"],
                    "new_this_month": new_this_month,
                    "new_last_month": new_last_month,
                    "growth_percentage": round(user_growth_percent, 2),
                },
                "posts": {
                    "total": stats_res["total_infoss"],
                    "new_30_days": stats_res["new_infoss_30_days"],
                    "total_kawn_ss": stats_res["total_kawanss"],
                    "new_30_days_kawanss": stats_res["new_kawanss_30_days"]
                },
                "top_content": top_content_res,
                "top_10_centrality": top_10_centrality,
                "integrations": {
                    "csv_export": {
                        "status": "ready"
                    },
                    "google_analytics": {
                        "status": "connected",
                        "active_users_now": 0
                    }
                }
            }
        }
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

async def export_neo4j_to_csv():
    """Mengambil data ringkasan user dari Neo4j dan mengembalikannya sebagai file CSV."""
    query = """
    MATCH (u:User)
    OPTIONAL MATCH (u)-[r:POSTED]->(p)
    RETURN u.id AS ID, u.nama AS Nama, count(r) AS Total_Post
    ORDER BY Total_Post DESC
    """
    try:
        with neo4j_driver.session() as session:
            records = session.run(query).data()
            
        if not records:
            raise HTTPException(status_code=404, detail="Tidak ada data di Neo4j untuk diexport.")

        stream = io.StringIO()
        writer = csv.writer(stream)
        
        writer.writerow(["User ID", "Nama User", "Total Postingan"])
        for r in records:
            writer.writerow([r["ID"], r["Nama"], r["Total_Post"]])
            
        return Response(
            content=stream.getvalue(),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=Laporan_SNA_Neo4j.csv"}
        )

    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=f"Gagal export Neo4j ke CSV: {str(e)}")


async def export_instagram_to_csv():
    """Mengambil data cache Instagram dan mengembalikannya sebagai file CSV."""
    cache_file = "instagram_data_cache.json"
    
    if not os.path.exists(cache_file):
        raise HTTPException(status_code=404, detail="Cache Instagram tidak ditemukan. Jalankan proses ingest terlebih dahulu.")
        
    try:
        with open(cache_file, "r", encoding="utf-8") as f:
            posts_data = json.load(f)

        stream = io.StringIO()
        writer = csv.writer(stream)
        
        writer.writerow(["Post ID", "Timestamp", "Total Likes", "Total Comments", "Caption"])
        for post in posts_data:
            writer.writerow([
                post.get('id'),
                post.get('timestamp'),
                post.get('like_count', 0),
                len(post.get('interactions', [])),
                str(post.get('caption', ''))[:150] 
            ])
            
        return Response(
            content=stream.getvalue(),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=Laporan_SNA_Instagram.csv"}
        )

    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=f"Gagal export Instagram ke CSV: {str(e)}")

def get_analytics_summary():
    """Placeholder untuk data external dari Google Analytics."""
    return {
        "status": "success",
        "message": "Data Google Analytics berhasil diambil",
        "data": {
            "active_users": 150,
            "page_views": 1200
        }
    }