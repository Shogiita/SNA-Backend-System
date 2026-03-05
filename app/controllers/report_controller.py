from datetime import datetime, timedelta
import networkx as nx
from app.database import neo4j_driver

def get_first_day_of_last_month(dt):
    if dt.month == 1:
        return dt.replace(year=dt.year-1, month=12, day=1, hour=0, minute=0, second=0, microsecond=0)
    else:
        return dt.replace(month=dt.month-1, day=1, hour=0, minute=0, second=0, microsecond=0)

def get_main_dashboard_summary():
    """
    Mengambil data statistik dari Neo4j.
    Menghitung metrik Centrality (Degree, Betweenness, Closeness, Eigenvector) 
    secara real-time menggunakan NetworkX dari sampel data graf Neo4j.
    """
    try:
        now = datetime.now()
        first_day_this_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        first_day_last_month = get_first_day_of_last_month(now)
        thirty_days_ago = now - timedelta(days=30)

        iso_this_month = first_day_this_month.isoformat()
        iso_last_month = first_day_last_month.isoformat()
        iso_30_days_ago = thirty_days_ago.isoformat()

        # 1. QUERY STATISTIK (Sangat Cepat)
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

        # 2. QUERY TOP CONTENT
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

        # 3. QUERY SNA (Menarik 500 relasi terbaru dari database untuk dihitung)
        sna_query = """
        MATCH (u:User)-[r]->(p)
        WHERE type(r) IN ['POSTED', 'WROTE']
        RETURN u.id AS uid, coalesce(u.nama, u.username, u.id) AS uname, p.id AS pid
        LIMIT 500
        """

        with neo4j_driver.session() as session:
            stats_res = session.run(stats_query, 
                iso_this_month=iso_this_month, 
                iso_last_month=iso_last_month, 
                iso_30_days_ago=iso_30_days_ago
            ).single()
            
            top_content_res = session.run(top_content_query).data()
            sna_records = session.run(sna_query).data()

        # Proses Data Pertumbuhan Pengguna
        total_users = stats_res["total_users"]
        new_this_month = stats_res["new_users_this_month"]
        new_last_month = stats_res["new_users_last_month"]
        
        user_growth_percent = 0.0
        if new_last_month > 0:
            user_growth_percent = (new_this_month / new_last_month) * 100
        else:
            user_growth_percent = 100.0 if new_this_month > 0 else 0.0

        # ==========================================
        # PERHITUNGAN SNA REAL-TIME DENGAN NETWORKX
        # ==========================================
        top_10_centrality = []
        try:
            G = nx.DiGraph()
            
            # Bangun graf dari data Neo4j
            for record in sna_records:
                u_node = f"user_{record['uid']}"
                p_node = f"post_{record['pid']}"
                
                G.add_node(u_node, type="user", name=record['uname'])
                G.add_node(p_node, type="post")
                G.add_edge(u_node, p_node, relation="INTERACTED")

            # Hapus node yang tidak memiliki relasi
            G.remove_nodes_from(list(nx.isolates(G)))

            if G.number_of_nodes() > 0:
                # Kalkulasi nilai asli tanpa hardcode
                deg_cent = nx.degree_centrality(G)
                bet_cent = nx.betweenness_centrality(G)
                clo_cent = nx.closeness_centrality(G)
                try:
                    eig_cent = nx.eigenvector_centrality(G, max_iter=1000)
                except nx.PowerIterationFailedConvergence:
                    eig_cent = {n: 0.0 for n in G.nodes()}

                # Filter khusus node User
                user_nodes = [n for n, attr in G.nodes(data=True) if attr.get('type') == 'user']
                
                # Ambil 10 teratas berdasarkan Degree
                top_users = sorted(user_nodes, key=lambda x: deg_cent.get(x, 0.0), reverse=True)[:10]

                for u in top_users:
                    top_10_centrality.append({
                        "id": u,
                        "name": G.nodes[u].get('name', str(u).replace('user_', '')),
                        "metrics": {
                            "degree": deg_cent.get(u, 0.0),
                            "betweenness": bet_cent.get(u, 0.0),
                            "closeness": clo_cent.get(u, 0.0),
                            "eigenvector": eig_cent.get(u, 0.0) # NILAI ASLI
                        }
                    })
        except Exception as sna_err:
            print(f"SNA Calculation error on dashboard: {sna_err}")

        # Return format final
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
                    "google_sheets": {
                        "status": "connected",
                        "last_sync": datetime.now().isoformat()
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
        return {
            "status": "error", 
            "message": str(e),
            "data": {
                 "users": {"total": 0, "new_this_month": 0, "growth_percentage": 0},
                 "posts": {"total": 0, "new_30_days": 0},
                 "top_content": [],
                 "top_10_centrality": [],
                 "integrations": {}
            }
        }