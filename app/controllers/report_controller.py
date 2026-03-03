from firebase_admin import firestore
from google.cloud.firestore import FieldFilter
from datetime import datetime, timedelta
import networkx as nx

db = firestore.client()

def get_main_dashboard_summary():
    """
    Mengambil data statistik internal dari Firestore:
    - User Growth
    - Post Stats
    - Top 10 Content (Menggunakan field 'jumlahView' dan 'judul')
    - Top 10 Centrality (Degree, Betweenness, Closeness, Eigenvector)
    """
    try:
        now = datetime.now()
        
        # --- 1. Statistik Users ---
        users_ref = db.collection('users')
        total_users_snapshot = users_ref.count().get()
        total_users = total_users_snapshot[0][0].value

        first_day_this_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        last_month = first_day_this_month - timedelta(days=1)
        first_day_last_month = last_month.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        
        new_users_this_month = len(users_ref.where(
            filter=FieldFilter('joinDate', '>=', first_day_this_month)
        ).get())
        
        new_users_last_month = len(users_ref.where(
            filter=FieldFilter('joinDate', '>=', first_day_last_month)
        ).where(
            filter=FieldFilter('joinDate', '<', first_day_this_month)
        ).get())
        
        user_growth_percent = 0
        if new_users_last_month > 0:
            user_growth_percent = (new_users_this_month / new_users_last_month) * 100
        else:
            user_growth_percent = 100.0 if new_users_this_month > 0 else 0.0

        # --- 2. Statistik Posts ---
        posts_ref = db.collection('infoss')
        total_posts = posts_ref.count().get()[0][0].value
        
        thirty_days_ago = now - timedelta(days=30)
        new_posts_30d = len(posts_ref.where(
            filter=FieldFilter('uploadDate', '>=', thirty_days_ago)
        ).get())

        top_posts_query = posts_ref.order_by('jumlahView', direction=firestore.Query.DESCENDING).limit(10).stream()

        kawan_ss_post = db.collection('kawanss')

        thirty_days_ago_kawanss = now - timedelta(days=30)
        new_posts_30d_kawanss = len(kawan_ss_post.where(
            filter=FieldFilter('uploadDate', '>=', thirty_days_ago_kawanss)
        ).get())

        total_kawanss = len(kawan_ss_post.get())
        
        top_posts = []
        for doc in top_posts_query:
            data = doc.to_dict()
            top_posts.append({
                "id": doc.id,
                "judul": data.get('judul', data.get('title', 'No Title')),
                "jumlahView": data.get('jumlahView', 0),
                "kategori": data.get('kategori', 'Umum'),
                "gambar": data.get('gambar', ''),
                "uploadDate": data.get('uploadDate').isoformat() if data.get('uploadDate') else None,
                "jumlahComment": data.get('jumlahComment', 0),
                "jumlahLike": data.get('jumlahLike', 0)
            })

        # --- 3. SNA (Top 10 Centrality) Fast Calculation ---
        top_10_centrality = []
        try:
            # Mengambil 500 post terbaru untuk memastikan dashboard tetap loading cepat
            recent_kawanss = kawan_ss_post.order_by('uploadDate', direction=firestore.Query.DESCENDING).limit(500).stream()
            
            G = nx.DiGraph()
            post_map = {}
            valid_posts = []
            
            for doc in recent_kawanss:
                data = doc.to_dict()
                pid = doc.id
                author = data.get('accountName')
                
                if not author or author.strip().lower() == "unknown user":
                    continue
                    
                post_map[pid] = author
                valid_posts.append((pid, data))
                
                u_node = f"user_{author}"
                p_node = f"post_{pid}"
                G.add_node(u_node, type="user", name=author)
                G.add_node(p_node, type="post")
                G.add_edge(u_node, p_node, relation="AUTHORED")
                
            for pid, data in valid_posts:
                reply_to_id = data.get('reply_to_id')
                if reply_to_id and reply_to_id in post_map:
                    G.add_edge(f"post_{pid}", f"post_{reply_to_id}", relation="REPLIED_TO")
                    
            G.remove_nodes_from(list(nx.isolates(G)))
            
            if G.number_of_nodes() > 0:
                deg_cent = nx.degree_centrality(G)
                bet_cent = nx.betweenness_centrality(G)
                clo_cent = nx.closeness_centrality(G)
                try:
                    eig_cent = nx.eigenvector_centrality(G, max_iter=1000)
                except nx.PowerIterationFailedConvergence:
                    eig_cent = {n: 0.0 for n in G.nodes()}
                    
                # Filter hanya node bertipe user
                user_nodes = [n for n, attr in G.nodes(data=True) if attr.get('type') == 'user']
                
                # Urutkan berdasarkan Degree Centrality tertinggi (Top 10)
                top_users = sorted(user_nodes, key=lambda x: deg_cent.get(x, 0.0), reverse=True)[:10]
                
                for u in top_users:
                    top_10_centrality.append({
                        "id": u,
                        "name": G.nodes[u].get('name', str(u).replace('user_', '')),
                        "metrics": {
                            "degree": deg_cent.get(u, 0.0),
                            "betweenness": bet_cent.get(u, 0.0),
                            "closeness": clo_cent.get(u, 0.0),
                            "eigenvector": eig_cent.get(u, 0.0)
                        }
                    })
        except Exception as sna_err:
            print(f"SNA Calculation error on dashboard: {sna_err}")

        # --- 4. Integrations ---
        integrations = {
            "google_sheets": {
                "status": "connected",
                "last_sync": datetime.now().isoformat()
            },
            "google_analytics": {
                "status": "connected",
                "active_users_now": 0
            }
        }

        return {
            "status": "success",
            "data": {
                "users": {
                    "total": total_users,
                    "total_post": total_posts,
                    "total_post_kawanss": total_kawanss,
                    "new_this_month": new_users_this_month,
                    "new_last_month": new_users_last_month,
                    "growth_percentage": round(user_growth_percent, 2),
                },
                "posts": {
                    "total": total_posts,
                    "new_30_days": new_posts_30d,
                    "total_kawn_ss": total_kawanss,
                    "new_30_days_kawanss": new_posts_30d_kawanss
                },
                "top_content": top_posts,
                "top_10_centrality": top_10_centrality, # <--- DATA CENTRALITY DIMASUKKAN DI SINI
                "integrations": integrations
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