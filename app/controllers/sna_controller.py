import requests
import json
import time
import os
import pandas as pd
import networkx as nx
import leidenalg as la
import igraph as ig
import concurrent.futures
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
from dateutil import parser
from pyvis.network import Network
from fastapi import HTTPException
from fastapi.responses import HTMLResponse, Response
from app import config
from app.database import neo4j_driver
import re
from collections import Counter
from fastapi import HTTPException, BackgroundTasks
from app.database import neo4j_driver

CACHE_FILE = "instagram_data_cache.json"
OUTPUT_HTML_DIR = "generated_graphs"

MAX_POSTS_TO_FETCH = 1000
FETCH_MONTHS_BACK = 12
MAX_WORKERS = 10

os.makedirs(OUTPUT_HTML_DIR, exist_ok=True)
session = requests.Session()


session = requests.Session()

def get_instagram_metrics(background_tasks: BackgroundTasks):
    query = """
    MATCH (n:DashboardCache {id: 'instagram_metrics'}) 
    RETURN n.top_posts AS top_posts, n.top_hashtags AS top_hashtags, n.last_updated AS last_updated
    """
    
    result = []
    with neo4j_driver.session() as db_session:
        result = db_session.run(query).data()
        
    current_time = time.time()
    
    if result:
        data = result[0]
        last_updated = data.get("last_updated", 0)
        
        # Load string JSON dari Neo4j kembali menjadi list/dictionary Python
        top_posts = json.loads(data.get("top_posts", "[]"))
        top_hashtags = json.loads(data.get("top_hashtags", "[]"))
        
        # Cek apakah data sudah usang (Lebih dari 15 menit / 900 detik)
        if current_time - last_updated > 900:
            print("[CACHE] Data sudah lebih dari 15 menit. Memicu update background...")
            # Panggil fungsi penarik data IG tanpa menyuruh user menunggu
            background_tasks.add_task(_background_sync_ig_to_neo4j)
            
        return {
            "status": "success",
            "message": "Data berhasil dimuat dari Cache Neo4j.",
            "data": {
                "top_10_posts": top_posts,
                "top_10_hashtags": top_hashtags
            }
        }
    else:
        # Jika Node sama sekali belum ada (Saat pertama kali aplikasi dijalankan seumur hidup)
        print("[CACHE] Data belum ada di Neo4j. Memicu tarikan pertama di background...")
        background_tasks.add_task(_background_sync_ig_to_neo4j)
        return {
            "status": "pending",
            "message": "Data sedang ditarik dari Instagram untuk pertama kalinya. Silakan refresh dalam 1 menit.",
            "data": {
                "top_10_posts": [],
                "top_10_hashtags": []
            }
        }

def _background_sync_ig_to_neo4j():
    """
    Berjalan di latar belakang (Makan waktu ~25 detik).
    Menarik 1000 post, menghitung Top 10, dan menyimpannya ke Node DashboardCache di Neo4j.
    """
    print("[IG SYNC] Memulai penarikan 1000 post dari Instagram...")
    max_posts = 1000
    all_posts = []
    
    url = f"{config.GRAPH_API_URL}/{config.IG_BUSINESS_ACCOUNT_ID}/media"
    params = {
        "access_token": config.IG_ACCESS_TOKEN,
        "fields": "id,caption,permalink,timestamp,like_count,comments_count",
        "limit": 50 
    }
    
    while url and len(all_posts) < max_posts:
        try:
            response = session.get(url, params=params)
            data = response.json()
            
            if 'error' in data: 
                print(f"[IG SYNC ERROR] {data['error']['message']}")
                break
                
            if 'data' in data: 
                all_posts.extend(data['data'])
                
            if 'paging' in data and 'next' in data['paging']:
                url = data['paging']['next']
                params = {} 
            else:
                break 
        except Exception as e:
            print(f"[IG SYNC REQUEST ERROR] {str(e)}")
            break

    # Pangkas jika lebih dari 1000
    all_posts = all_posts[:max_posts]
    
    if not all_posts:
        print("[IG SYNC] Gagal menarik data. Proses update dibatalkan.")
        return

    # ==========================================
    # A. MENGHITUNG TOP 10 POSTS
    # ==========================================
    sorted_posts = sorted(all_posts, key=lambda x: x.get('like_count', 0) + x.get('comments_count', 0), reverse=True)
    top_10_posts = []
    for p in sorted_posts[:10]:
        clean_caption = p.get("caption", "").replace('\n', ' ').replace('\r', ' ')
        preview_caption = clean_caption[:100] + "..." if len(clean_caption) > 100 else clean_caption
        top_10_posts.append({
            "id": p.get("id"),
            "permalink": p.get("permalink", ""),
            "caption": preview_caption,
            "like_count": p.get("like_count", 0),
            "comments_count": p.get("comments_count", 0),
            "total_engagement": p.get("like_count", 0) + p.get("comments_count", 0),
            "timestamp": p.get("timestamp", "")
        })

    # ==========================================
    # B. MENGHITUNG TOP 10 HASHTAGS
    # ==========================================
    all_hashtags = []
    for p in all_posts:
        caption = p.get('caption', '')
        if caption:
            tags = re.findall(r"#(\w+)", caption.lower())
            all_hashtags.extend(tags)
            
    hashtag_counts = Counter(all_hashtags)
    top_10_hashtags = [{"hashtag": f"#{tag}", "count": c} for tag, c in hashtag_counts.most_common(10)]

    # ==========================================
    # C. MENYIMPAN KE NEO4J MENGGUNAKAN MERGE
    # ==========================================
    # Neo4j tidak bisa menyimpan array of JSON secara langsung ke dalam node property.
    # Maka kita harus mengubahnya menjadi String terlebih dahulu menggunakan json.dumps()
    save_query = """
    MERGE (n:DashboardCache {id: 'instagram_metrics'})
    SET n.top_posts = $top_posts,
        n.top_hashtags = $top_hashtags,
        n.last_updated = $last_updated
    """
    
    try:
        with neo4j_driver.session() as db_session:
            db_session.run(
                save_query,
                top_posts=json.dumps(top_10_posts),
                top_hashtags=json.dumps(top_10_hashtags),
                last_updated=time.time() # Menyimpan waktu saat ini (Epoch seconds)
            )
        print("[IG SYNC] SUKSES! Node DashboardCache di Neo4j telah diperbarui.")
    except Exception as e:
        print(f"[IG SYNC NEO4J ERROR] {str(e)}")

def _get_posts_recursive(start_date, end_date, max_posts=MAX_POSTS_TO_FETCH):
    all_posts = []
    url = f"{config.GRAPH_API_URL}/{config.IG_BUSINESS_ACCOUNT_ID}/media"
    params = {
        "access_token": config.IG_ACCESS_TOKEN,
        "fields": "id,caption,media_type,media_url,permalink,timestamp,like_count,comments_count",
        "limit": 50
    }
    
    while url and len(all_posts) < max_posts:
        try:
            response = session.get(url, params=params)
            data = response.json()
            if 'error' in data or 'data' not in data: break
            
            stop_fetching = False
            for post in data['data']:
                post_time = parser.isoparse(post['timestamp'])
                if post_time > end_date: continue
                if post_time < start_date:
                    stop_fetching = True
                    break
                all_posts.append(post)
                if len(all_posts) >= max_posts:
                    stop_fetching = True
                    break
            if stop_fetching: break
            
            if 'paging' in data and 'next' in data['paging']:
                url = data['paging']['next']
                params = {}
            else:
                break
        except Exception:
            break
    return all_posts

def _fetch_comments_and_replies(post):
    post_id = post['id']
    interactions = []
    post_item = {
        "id": post_id, 
        "caption": post.get('caption', ''), 
        "media_url": post.get('media_url', ''),
        "permalink": post.get('permalink', ''), 
        "like_count": post.get('like_count', 0),
        "comments_count": post.get('comments_count', 0),
        "timestamp": post.get('timestamp'), 
        "interactions": []
    }
    
    if post_item["comments_count"] == 0: 
        return post_item

    # Mengambil comment beserta replies-nya sekaligus
    url = f"{config.GRAPH_API_URL}/{post_id}/comments"
    params = {
        "access_token": config.IG_ACCESS_TOKEN, 
        "fields": "id,text,username,like_count,timestamp,replies{id,text,username,like_count,timestamp}", 
        "limit": 50
    }
    
    try:
        resp = session.get(url, params=params)
        data = resp.json()
        
        for comment in data.get('data', []):
            comment_username = comment.get('username', 'Unknown')
            # Tambahkan Data Komentar Utama
            interactions.append({
                "interaction_type": "COMMENT",
                "source_username": comment_username, 
                "target_id": post_id,
                "target_type": "POST",
                "content": comment.get('text', ''), 
                "likes": comment.get('like_count', 0),
                "timestamp": comment.get('timestamp')
            })
            
            # Cek jika ada balasan (replies) pada komentar ini
            if 'replies' in comment:
                for reply in comment['replies'].get('data', []):
                    interactions.append({
                        "interaction_type": "REPLY",
                        "source_username": reply.get('username', 'Unknown'), 
                        "target_id": comment_username, # Reply ditargetkan ke username pembuat komen
                        "target_type": "USER",
                        "content": reply.get('text', ''), 
                        "likes": reply.get('like_count', 0),
                        "timestamp": reply.get('timestamp')
                    })
    except Exception as e:
        print(f"Error fetching comments for {post_id}: {e}")
        
    post_item["interactions"] = interactions
    return post_item

def background_ingestion_task():
    """Fungsi ini berjalan di background agar tidak memblokir response API"""
    try:
        print("[INGESTION] Memulai proses penarikan data dari Instagram...")
        end_date = datetime.now(timezone.utc)
        start_date = end_date - relativedelta(months=FETCH_MONTHS_BACK)
        
        raw_posts = _get_posts_recursive(start_date, end_date, max_posts=MAX_POSTS_TO_FETCH)
        full_dataset = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(_fetch_comments_and_replies, post): post for post in raw_posts}
            for future in concurrent.futures.as_completed(futures):
                try: 
                    full_dataset.append(future.result())
                except Exception as e: 
                    print(f"[INGESTION ERROR] {e}")
                    
        with open(CACHE_FILE, "w", encoding="utf-8") as f: 
            json.dump(full_dataset, f, ensure_ascii=False, indent=2)
            
        print(f"[INGESTION] Selesai. Tersimpan {len(full_dataset)} posts ke cache.")
    except Exception as e:
        print(f"[FATAL ERROR] Ingestion gagal: {e}")

# ==================================================
# DATASET GENERATOR (SNA FORMAT)
# ==================================================
def get_dataset_flat():
    if not os.path.exists(CACHE_FILE): 
        raise HTTPException(status_code=404, detail="Cache belum ada.")
        
    with open(CACHE_FILE, "r", encoding="utf-8") as f: 
        posts_data = json.load(f)
        
    dataset = []
    
    for post in posts_data:
        post_id = post['id']
        post_likes = post.get('like_count', 0)
        post_comments_count = post.get('comments_count', 0)
        
        # Bersihkan caption dari enter agar tidak merusak CSV
        clean_caption = post.get('caption', '').replace('\n', ' ').replace('\r', ' ').replace('"', "'")

        # 1. Masukkan Node Utama (Postingan)
        dataset.append({
            "Source": "Suara_Surabaya_Official",
            "Target": post_id,
            "Interaction_Type": "POST",
            "Post_Like_Count": post_likes,
            "Post_Comment_Count": post_comments_count,
            "Interaction_Like_Count": 0,
            "Content": clean_caption
        })

        # 2. Proses Komentar dan Reply
        for act in post.get('interactions', []):
            interact_type = act.get('type') # COMMENT atau REPLY
            source_user = act.get('source_username')
            
            # Jika dia komen, targetnya adalah ID Postingan. 
            # Jika dia reply, targetnya adalah username yang dia balas.
            target_id = act.get('target_id') 
            
            clean_content = act.get('content', '').replace('\n', ' ').replace('\r', ' ').replace('"', "'")
            interact_likes = act.get('likes', 0)

            dataset.append({
                "Source": source_user,
                "Target": target_id,
                "Interaction_Type": interact_type,
                "Post_Like_Count": post_likes,
                "Post_Comment_Count": post_comments_count,
                "Interaction_Like_Count": interact_likes,
                "Content": clean_content
            })
            
    # Buat DataFrame
    df = pd.DataFrame(dataset)
    new_filename = "dataset_sna_suarasurabaya.csv"
    
    # Simpan ke CSV dengan separator koma standar
    df.to_csv(new_filename, index=False, encoding="utf-8")
    
    with open(new_filename, "r", encoding="utf-8") as f: 
        csv_content = f.read()
        
    return Response(
        content=csv_content, 
        media_type="text/csv", 
        headers={"Content-Disposition": f"attachment; filename={new_filename}"}
    )

def _build_neo4j_graph(mode: int):
    """
    Helper untuk membangun NetworkX DiGraph langsung dari Neo4j dengan Cypher.
    """
    G = nx.DiGraph()

    if mode == 1:
        # --- MODE 1: USER TO USER (1-MODE GRAPH) ---
        # Definisi: User A berinteraksi dengan User B jika A me-like/komen postingan B.
        query = """
        // 1. Relasi LIKES (Bobot 1)
        MATCH (u1:User)-[:LIKES_KAWAN]->(p:KawanSS)<-[:POSTED]-(u2:User)
        WHERE u1.id <> u2.id
        RETURN u1.id AS source_id, coalesce(u1.nama, u1.username, 'Unknown') AS source_name,
               u2.id AS target_id, coalesce(u2.nama, u2.username, 'Unknown') AS target_name,
               'LIKE' AS type, 1 AS weight
        UNION ALL
        // 2. Relasi COMMENTS (Bobot 3)
        MATCH (u1:User)-[:WROTE]->(c:KawanssComment)-[:COMMENTED_ON]->(p:KawanSS)<-[:POSTED]-(u2:User)
        WHERE u1.id <> u2.id
        RETURN u1.id AS source_id, coalesce(u1.nama, u1.username, 'Unknown') AS source_name,
               u2.id AS target_id, coalesce(u2.nama, u2.username, 'Unknown') AS target_name,
               'COMMENT' AS type, 3 AS weight
        """
        
        with neo4j_driver.session() as session:
            records = session.run(query).data()
            
        for r in records:
            s_id, t_id = r['source_id'], r['target_id']
            if not G.has_node(s_id): G.add_node(s_id, label=r['source_name'], type="user")
            if not G.has_node(t_id): G.add_node(t_id, label=r['target_name'], type="user")
            
            if G.has_edge(s_id, t_id):
                G[s_id][t_id]['weight'] += r['weight']
            else:
                G.add_edge(s_id, t_id, weight=r['weight'], type=r['type'])

    elif mode == 2:
        # --- MODE 2: USER TO POST (2-MODE / BIPARTITE GRAPH) ---
        query = """
        // 1. Relasi KEPEMILIKAN POST (Bobot 5)
        MATCH (u:User)-[:POSTED]->(p:KawanSS)
        RETURN u.id AS source_id, coalesce(u.nama, u.username, 'Unknown') AS source_name, 'user' AS source_type,
               p.id AS target_id, coalesce(p.title, 'Postingan') AS target_name, 'post' AS target_type,
               'AUTHORED' AS type, 5 AS weight
        UNION ALL
        // 2. Relasi LIKES_KAWAN (Bobot 1)
        MATCH (u:User)-[:LIKES_KAWAN]->(p:KawanSS)
        RETURN u.id AS source_id, coalesce(u.nama, u.username, 'Unknown') AS source_name, 'user' AS source_type,
               p.id AS target_id, coalesce(p.title, 'Postingan') AS target_name, 'post' AS target_type,
               'LIKE' AS type, 1 AS weight
        UNION ALL
        // 3. Relasi COMMENTED_ON (Bobot 3)
        MATCH (u:User)-[:WROTE]->(c:KawanssComment)-[:COMMENTED_ON]->(p:KawanSS)
        RETURN u.id AS source_id, coalesce(u.nama, u.username, 'Unknown') AS source_name, 'user' AS source_type,
               p.id AS target_id, coalesce(p.title, 'Postingan') AS target_name, 'post' AS target_type,
               'COMMENT' AS type, 3 AS weight
        """
        
        with neo4j_driver.session() as session:
            records = session.run(query).data()
            
        for r in records:
            s_id = f"user_{r['source_id']}"
            t_id = f"post_{r['target_id']}"
            
            if not G.has_node(s_id): G.add_node(s_id, label=r['source_name'], type="user")
            if not G.has_node(t_id): G.add_node(t_id, label=r['target_name'], type="post")
            
            if G.has_edge(s_id, t_id):
                G[s_id][t_id]['weight'] += r['weight']
            else:
                G.add_edge(s_id, t_id, weight=r['weight'], type=r['type'])

    # Hapus node yang terisolasi
    G.remove_nodes_from(list(nx.isolates(G)))

    # --- CLUSTERING (LEIDEN ALGORITHM) ---
    if G.number_of_nodes() > 0:
        mapping = {node: i for i, node in enumerate(G.nodes())}
        reverse_mapping = {i: node for node, i in mapping.items()}
        
        ig_G = ig.Graph(directed=True)
        ig_G.add_vertices(len(G.nodes()))
        ig_G.add_edges([(mapping[u], mapping[v]) for u, v in G.edges()])
        
        # Menerapkan weights ke iGraph untuk deteksi komunitas yang akurat
        if nx.is_weighted(G):
            ig_G.es['weight'] = [G[u][v]['weight'] for u, v in G.edges()]

        # Find Partitions
        partition = la.find_partition(
            ig_G, 
            la.ModularityVertexPartition,
            weights=ig_G.es['weight'] if 'weight' in ig_G.es.attributes() else None,
            n_iterations=-1
        )

        # Mapping warna cluster ke node
        for comm_id, members in enumerate(partition):
            for idx in members:
                node_id = reverse_mapping[idx]
                G.nodes[node_id]['community'] = comm_id

    return G


async def visualize_neo4j_network(mode: int = 1):
    """
    Menghasilkan HTML interaktif visualisasi graf berdasarkan data Neo4j.
    """
    try:
        G = _build_neo4j_graph(mode)
        
        if G.number_of_nodes() == 0:
            return HTMLResponse("<h1>Graf Kosong</h1><p>Belum ada data relasi di Neo4j. Lakukan migrasi terlebih dahulu.</p>")

        degree_cent = nx.degree_centrality(G)
        
        net = Network(height="100vh", width="100%", bgcolor="#1e1e1e", font_color="white", cdn_resources='in_line')
        
        # --- MENAMBAHKAN NODE ---
        for node, data in G.nodes(data=True):
            group = data.get('community', 0) # Warna dinamis dari Leiden
            score = degree_cent.get(node, 0)
            size = 15 + (score * 60) # Ukuran lingkaran menyesuaikan Degree Centrality
            
            # Bentuk berbeda untuk 2-Mode
            shape = 'dot'
            if mode == 2 and data.get('type') == 'post':
                shape = 'square'
                
            title_html = f"<b>{data.get('type').upper()}:</b> {data.get('label')}<br><b>Cluster/Komunitas:</b> {group}"
            
            net.add_node(
                node, 
                label=data.get('label', str(node))[:15], 
                title=title_html,
                group=group, 
                size=size,
                shape=shape
            )
            
        # --- MENAMBAHKAN WEIGHTED EDGES ---
        for u, v, data in G.edges(data=True):
            weight = data.get('weight', 1)
            edge_type = data.get('type', 'Interaction')
            
            # Value akan membuat garis di HTML lebih tebal sesuai bobotnya
            net.add_edge(u, v, value=weight, title=f"Tipe: {edge_type}<br>Total Bobot: {weight}")

        net.toggle_physics(True)
        
        output_path = f"{OUTPUT_HTML_DIR}/neo4j_mode_{mode}.html"
        net.save_graph(output_path)
        
        with open(output_path, "r", encoding="utf-8") as f:
            html_content = f.read()
            
        return HTMLResponse(content=html_content, status_code=200)

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Gagal memvisualisasikan graf Neo4j: {str(e)}")

async def analyze_neo4j_network(mode: int = 1):
    """
    Mengembalikan JSON murni jika Flutter Anda membutuhkan raw data Nodes & Edges beserta Komunitasnya.
    """
    try:
        G = _build_neo4j_graph(mode)
        nodes_result = [{"id": n, "attributes": G.nodes[n]} for n in G.nodes()]
        edges_result = [{"source": u, "target": v, "attributes": G[u][v]} for u, v in G.edges()]
        
        return {
            "meta": {"mode": mode, "total_nodes": G.number_of_nodes(), "total_edges": G.number_of_edges()},
            "graph_data": {"nodes": nodes_result, "edges": edges_result}
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))