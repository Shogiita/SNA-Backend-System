import requests
import json
import time
import networkx as nx
import leidenalg as la
import igraph as ig
import concurrent.futures
import textwrap
import os
import glob
import re
import pandas as pd
import asyncio  # Tambahan untuk async
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
from dateutil import parser
from pyvis.network import Network
from fastapi import HTTPException
from fastapi.responses import HTMLResponse, Response
from app import config
from app.database import db # Tambahan untuk koneksi Firestore
from google.cloud.firestore import FieldFilter

CACHE_FILE = "instagram_data_cache.json"
OUTPUT_HTML_DIR = "generated_graphs"

# --- KONFIGURASI LIMIT ---
MAX_POSTS_TO_FETCH = 10000
FETCH_MONTHS_BACK = 12
MAX_WORKERS = 20
# -------------------------

os.makedirs(OUTPUT_HTML_DIR, exist_ok=True)

session = requests.Session()

def _get_posts_recursive(start_date, end_date, max_posts=MAX_POSTS_TO_FETCH):
    """Mengambil daftar post ID dari Instagram API dengan limitasi."""
    all_posts = []
    url = f"{config.GRAPH_API_URL}/{config.IG_BUSINESS_ACCOUNT_ID}/media"
    params = {
        "access_token": config.IG_ACCESS_TOKEN,
        "fields": "id,caption,media_type,media_url,permalink,timestamp,like_count,comments_count",
        "limit": 50
    }
    
    print(f"📡 Memulai pengambilan list post (Maksimal: {max_posts} post)...")
    
    while url and len(all_posts) < max_posts:
        try:
            response = session.get(url, params=params)
            data = response.json()
            
            if 'error' in data:
                print(f"API Error: {data['error']['message']}")
                break
            if 'data' not in data: break
            
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
                time.sleep(0.1)
            else:
                break
        except Exception as e:
            print(f"Error fetching post list: {e}")
            break
            
    print(f"✅ Berhasil mendapatkan {len(all_posts)} post ID.")
    return all_posts

def _fetch_comments_parallel(post):
    """Worker untuk mengambil komentar & reply per post."""
    post_id = post['id']
    interactions = []
    
    post_item = {
        "id": post_id,
        "caption": post.get('caption', ''),
        "media_url": post.get('media_url', ''),
        "permalink": post.get('permalink', ''),
        "like_count": post.get('like_count', 0),
        "timestamp": post.get('timestamp'),
        "interactions": []
    }
    
    if post.get('comments_count', 0) == 0:
        return post_item

    url = f"{config.GRAPH_API_URL}/{post_id}/comments"
    params = {
        "access_token": config.IG_ACCESS_TOKEN,
        "fields": "id,text,username,like_count,timestamp,replies{id,text,username,like_count,timestamp}",
        "limit": 50 
    }
    
    try:
        page_count = 0
        while url and page_count < 3: 
            resp = session.get(url, params=params)
            data = resp.json()
            if 'error' in data: break
            
            items = data.get('data', [])
            if not items: break

            for comment in items:
                c_user = comment.get('username', 'Unknown')
                c_id = comment.get('id')
                
                interactions.append({
                    "id": c_id,
                    "type": "COMMENT", 
                    "source_username": c_user, 
                    "target_id": post_id,
                    "content": comment.get('text', ''), 
                    "likes": comment.get('like_count', 0),
                    "timestamp": comment.get('timestamp')
                })
                
                if 'replies' in comment:
                    for r in comment['replies']['data']:
                        r_user = r.get('username', 'Unknown')
                        r_id = r.get('id')
                        
                        interactions.append({
                            "id": r_id,
                            "type": "REPLY", 
                            "source_username": r_user, 
                            "target_id": c_id,
                            "content": r.get('text', ''), 
                            "likes": r.get('like_count', 0),
                            "timestamp": r.get('timestamp')
                        })
            
            if 'paging' in data and 'next' in data['paging']:
                url = data['paging']['next']
                params = {}
                page_count += 1
            else:
                break
    except Exception:
        pass
    
    post_item["interactions"] = interactions
    return post_item

def run_ingestion_process():
    """Mengambil data Instagram dan menyimpannya ke JSON Cache."""
    if not config.IG_ACCESS_TOKEN:
        raise HTTPException(status_code=500, detail="Token Instagram belum diatur di .env")

    end_date = datetime.now(timezone.utc)
    start_date = end_date - relativedelta(months=FETCH_MONTHS_BACK)
    
    raw_posts = _get_posts_recursive(start_date, end_date, max_posts=MAX_POSTS_TO_FETCH)
    
    full_dataset = []
    total_posts = len(raw_posts)
    completed = 0

    print(f"🚀 Memulai download detail komentar untuk {total_posts} post...")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(_fetch_comments_parallel, post): post for post in raw_posts}
        
        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
                full_dataset.append(result)
                completed += 1
                if completed % 10 == 0:
                    print(f"   ...Progress: {completed}/{total_posts} post selesai.")
            except Exception as e:
                print(f"Error pada worker: {e}")

    with open(CACHE_FILE, "w") as f:
        json.dump(full_dataset, f)
        
    print("✅ Ingestion Selesai!")
    return {
        "message": "Ingestion Selesai.",
        "params_used": {
            "months_back": FETCH_MONTHS_BACK,
            "max_posts_limit": MAX_POSTS_TO_FETCH
        },
        "total_posts_fetched": len(full_dataset),
        "file_saved": CACHE_FILE
    }

def get_dataset_flat():
    """
    Membaca cache, membuat file CSV baru dengan penomoran otomatis (dataset_ss1, dataset_ss2...),
    menyimpannya di server, lalu mengirimkannya ke user.
    """
    if not os.path.exists(CACHE_FILE):
        raise HTTPException(status_code=404, detail="Cache data belum ada. Jalankan /sna/ingest terlebih dahulu.")
    
    with open(CACHE_FILE, "r") as f:
        posts_data = json.load(f)
        
    dataset = []
    owner_id = config.IG_BUSINESS_ACCOUNT_ID
    owner_username = config.IG_USERNAME or "OWNER_ACCOUNT"
    
    for post in posts_data:
        # Baris 1: Post
        dataset.append({
            "type": "POST",
            "post_id": post['id'],
            "user_id": owner_id, 
            "username": owner_username, 
            "caption": post.get('caption', ''),
            "media_url": post.get('media_url', ''),
            "content": post.get('caption', ''), 
            "target_id": None, 
            "interaction_id": post['id'], 
            "timestamp": post.get('timestamp')
        })
        
        # Baris 2..n: Interactions
        for act in post['interactions']:
            dataset.append({
                "type": act['type'],
                "post_id": post['id'], 
                "user_id": act.get('source_username'), 
                "username": act.get('source_username'),
                "caption": None, 
                "media_url": None, 
                "content": act.get('content'),
                "target_id": act.get('target_id'), 
                "interaction_id": act.get('id'), 
                "timestamp": act.get('timestamp')
            })
            
    # --- PROSES PEMBUATAN FILE CSV ---
    df = pd.DataFrame(dataset)
    
    # Rapikan kolom
    columns_order = [
        "interaction_id", "timestamp", "type", "username", "content", 
        "target_id", "post_id", "media_url"
    ]
    valid_columns = [col for col in columns_order if col in df.columns]
    df = df[valid_columns]

    # --- LOGIKA PENAMAAN FILE OTOMATIS (dataset_ss1, dataset_ss2, ...) ---
    base_name = "dataset_ss"
    ext = ".csv"
    
    existing_files = glob.glob(f"{base_name}*{ext}")
    max_num = 0
    
    for f in existing_files:
        try:
            name = os.path.basename(f)
            match = re.search(r"dataset_ss(\d+)\.csv", name)
            if match:
                num = int(match.group(1))
                if num > max_num:
                    max_num = num
        except:
            continue
            
    next_num = max_num + 1
    new_filename = f"{base_name}{next_num}{ext}"
    
    df.to_csv(new_filename, index=False)
    print(f"✅ File CSV baru berhasil disimpan: {new_filename}")

    with open(new_filename, "r", encoding="utf-8") as f:
        csv_content = f.read()

    return Response(
        content=csv_content,
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={new_filename}"}
    )

def generate_sna_html(metric_type: str = "degree"):
    """Visualisasi Graph."""
    
    if not os.path.exists(CACHE_FILE):
        raise HTTPException(status_code=404, detail="File cache tidak ditemukan. Jalankan /sna/ingest terlebih dahulu.")
    
    with open(CACHE_FILE, "r") as f:
        posts_data = json.load(f)
        
    G = nx.DiGraph()
    for post in posts_data:
        post_id = post['id']
        caption = textwrap.shorten(post.get('caption', ''), width=30)
        
        G.add_node(post_id, label=caption, title=post.get('caption', ''), type="post", 
                   color="#FF5733", size=20 + (post.get('like_count', 0) * 0.05))
        
        for act in post['interactions']:
            src = act.get('source_username', 'Unknown')
            tgt = act.get('target_id')
            
            if src == 'Unknown' or not tgt: continue
            
            if not G.has_node(src): 
                G.add_node(src, label=src, type="user", color="#3498DB", size=10)
            
            real_target = post_id 
            
            if G.has_edge(src, real_target):
                G[src][real_target]['weight'] += 1
                G[src][real_target]['title'] += f"<br>{act['content']}"
            else:
                G.add_edge(src, real_target, weight=1, title=act['content'])

    if G.number_of_nodes() > 0:
        node_names = list(G.nodes())
        mapping = {name: i for i, name in enumerate(node_names)}
        ig_G = ig.Graph(directed=True)
        ig_G.add_vertices(len(node_names))
        ig_G.add_edges([(mapping[u], mapping[v]) for u, v in G.edges()])
        
        partition = la.find_partition(ig_G, la.ModularityVertexPartition)
        community_map = {}
        for comm_id, members in enumerate(partition):
            for idx in members:
                community_map[node_names[idx]] = comm_id
        
        for node in G.nodes():
            if node in community_map: G.nodes[node]['group'] = community_map[node]

    layout_pos = nx.spring_layout(G, k=0.15, iterations=50, seed=42)
    
    centrality = {}
    if metric_type == "degree": centrality = nx.degree_centrality(G)
    elif metric_type == "betweenness": centrality = nx.betweenness_centrality(G)
    elif metric_type == "closeness": centrality = nx.closeness_centrality(G)
    else: centrality = nx.degree_centrality(G)

    vals = list(centrality.values())
    if vals:
        min_val, max_val = min(vals), max(vals)
        for node in G.nodes():
            score = float(centrality.get(node, 0))
            size = 15.0 if max_val == min_val else 10.0 + ((score - min_val) / (max_val - min_val)) * 40.0
            G.nodes[node]['size'] = size
            
            if node in layout_pos:
                G.nodes[node]['x'] = float(layout_pos[node][0]) * 1000
                G.nodes[node]['y'] = float(layout_pos[node][1]) * 1000
            
            base = G.nodes[node].get('title', '')[:50]
            comm = G.nodes[node].get('group', '-')
            G.nodes[node]['title'] = f"{base}<br><hr><b>{metric_type}:</b> {score:.4f}<br><b>Comm:</b> {comm}"

    net = Network(height="700px", width="100%", bgcolor="#222222", font_color="white", cdn_resources='in_line')
    net.from_nx(G)
    net.toggle_physics(False)
    
    output_path = f"{OUTPUT_HTML_DIR}/sna_{metric_type}.html"
    net.save_graph(output_path)
    
    with open(output_path, "r", encoding="utf-8") as f:
        html_content = f.read()
        
    return HTMLResponse(content=html_content, status_code=200)

async def analyze_firestore_network():
    """
    Membangun graf SNA dari data Firestore (Users, Likes, Comments).
    Menghitung Centrality (Degree, Betweenness, Closeness, Eigenvector) & Komunitas (Leiden).
    """
    try:
        # --- 1. FETCH DATA (Async Wrapper untuk Firestore Client yg Blocking) ---
        # Kita gunakan asyncio.to_thread agar tidak memblokir main thread FastAPI
        users_ref = db.collection('user') # Menggunakan nama koleksi 'user' (bukan users) sesuai skema Anda
        posts_ref = db.collection('kawanss')
        likes_ref = db.collection('kawanssLikes')
        comments_ref = db.collection('kawanssComments')

        # Helper untuk fetch semua docs
        def get_all_docs(collection_ref):
            return {doc.id: doc.to_dict() for doc in collection_ref.stream()}
        
        # Jalankan fetch secara paralel
        users_data, posts_data, likes_data, comments_data = await asyncio.gather(
            asyncio.to_thread(get_all_docs, users_ref),
            asyncio.to_thread(get_all_docs, posts_ref),
            asyncio.to_thread(get_all_docs, likes_ref),
            asyncio.to_thread(get_all_docs, comments_ref)
        )

        # --- 2. BANGUN GRAF (NetworkX) ---
        G = nx.DiGraph()

        # Tambahkan Nodes (User)
        for uid, data in users_data.items():
            # Gunakan nama asli atau fallback ke 'Unknown'
            label = data.get('nama', data.get('username', 'Unknown')) 
            G.add_node(uid, label=label, type="user")

        # Mapping: Post ID -> Author ID
        # Agar kita tahu siapa yang harus diberi "skor" saat dilike/dikomen
        post_author_map = {}
        for pid, data in posts_data.items():
            author_id = data.get('userId') # Sesuai struktur data Anda
            if author_id:
                post_author_map[pid] = author_id

        # Tambahkan Edges (Interaksi)
        # Bobot: Like = 1, Comment = 3
        
        # Proses Likes
        for data in likes_data.values():
            liker_id = data.get('userUid') # Sesuai struktur 'kawanssLikes'
            post_id = data.get('kawanssUid')
            
            if liker_id and post_id and post_id in post_author_map:
                target_id = post_author_map[post_id]
                if liker_id != target_id: # Abaikan self-like
                    if G.has_edge(liker_id, target_id):
                        G[liker_id][target_id]['weight'] += 1
                    else:
                        G.add_edge(liker_id, target_id, weight=1, type='LIKE')

        # Proses Comments
        for data in comments_data.values():
            commenter_id = data.get('userId') # Sesuai struktur 'kawanssComments'
            post_id = data.get('kawanssUid')
            
            if commenter_id and post_id and post_id in post_author_map:
                target_id = post_author_map[post_id]
                if commenter_id != target_id:
                    if G.has_edge(commenter_id, target_id):
                        G[commenter_id][target_id]['weight'] += 3
                    else:
                        G.add_edge(commenter_id, target_id, weight=3, type='COMMENT')

        # --- 3. HITUNG CENTRALITY ---
        if G.number_of_nodes() == 0:
            return {"message": "Graf kosong, tidak ada data user/interaksi."}

        degree = nx.degree_centrality(G)
        betweenness = nx.betweenness_centrality(G, weight='weight')
        closeness = nx.closeness_centrality(G)
        try:
            eigenvector = nx.eigenvector_centrality(G, weight='weight', max_iter=500)
        except:
            eigenvector = {n: 0 for n in G.nodes()} # Fallback jika tidak konvergen

        # --- 4. DETEKSI KOMUNITAS (Leiden) ---
        # Konversi ke iGraph untuk Leiden
        mapping = {node: i for i, node in enumerate(G.nodes())}
        reverse_mapping = {i: node for node, i in mapping.items()}
        
        ig_G = ig.Graph(directed=True)
        ig_G.add_vertices(len(G.nodes()))
        ig_edges = [(mapping[u], mapping[v]) for u, v in G.edges()]
        ig_G.add_edges(ig_edges)
        
        if nx.is_weighted(G):
            ig_G.es['weight'] = [G[u][v]['weight'] for u, v in G.edges()]

        # Jalankan Leiden
        # Menggunakan ModularityVertexPartition untuk kualitas klaster terbaik
        partition = la.find_partition(
            ig_G, 
            la.ModularityVertexPartition,
            weights=ig_G.es['weight'] if 'weight' in ig_G.es.attributes() else None,
            n_iterations=-1
        )

        # Mapping hasil komunitas kembali ke Node ID
        communities = {}
        for comm_id, members in enumerate(partition):
            for idx in members:
                node_id = reverse_mapping[idx]
                communities[node_id] = comm_id

        # --- 5. SUSUN HASIL JSON ---
        nodes_result = []
        for node in G.nodes():
            nodes_result.append({
                "id": node,
                "label": G.nodes[node].get('label'),
                "community": communities.get(node, 0),
                "metrics": {
                    "degree": degree.get(node, 0),
                    "betweenness": betweenness.get(node, 0),
                    "closeness": closeness.get(node, 0),
                    "eigenvector": eigenvector.get(node, 0)
                }
            })

        edges_result = []
        for u, v in G.edges():
            edges_result.append({
                "source": u,
                "target": v,
                "weight": G[u][v]['weight']
            })

        # Urutkan Top Influencer berdasarkan Betweenness
        top_influencers = sorted(
            nodes_result, 
            key=lambda x: x['metrics']['betweenness'], 
            reverse=True
        )[:10]

        return {
            "meta": {
                "total_nodes": G.number_of_nodes(),
                "total_edges": G.number_of_edges(),
                "total_communities": len(partition)
            },
            "top_influencers": top_influencers,
            "graph_data": {
                "nodes": nodes_result,
                "edges": edges_result
            }
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Gagal melakukan analisis SNA Firestore: {str(e)}")