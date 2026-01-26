import requests
import json
import time
import networkx as nx
import leidenalg as la
import igraph as ig
import concurrent.futures
import textwrap
import os
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
from dateutil import parser
from pyvis.network import Network
from fastapi import HTTPException
from fastapi.responses import HTMLResponse
from app import config 

CACHE_FILE = "instagram_data_cache.json"
OUTPUT_HTML_DIR = "generated_graphs"

os.makedirs(OUTPUT_HTML_DIR, exist_ok=True)

session = requests.Session()

def _get_posts_recursive(start_date, end_date):
    """Mengambil daftar post ID dari Instagram API (Pagination)."""
    all_posts = []
    url = f"{config.GRAPH_API_URL}/{config.IG_BUSINESS_ACCOUNT_ID}/media"
    params = {
        "access_token": config.IG_ACCESS_TOKEN,
        "fields": "id,caption,media_type,media_url,permalink,timestamp,like_count,comments_count",
        "limit": 20
    }
    
    print(f"📡 Fetching posts from {start_date} to {end_date}...")
    
    while url:
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
                if post_time >= start_date:
                    all_posts.append(post)
                else:
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
            
    return all_posts

def _fetch_comments_parallel(post):
    """Worker untuk mengambil komentar per post."""
    post_id = post['id']
    interactions = []
    
    post_item = {
        "id": post_id,
        "caption": post.get('caption', ''),
        "media_url": post.get('media_url', ''),
        "like_count": post.get('like_count', 0),
        "timestamp": post.get('timestamp'),
        "interactions": []
    }
    
    url = f"{config.GRAPH_API_URL}/{post_id}/comments"
    params = {
        "access_token": config.IG_ACCESS_TOKEN,
        "fields": "id,text,username,like_count,timestamp,replies{id,text,username,like_count}",
        "limit": 20
    }
    
    try:
        page_count = 0
        while url and page_count < 3:
            resp = session.get(url, params=params)
            data = resp.json()
            if 'error' in data: break
            
            for comment in data.get('data', []):
                c_user = comment.get('username', 'Unknown')
                interactions.append({
                    "type": "COMMENT", "source": c_user, "target": post_id, 
                    "content": comment.get('text', ''), "likes": comment.get('like_count', 0)
                })
                if 'replies' in comment:
                    for r in comment['replies']['data']:
                        r_user = r.get('username', 'Unknown')
                        interactions.append({
                            "type": "REPLY", "source": r_user, "target": c_user, 
                            "content": r.get('text', ''), "likes": r.get('like_count', 0)
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
    """Menjalankan proses ambil data berat dan simpan ke JSON."""
    if not config.IG_ACCESS_TOKEN:
        raise HTTPException(status_code=500, detail="Token Instagram belum diatur di .env")

    end_date = datetime.now(timezone.utc)
    start_date = end_date - relativedelta(months=10)
    
    raw_posts = _get_posts_recursive(start_date, end_date)
    
    full_dataset = []
    workers = 5 
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(_fetch_comments_parallel, post) for post in raw_posts]
        for future in concurrent.futures.as_completed(futures):
            try:
                full_dataset.append(future.result())
            except Exception:
                pass

    with open(CACHE_FILE, "w") as f:
        json.dump(full_dataset, f)
        
    return {
        "message": "Ingestion Selesai",
        "total_posts": len(full_dataset),
        "file_saved": CACHE_FILE
    }

def generate_sna_html(metric_type: str = "degree"):
    """Membaca JSON cache, bangun graph, hitung centrality, dan return HTML."""
    
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
            src, tgt = act['source'], act['target']
            if src == 'Unknown' or tgt == 'Unknown': continue
            
            if not G.has_node(src): G.add_node(src, label=src, type="user", color="#3498DB", size=10)
            if act['type'] == 'REPLY' and not G.has_node(tgt):
                 G.add_node(tgt, label=tgt, type="user", color="#3498DB", size=10)

            if G.has_edge(src, tgt):
                G[src][tgt]['weight'] += 1
                G[src][tgt]['title'] += f"<br>{act['content']}"
            else:
                G.add_edge(src, tgt, weight=1, title=act['content'])

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
    if metric_type == "degree":
        centrality = nx.degree_centrality(G)
    elif metric_type == "betweenness":
        centrality = nx.betweenness_centrality(G)
    elif metric_type == "closeness":
        centrality = nx.closeness_centrality(G)
    elif metric_type == "eigenvector":
        try:
            centrality = nx.eigenvector_centrality(G, max_iter=1000)
        except:
            centrality = nx.degree_centrality(G)
    else:
        centrality = nx.degree_centrality(G)

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