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
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
from dateutil import parser
from pyvis.network import Network
from fastapi import HTTPException
from fastapi.responses import HTMLResponse, Response
from app import config
from app.database import neo4j_driver # Menggunakan Neo4j

CACHE_FILE = "instagram_data_cache.json"
OUTPUT_HTML_DIR = "generated_graphs"

MAX_POSTS_TO_FETCH = 10000
FETCH_MONTHS_BACK = 12
MAX_WORKERS = 20

os.makedirs(OUTPUT_HTML_DIR, exist_ok=True)
session = requests.Session()

# ==================================================
# INSTAGRAM INGESTION LOGIC
# ==================================================
def _get_posts_recursive(start_date, end_date, max_posts=MAX_POSTS_TO_FETCH):
    # Logika Instagram Ingestion tetap sama
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
            if 'error' in data: break
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
            else:
                break
        except Exception:
            break
    return all_posts

def _fetch_comments_parallel(post):
    post_id = post['id']
    interactions = []
    post_item = {
        "id": post_id, "caption": post.get('caption', ''), "media_url": post.get('media_url', ''),
        "permalink": post.get('permalink', ''), "like_count": post.get('like_count', 0),
        "timestamp": post.get('timestamp'), "interactions": []
    }
    if post.get('comments_count', 0) == 0: return post_item

    url = f"{config.GRAPH_API_URL}/{post_id}/comments"
    params = {"access_token": config.IG_ACCESS_TOKEN, "fields": "id,text,username,like_count,timestamp", "limit": 50}
    try:
        resp = session.get(url, params=params)
        data = resp.json()
        for comment in data.get('data', []):
            interactions.append({
                "id": comment.get('id'), "type": "COMMENT", "source_username": comment.get('username', 'Unknown'), 
                "target_id": post_id, "content": comment.get('text', ''), "likes": comment.get('like_count', 0),
                "timestamp": comment.get('timestamp')
            })
    except: pass
    post_item["interactions"] = interactions
    return post_item

def run_ingestion_process():
    end_date = datetime.now(timezone.utc)
    start_date = end_date - relativedelta(months=FETCH_MONTHS_BACK)
    raw_posts = _get_posts_recursive(start_date, end_date, max_posts=MAX_POSTS_TO_FETCH)
    full_dataset = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(_fetch_comments_parallel, post): post for post in raw_posts}
        for future in concurrent.futures.as_completed(futures):
            try: full_dataset.append(future.result())
            except: pass
    with open(CACHE_FILE, "w") as f: json.dump(full_dataset, f)
    return {"message": "Ingestion Selesai.", "total_posts_fetched": len(full_dataset)}

def get_dataset_flat():
    if not os.path.exists(CACHE_FILE): raise HTTPException(status_code=404, detail="Cache belum ada.")
    with open(CACHE_FILE, "r") as f: posts_data = json.load(f)
    dataset = []
    for post in posts_data:
        dataset.append({"type": "POST", "post_id": post['id'], "content": post.get('caption', '')})
        for act in post['interactions']:
            dataset.append({"type": act['type'], "post_id": post['id'], "user_id": act.get('source_username'), "content": act.get('content')})
    df = pd.DataFrame(dataset)
    new_filename = "dataset_ss.csv"
    df.to_csv(new_filename, index=False)
    with open(new_filename, "r", encoding="utf-8") as f: csv_content = f.read()
    return Response(content=csv_content, media_type="text/csv", headers={"Content-Disposition": f"attachment; filename={new_filename}"})


# ==================================================
# NEO4J SNA LOGIC (1-MODE, 2-MODE, CLUSTERING, WEIGHTS)
# ==================================================

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