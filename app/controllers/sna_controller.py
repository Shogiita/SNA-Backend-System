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
from collections import Counter, defaultdict
from fastapi import HTTPException, BackgroundTasks
from app.database import neo4j_driver
import calendar
import traceback 
from apscheduler.schedulers.background import BackgroundScheduler

CACHE_FILE = "instagram_data_cache.json"
OUTPUT_HTML_DIR = "generated_graphs"

MAX_POSTS_TO_FETCH = 1000
FETCH_MONTHS_BACK = 12
MAX_WORKERS = 10

HASHTAG_REGEX = re.compile(r"#(\w+)")

os.makedirs(OUTPUT_HTML_DIR, exist_ok=True)

# Gunakan Session untuk mempercepat koneksi (Connection Pooling)
session = requests.Session()

def _build_neo4j_graph(mode: int, limit: int = 1000):
    G = nx.DiGraph()

    with neo4j_driver.session() as session:
        if mode == 1:
            query = """
            CALL {
                MATCH (u1:InstagramUser)-[:WROTE_IG]->(c:InstagramComment)-[:COMMENTED_ON_IG]->(p:InstagramPost)<-[:POSTED_IG]-(u2:InstagramUser)
                WHERE u1.username <> u2.username
                RETURN u1.username AS s_id, u2.username AS t_id, 3 AS w, 'COMMENT' AS t
                UNION ALL
                MATCH (u1:InstagramUser)-[:WROTE_IG]->(r:InstagramComment)-[:REPLIED_TO_IG]->(c:InstagramComment)<-[:WROTE_IG]-(u2:InstagramUser)
                WHERE u1.username <> u2.username
                RETURN u1.username AS s_id, u2.username AS t_id, 4 AS w, 'REPLY' AS t
            }
            WITH s_id, t_id, sum(w) AS total_weight, collect(DISTINCT t) AS rel_types
            ORDER BY total_weight DESC
            LIMIT $limit
            RETURN s_id, t_id, total_weight AS weight, rel_types
            """
            records = session.run(query, limit=limit).data()
            for r in records:
                s_id, t_id = f"user_{r['s_id']}", f"user_{r['t_id']}"
                if not G.has_node(s_id): G.add_node(s_id, type="user", label=r['s_id'])
                if not G.has_node(t_id): G.add_node(t_id, type="user", label=r['t_id'])
                
                relation_str = ", ".join(r['rel_types'])
                if G.has_edge(s_id, t_id):
                    G[s_id][t_id]['weight'] += r['weight']
                else:
                    G.add_edge(s_id, t_id, relation=relation_str, weight=r['weight'])

        elif mode == 2:
            query_posts = """
            MATCH (u:InstagramUser)-[:POSTED_IG]->(p:InstagramPost)
            RETURN u.username AS uid, p.id AS pid, coalesce(p.caption, '') AS text, coalesce(p.like_count, 0) AS likes
            LIMIT $limit
            """
            query_comments = """
            MATCH (u:InstagramUser)-[:WROTE_IG]->(c:InstagramComment)-[:COMMENTED_ON_IG]->(p:InstagramPost)
            RETURN u.username AS uid, c.id AS cid, coalesce(c.text, '') AS text, coalesce(c.likes, 0) AS likes, p.id AS target_id
            LIMIT $limit
            """
            query_replies = """
            MATCH (u:InstagramUser)-[:WROTE_IG]->(r:InstagramComment)-[:REPLIED_TO_IG]->(c:InstagramComment)
            RETURN u.username AS uid, r.id AS cid, coalesce(r.text, '') AS text, coalesce(r.likes, 0) AS likes, c.id AS target_id
            LIMIT $limit
            """
            
            posts_data = session.run(query_posts, limit=limit).data()
            comments_data = session.run(query_comments, limit=limit).data()
            replies_data = session.run(query_replies, limit=limit).data()
            
            for r in posts_data:
                u_id, p_id = f"user_{r['uid']}", f"post_{r['pid']}"
                text = r['text']
                if not G.has_node(u_id): G.add_node(u_id, type="user", label=r['uid'])
                if not G.has_node(p_id): G.add_node(p_id, type="post_ig", label=text[:20]+"...", full_text=text, likes=r['likes'])
                G.add_edge(u_id, p_id, relation="POSTED_IG", weight=5)
                
                hashtags = set(HASHTAG_REGEX.findall(text.lower()))
                for tag in hashtags:
                    h_id = f"tag_{tag}"
                    if not G.has_node(h_id): G.add_node(h_id, type="hashtag", label=f"#{tag}")
                    G.add_edge(p_id, h_id, relation="HAS_HASHTAG", weight=2)
                    
            for r in comments_data:
                u_id, c_id, target_id = f"user_{r['uid']}", f"comment_{r['cid']}", f"post_{r['target_id']}"
                text = r['text']
                if not G.has_node(u_id): G.add_node(u_id, type="user", label=r['uid'])
                if not G.has_node(c_id): G.add_node(c_id, type="comment_ig", label=text[:20]+"...", full_text=text, likes=r['likes'])
                if G.has_node(target_id):
                    G.add_edge(u_id, c_id, relation="WROTE_IG", weight=3)
                    G.add_edge(c_id, target_id, relation="COMMENTED_ON_IG", weight=3)
                    
                    hashtags = set(HASHTAG_REGEX.findall(text.lower()))
                    for tag in hashtags:
                        h_id = f"tag_{tag}"
                        if not G.has_node(h_id): G.add_node(h_id, type="hashtag", label=f"#{tag}")
                        G.add_edge(c_id, h_id, relation="HAS_HASHTAG", weight=2)
                        
            for r in replies_data:
                u_id, r_id, target_id = f"user_{r['uid']}", f"reply_{r['cid']}", f"comment_{r['target_id']}"
                text = r['text']
                if not G.has_node(u_id): G.add_node(u_id, type="user", label=r['uid'])
                if not G.has_node(r_id): G.add_node(r_id, type="reply_ig", label=text[:20]+"...", full_text=text, likes=r['likes'])
                if G.has_node(target_id):
                    G.add_edge(u_id, r_id, relation="WROTE_IG", weight=4)
                    G.add_edge(r_id, target_id, relation="REPLIED_TO_IG", weight=4)
                    
                    hashtags = set(HASHTAG_REGEX.findall(text.lower()))
                    for tag in hashtags:
                        h_id = f"tag_{tag}"
                        if not G.has_node(h_id): G.add_node(h_id, type="hashtag", label=f"#{tag}")
                        G.add_edge(r_id, h_id, relation="HAS_HASHTAG", weight=2)

    G.remove_nodes_from(list(nx.isolates(G)))

    if G.number_of_nodes() > 0:
        mapping = {node: i for i, node in enumerate(G.nodes())}
        reverse_mapping = {i: node for node, i in mapping.items()}
        
        ig_G = ig.Graph(directed=True)
        ig_G.add_vertices(len(G.nodes()))
        ig_G.add_edges([(mapping[u], mapping[v]) for u, v in G.edges()])
        
        if nx.is_weighted(G):
            ig_G.es['weight'] = [G[u][v]['weight'] for u, v in G.edges()]

        partition = la.find_partition(
            ig_G, la.ModularityVertexPartition,
            weights=ig_G.es['weight'] if 'weight' in ig_G.es.attributes() else None,
            n_iterations=-1
        )

        for comm_id, members in enumerate(partition):
            for idx in members:
                G.nodes[reverse_mapping[idx]]['community'] = comm_id

    return G

def _process_ig_to_neo4j_batch(posts_batch, comments_batch):
    """Fungsi helper untuk memasukkan data ke Neo4j secara batching"""
    post_query = """
    UNWIND $posts AS post
    MERGE (p:InstagramPost {id: post.id})
    SET p.caption = post.caption,
        p.permalink = post.permalink,
        p.media_type = post.media_type,
        p.like_count = post.like_count,
        p.comments_count = post.comments_count,
        p.share_count = post.share_count, // Placeholder API limit
        p.view_count = post.view_count,   // Placeholder API limit
        p.timestamp = post.timestamp
    
    // Asosiasi User Pembuat Post (Dipisah menggunakan label InstagramUser)
    MERGE (u:InstagramUser {username: post.username})
    MERGE (u)-[:POSTED_IG]->(p)
    """

    comment_query = """
    UNWIND $comments AS comm
    MERGE (c:InstagramComment {id: comm.id})
    SET c.text = comm.text,
        c.likes = comm.likes,
        c.timestamp = comm.timestamp,
        c.type = comm.type,
        c.replies_count = comm.replies_count
        
    // Asosiasi User Penulis Komen (Dipisah menggunakan label InstagramUser)
    MERGE (u:InstagramUser {username: comm.username})
    MERGE (u)-[:WROTE_IG]->(c)
    
    // Relasi ke Postingan atau ke Komentar Induk (Reply)
    WITH c, comm
    CALL {
        WITH c, comm
        WITH c, comm WHERE comm.type = 'COMMENT'
        MERGE (p:InstagramPost {id: comm.target_id})
        MERGE (c)-[:COMMENTED_ON_IG]->(p)
    }
    CALL {
        WITH c, comm
        WITH c, comm WHERE comm.type = 'REPLY'
        MERGE (parent:InstagramComment {id: comm.target_id})
        MERGE (c)-[:REPLIED_TO_IG]->(parent)
    }
    """
    
    try:
        with neo4j_driver.session() as session:
            if posts_batch:
                session.run(post_query, posts=posts_batch)
            if comments_batch:
                session.run(comment_query, comments=comments_batch)
    except Exception as e:
        print(f"[NEO4J ERROR] Gagal insert batch: {e}")

def sync_instagram_to_neo4j(is_initial_sync=False):
    """
    Fungsi utama untuk menarik data dan memasukannya ke Neo4j.
    """
    print(f"🔄 Memulai Sinkronisasi IG ke Neo4j. Initial Sync: {is_initial_sync}")
    
    end_date = datetime.now(timezone.utc)
    
    # Batas pasti 2 bulan terakhir (Sliding window)
    two_months_ago = end_date - relativedelta(months=2)
    
    if is_initial_sync:
        # Tarik data 2 Bulan ke belakang
        start_date = two_months_ago 
        max_posts = 5000 
    else:
        # Untuk update harian/jam, cukup cek 2 hari ke belakang untuk menghemat limit API
        start_date = end_date - relativedelta(days=2)
        max_posts = 100

    url = f"{config.GRAPH_API_URL}/{config.IG_BUSINESS_ACCOUNT_ID}/media"
    params = {
        "access_token": config.IG_ACCESS_TOKEN,
        "fields": "id,caption,media_type,media_url,permalink,timestamp,like_count,comments_count,username",
        "limit": 50
    }
    
    posts_batch = []
    comments_batch = []
    batch_size = 100

    try:
        while url:
            resp = session.get(url, params=params)
            data = resp.json()
            if 'error' in data or 'data' not in data: 
                break
                
            stop_fetching = False
            for post in data['data']:
                post_time = parser.isoparse(post['timestamp'])
                
                # Berhenti jika menemui postingan yang lebih lama dari start_date
                if post_time < start_date:
                    stop_fetching = True
                    break
                
                # 1. Siapkan Data Post
                posts_batch.append({
                    "id": post.get('id'),
                    "username": post.get('username', 'Suara_Surabaya_Official'),
                    "caption": post.get('caption', ''),
                    "permalink": post.get('permalink', ''),
                    "media_type": post.get('media_type', 'UNKNOWN'),
                    "like_count": post.get('like_count', 0),
                    "comments_count": post.get('comments_count', 0),
                    "share_count": 0,
                    "view_count": 0,
                    "timestamp": post.get('timestamp')
                })

                # 2. Tarik Data Komentar dan Reply
                if post.get('comments_count', 0) > 0:
                    comm_url = f"{config.GRAPH_API_URL}/{post['id']}/comments"
                    comm_params = {
                        "access_token": config.IG_ACCESS_TOKEN, 
                        "fields": "id,text,username,like_count,timestamp,replies{id,text,username,like_count,timestamp}",
                        "limit": 50
                    }
                    try:
                        comm_resp = session.get(comm_url, params=comm_params)
                        comm_data = comm_resp.json().get('data', [])
                        
                        for c in comm_data:
                            replies_data = c.get('replies', {}).get('data', [])
                            comments_batch.append({
                                "id": c['id'],
                                "target_id": post['id'],
                                "type": "COMMENT",
                                "text": c.get('text', ''),
                                "username": c.get('username', 'Unknown'),
                                "likes": c.get('like_count', 0),
                                "replies_count": len(replies_data),
                                "timestamp": c.get('timestamp')
                            })
                            
                            for r in replies_data:
                                comments_batch.append({
                                    "id": r['id'],
                                    "target_id": c['id'],
                                    "type": "REPLY",
                                    "text": r.get('text', ''),
                                    "username": r.get('username', 'Unknown'),
                                    "likes": r.get('like_count', 0),
                                    "replies_count": 0,
                                    "timestamp": r.get('timestamp')
                                })
                    except Exception as ce:
                        print(f"Error fetching comments for {post['id']}: {ce}")

                if len(posts_batch) >= batch_size or len(comments_batch) >= batch_size * 5:
                    _process_ig_to_neo4j_batch(posts_batch, comments_batch)
                    posts_batch.clear()
                    comments_batch.clear()

            if stop_fetching: break
            url = data.get('paging', {}).get('next')
            params = {}

        if posts_batch or comments_batch:
            _process_ig_to_neo4j_batch(posts_batch, comments_batch)
            
        print("✅ Tarikan data IG ke Neo4j Selesai.")
        cutoff_iso_string = two_months_ago.strftime('%Y-%m-%dT%H:%M:%S+0000')
        print(f"🧹 Membersihkan data Instagram yang lebih lama dari {cutoff_iso_string}")
        
        cleanup_query = """
        MATCH (c:InstagramComment) WHERE c.timestamp < $cutoff
        DETACH DELETE c
        """
        cleanup_posts_query = """
        MATCH (p:InstagramPost) WHERE p.timestamp < $cutoff
        DETACH DELETE p
        """
        cleanup_users_query = """
        MATCH (u:InstagramUser) WHERE NOT (u)--()
        DELETE u
        """
        
        try:
            with neo4j_driver.session() as db_session:
                db_session.run(cleanup_query, cutoff=cutoff_iso_string)
                db_session.run(cleanup_posts_query, cutoff=cutoff_iso_string)
                db_session.run(cleanup_users_query)
            print("✅ Sinkronisasi dan Pembersihan Sliding Window Berhasil!")
        except Exception as e:
            print(f"[CLEANUP ERROR] Gagal membersihkan data lama: {e}")

    except Exception as e:
        print(f"❌ Sinkronisasi Gagal: {e}")
        import traceback
        traceback.print_exc()

def get_instagram_metrics(start_date: str = None, end_date: str = None):
    # Gunakan perf_counter untuk mengukur performa yang lebih akurat (pecahan detik)
    start_time = time.perf_counter() 
    now = datetime.now(timezone.utc)
    
    # 1. Parsing Tanggal
    if start_date:
        start_dt = parser.parse(start_date).replace(tzinfo=timezone.utc)
    else:
        start_dt = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    if end_date:
        end_dt = parser.parse(end_date).replace(hour=23, minute=59, second=59, tzinfo=timezone.utc)
    else:
        last_day_of_month = calendar.monthrange(now.year, now.month)[1]
        end_dt = now.replace(day=last_day_of_month, hour=23, minute=59, second=59, microsecond=0)

    str_start_iso = start_dt.strftime('%Y-%m-%dT%H:%M:%S+0000') 
    str_end_iso = end_dt.strftime('%Y-%m-%dT%H:%M:%S+0000')

    # 2. Query ke Neo4j (Dioptimalkan)
    records = []
    try:
        with neo4j_driver.session() as db_session:
            query = """
            MATCH (p:InstagramPost)
            WHERE p.timestamp >= $start_iso AND p.timestamp <= $end_iso
            RETURN p.id AS id, 
                   p.permalink AS permalink, 
                   coalesce(p.caption, '') AS caption, 
                   coalesce(toInteger(p.like_count), 0) AS like_count, 
                   coalesce(toInteger(p.comments_count), 0) AS comments_count, 
                   p.timestamp AS timestamp
            """
            # Langsung ambil .data() secara efisien
            records = db_session.run(query, start_iso=str_start_iso, end_iso=str_end_iso).data()
    except Exception as e:
        return {"status": "error", "message": f"Database Error: {str(e)}"}

    if not records:
        return {
            "status": "success", 
            "message": "Tidak ada postingan di rentang waktu tersebut.", 
            "data": {"top_10_posts": [], "top_10_hashtags": []}
        }

    # 3. Proses Analisis di RAM Python (Sangat Cepat - O(N))
    hashtag_counts = Counter()
    hashtag_to_posts = defaultdict(list)

    for p in records:
        caption = p['caption']
        if caption:
            # Menggunakan regex yang sudah terkompilasi
            tags = set(HASHTAG_REGEX.findall(caption.lower()))
            clean_cap = caption.replace('\n', ' ')
            short_cap = clean_cap[:100] + "..." if len(clean_cap) > 100 else clean_cap
            
            post_info = {
                "id": p["id"],
                "permalink": p["permalink"],
                "caption": short_cap,
                "like_count": p["like_count"],
                "comments_count": p["comments_count"],
                "timestamp": p["timestamp"]
            }
            
            for tag in tags:
                hashtag_counts[tag] += 1
                hashtag_to_posts[tag].append(post_info)

    # Susun Top 10 Hashtags
    top_10_hashtags = []
    for tag, count in hashtag_counts.most_common(10):
        # Ambil Top 3 post dari list secara efisien
        sorted_posts = sorted(hashtag_to_posts[tag], key=lambda x: x["like_count"], reverse=True)[:3]
        top_10_hashtags.append({
            "hashtag": f"#{tag}",
            "count": count,
            "top_posts": sorted_posts
        })

    # 4. Susun Top 10 Posts
    records.sort(key=lambda x: x["like_count"], reverse=True)
    top_10_posts = []
    for p in records[:10]:
        clean_cap = p['caption'].replace('\n', ' ')
        top_10_posts.append({
            "id": p["id"],
            "permalink": p["permalink"],
            "caption": clean_cap[:100] + "..." if len(clean_cap) > 100 else clean_cap,
            "like_count": p["like_count"],
            "comments_count": p["comments_count"],
            "timestamp": p["timestamp"]
        })

    # Hitung total waktu proses dalam bentuk detik (sampai 4 angka di belakang koma)
    process_time = round(time.perf_counter() - start_time, 4)

    return {
        "status": "success",
        "message": f"Data {len(records)} postingan berhasil dianalisis dalam {process_time} detik.",
        "data": {
            "top_10_posts": top_10_posts,
            "top_10_hashtags": top_10_hashtags
        }
    }

def _background_sync_ig_to_neo4j():
    """
    Fungsi Worker: Menarik 1000 post, mencari Top 3 Posts per Hashtag, simpan ke Neo4j.
    """
    print("[IG SYNC] Memulai penarikan 1000 post...")
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
            if 'error' in data: break
            if 'data' in data: all_posts.extend(data['data'])
            if 'paging' in data and 'next' in data['paging']:
                url = data['paging']['next']
                params = {} 
            else: break 
        except: break

    all_posts = all_posts[:max_posts]
    if not all_posts: return

    # A. TOP 10 POSTS GLOBAL
    sorted_posts = sorted(all_posts, key=lambda x: x.get('like_count', 0) + x.get('comments_count', 0), reverse=True)
    top_10_posts = []
    for p in sorted_posts[:10]:
        clean_cap = p.get("caption", "").replace('\n', ' ')
        top_10_posts.append({
            "id": p.get("id"),
            "permalink": p.get("permalink", ""),
            "caption": clean_cap[:100] + "...",
            "like_count": p.get("like_count", 0),
            "comments_count": p.get("comments_count", 0),
            "total_engagement": p.get("like_count", 0) + p.get("comments_count", 0),
            "timestamp": p.get("timestamp", "")
        })

    # B. TOP 10 HASHTAGS + TOP 3 POSTS PER HASHTAG
    hashtag_counts = Counter()
    hashtag_to_posts = defaultdict(list)

    for p in all_posts:
        caption = p.get('caption', '')
        if caption:
            tags = set(re.findall(r"#(\w+)", caption.lower()))
            
            # Siapkan info post LENGKAP untuk dilampirkan ke dalam hashtag
            engagement = p.get('like_count', 0) + p.get('comments_count', 0)
            clean_cap = p.get("caption", "").replace('\n', ' ')
            
            post_info = {
                "id": p.get("id"),
                "permalink": p.get("permalink", ""),
                "caption": clean_cap[:100] + "...",
                "like_count": p.get("like_count", 0),
                "comments_count": p.get("comments_count", 0),
                "total_engagement": engagement,
                "timestamp": p.get("timestamp", "")
            }
            
            for tag in tags:
                hashtag_counts[tag] += 1
                hashtag_to_posts[tag].append(post_info)

    top_10_hashtags = []
    for tag, count in hashtag_counts.most_common(10):
        # Urutkan berdasarkan engagement tertinggi
        sorted_posts_for_tag = sorted(hashtag_to_posts[tag], key=lambda x: x["total_engagement"], reverse=True)
        top_10_hashtags.append({
            "hashtag": f"#{tag}",
            "count": count,
            "top_posts": sorted_posts_for_tag[:3] # Sekarang berisi data lengkap
        })

    # C. SIMPAN KE NEO4J
    save_query = """
    MERGE (n:InstagramMetrics {id: 'latest_metrics'})
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
                last_updated=time.time()
            )
        print("[IG SYNC] SUKSES memperbarui Neo4j.")
    except Exception as e:
        print(f"[IG SYNC ERROR] {str(e)}")
         
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

def _build_neo4j_graph(mode: int, limit: int = 1000):
    G = nx.DiGraph()

    with neo4j_driver.session() as session:
        if mode == 1:
            # Mode 1: User to User (Mendukung InstagramPosts & InstagramPost)
            query = """
            CALL {
                MATCH (u1:InstagramUser)-[:WROTE_IG]->(c:InstagramComment)-[:COMMENTED_ON_IG]->(p)<-[:POSTED_IG]-(u2:InstagramUser)
                WHERE (p:InstagramPosts OR p:InstagramPost) AND u1.username <> u2.username
                RETURN u1.username AS s_id, u2.username AS t_id, 3 AS w, 'COMMENT' AS t
                UNION ALL
                MATCH (u1:InstagramUser)-[:WROTE_IG]->(r:InstagramComment)-[:REPLIED_TO_IG]->(c:InstagramComment)<-[:WROTE_IG]-(u2:InstagramUser)
                WHERE u1.username <> u2.username
                RETURN u1.username AS s_id, u2.username AS t_id, 4 AS w, 'REPLY' AS t
            }
            WITH s_id, t_id, sum(w) AS total_weight, collect(DISTINCT t) AS rel_types
            ORDER BY total_weight DESC
            LIMIT $limit
            RETURN s_id, t_id, total_weight AS weight, rel_types
            """
            records = session.run(query, limit=limit).data()
            for r in records:
                s_id, t_id = f"user_{r['s_id']}", f"user_{r['t_id']}"
                if not G.has_node(s_id): G.add_node(s_id, type="user", label=r['s_id'])
                if not G.has_node(t_id): G.add_node(t_id, type="user", label=r['t_id'])
                
                relation_str = ", ".join(r['rel_types'])
                if G.has_edge(s_id, t_id):
                    G[s_id][t_id]['weight'] += r['weight']
                else:
                    G.add_edge(s_id, t_id, relation=relation_str, weight=r['weight'])

        elif mode == 2:
            # Mode 2: Multi-modal (Mendukung InstagramPosts & Hashtag dari Comment)
            query_posts = """
            MATCH (u:InstagramUser)-[:POSTED_IG]->(p)
            WHERE p:InstagramPosts OR p:InstagramPost
            RETURN u.username AS uid, p.id AS pid, coalesce(p.caption, '') AS text, coalesce(p.like_count, 0) AS likes
            LIMIT $limit
            """
            query_comments = """
            MATCH (u:InstagramUser)-[:WROTE_IG]->(c:InstagramComment)-[:COMMENTED_ON_IG]->(p)
            WHERE p:InstagramPosts OR p:InstagramPost
            RETURN u.username AS uid, c.id AS cid, coalesce(c.text, '') AS text, coalesce(c.likes, 0) AS likes, p.id AS target_id
            LIMIT $limit
            """
            query_replies = """
            MATCH (u:InstagramUser)-[:WROTE_IG]->(r:InstagramComment)-[:REPLIED_TO_IG]->(c:InstagramComment)
            RETURN u.username AS uid, r.id AS cid, coalesce(r.text, '') AS text, coalesce(r.likes, 0) AS likes, c.id AS target_id
            LIMIT $limit
            """
            
            posts_data = session.run(query_posts, limit=limit).data()
            comments_data = session.run(query_comments, limit=limit).data()
            replies_data = session.run(query_replies, limit=limit).data()
            
            # --- 1. Proses Posting ---
            for r in posts_data:
                u_id, p_id = f"user_{r['uid']}", f"post_{r['pid']}"
                text = r['text']
                if not G.has_node(u_id): G.add_node(u_id, type="user", label=r['uid'])
                if not G.has_node(p_id): G.add_node(p_id, type="post_ig", label=text[:20]+"...", full_text=text, likes=r['likes'])
                G.add_edge(u_id, p_id, relation="POSTED_IG", weight=5)
                    
            # --- 2. Proses Comment & Ekstrak Hashtag dari Comment ---
            for r in comments_data:
                u_id, c_id, target_id = f"user_{r['uid']}", f"comment_{r['cid']}", f"post_{r['target_id']}"
                text = r['text']
                if not G.has_node(u_id): G.add_node(u_id, type="user", label=r['uid'])
                if not G.has_node(c_id): G.add_node(c_id, type="comment_ig", label=text[:20]+"...", full_text=text, likes=r['likes'])
                
                if G.has_node(target_id):
                    G.add_edge(u_id, c_id, relation="WROTE_IG", weight=3)
                    G.add_edge(c_id, target_id, relation="COMMENTED_ON_IG", weight=3)
                    
                # Ekstrak HASHTAG khusus dari text InstagramComment
                hashtags = set(HASHTAG_REGEX.findall(text.lower()))
                for tag in hashtags:
                    h_id = f"tag_{tag}"
                    if not G.has_node(h_id): G.add_node(h_id, type="hashtag", label=f"#{tag}")
                    G.add_edge(c_id, h_id, relation="COMMENT_HASHTAG", weight=2)
                        
            # --- 3. Proses Reply & Ekstrak Hashtag dari Reply ---
            for r in replies_data:
                u_id, r_id, target_id = f"user_{r['uid']}", f"reply_{r['cid']}", f"comment_{r['target_id']}"
                text = r['text']
                if not G.has_node(u_id): G.add_node(u_id, type="user", label=r['uid'])
                if not G.has_node(r_id): G.add_node(r_id, type="reply_ig", label=text[:20]+"...", full_text=text, likes=r['likes'])
                
                if G.has_node(target_id):
                    G.add_edge(u_id, r_id, relation="WROTE_IG", weight=4)
                    G.add_edge(r_id, target_id, relation="REPLIED_TO_IG", weight=4)
                    
                # Ekstrak HASHTAG khusus dari text InstagramComment (tipe Reply)
                hashtags = set(HASHTAG_REGEX.findall(text.lower()))
                for tag in hashtags:
                    h_id = f"tag_{tag}"
                    if not G.has_node(h_id): G.add_node(h_id, type="hashtag", label=f"#{tag}")
                    G.add_edge(r_id, h_id, relation="REPLY_HASHTAG", weight=2)

    # Menghapus node yang terisolasi
    G.remove_nodes_from(list(nx.isolates(G)))

    # Algoritma Clustering (Leiden)
    if G.number_of_nodes() > 0:
        mapping = {node: i for i, node in enumerate(G.nodes())}
        reverse_mapping = {i: node for node, i in mapping.items()}
        
        ig_G = ig.Graph(directed=True)
        ig_G.add_vertices(len(G.nodes()))
        ig_G.add_edges([(mapping[u], mapping[v]) for u, v in G.edges()])
        
        if nx.is_weighted(G):
            ig_G.es['weight'] = [G[u][v]['weight'] for u, v in G.edges()]

        partition = la.find_partition(
            ig_G, la.ModularityVertexPartition,
            weights=ig_G.es['weight'] if 'weight' in ig_G.es.attributes() else None,
            n_iterations=-1
        )

        for comm_id, members in enumerate(partition):
            for idx in members:
                G.nodes[reverse_mapping[idx]]['community'] = comm_id

    return G

async def visualize_neo4j_network(mode: int = 1, limit: int = 1000):
    try:
        from pyvis.network import Network
        # Parameter limit sudah ditambahkan dengan benar
        G = _build_neo4j_graph(mode, limit)
        
        if G.number_of_nodes() == 0:
            return HTMLResponse("<h1>Graf Kosong</h1><p>Belum ada data relasi di Neo4j. Lakukan migrasi terlebih dahulu.</p>")

        degree_cent = nx.degree_centrality(G)
        net = Network(height="100vh", width="100%", bgcolor="#1e1e1e", font_color="white", cdn_resources='in_line')
        
        for node, data in G.nodes(data=True):
            group = data.get('community', 0)
            score = degree_cent.get(node, 0)
            size = 15 + (score * 60)
            
            node_type = data.get('type', 'user')
            shape = 'dot'
            color = None
            
            if node_type == 'user':
                shape = 'dot'
            elif node_type == 'post_ig':
                shape = 'square'
                color = "#E1306C" # Pink/Ungu IG
            elif node_type == 'comment_ig':
                shape = 'triangle'
                color = "#F56040" # Orange 
            elif node_type == 'reply_ig':
                shape = 'triangleDown'
                color = "#FCAF45" # Kuning/Orange
            elif node_type == 'hashtag':
                shape = 'star'
                color = "#833AB4" # Ungu hashtag
                
            label = data.get('label', str(node))
            title_html = f"<b>{str(node_type).upper()}:</b> {label}<br><b>Cluster/Komunitas:</b> {group}"
            if 'likes' in data:
                title_html += f"<br><b>Total Likes:</b> {data['likes']}"
            
            if color:
                net.add_node(node, label=str(label)[:15], title=title_html, group=group, size=size, shape=shape, color=color)
            else:
                net.add_node(node, label=str(label)[:15], title=title_html, group=group, size=size, shape=shape)
            
        for u, v, data in G.edges(data=True):
            weight = data.get('weight', 1)
            edge_type = data.get('relation', 'Interaction')
            net.add_edge(u, v, value=weight, title=f"Tipe: {edge_type}<br>Total Bobot: {weight}")

        net.toggle_physics(True)
        output_path = f"{OUTPUT_HTML_DIR}/neo4j_mode_{mode}.html"
        
        html_content = net.generate_html(output_path)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(html_content)

        return HTMLResponse(content=html_content, status_code=200)

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Gagal memvisualisasikan graf Neo4j: {str(e)}")

async def analyze_neo4j_network(mode: int = 1, limit: int = 1000):
    try:
        G = _build_neo4j_graph(mode, limit)
        nodes_result = [{"id": n, "attributes": G.nodes[n]} for n in G.nodes()]
        edges_result = [{"source": u, "target": v, "attributes": G[u][v]} for u, v in G.edges()]
        
        return {
            "meta": {"mode": mode, "total_nodes": G.number_of_nodes(), "total_edges": G.number_of_edges()},
            "graph_data": {"nodes": nodes_result, "edges": edges_result}
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    try:
        G = _build_neo4j_graph(mode, limit)
        nodes_result = [{"id": n, "attributes": G.nodes[n]} for n in G.nodes()]
        edges_result = [{"source": u, "target": v, "attributes": G[u][v]} for u, v in G.edges()]
        
        return {
            "meta": {"mode": mode, "total_nodes": G.number_of_nodes(), "total_edges": G.number_of_edges()},
            "graph_data": {"nodes": nodes_result, "edges": edges_result}
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
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