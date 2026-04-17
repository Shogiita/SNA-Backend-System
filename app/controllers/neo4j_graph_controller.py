import networkx as nx
import leidenalg as la
import igraph as ig
import os
import re
from fastapi import HTTPException
from fastapi.responses import HTMLResponse
from app.database import neo4j_driver

OUTPUT_HTML_DIR = "generated_graphs"
os.makedirs(OUTPUT_HTML_DIR, exist_ok=True)

HASHTAG_REGEX = re.compile(r'#\w+')

def _build_neo4j_graph_internal(limit: int, mode: int):
    G = nx.DiGraph()
    
    with neo4j_driver.session() as session:
        if mode == 1:
            # Mode 1: User-to-User Interactions
            query = """
            CALL {
                MATCH (u1:FirebaseUser)-[:LIKES_KAWAN_FB]->(p:FirebaseKawanSS)<-[:POSTED_FB]-(u2:FirebaseUser)
                WHERE u1.id <> u2.id
                RETURN u1.id AS source_id, coalesce(u1.nama, u1.username, 'Unknown') AS source_name,
                       u2.id AS target_id, coalesce(u2.nama, u2.username, 'Unknown') AS target_name,
                       1 AS w, 'LIKE' AS t
                UNION ALL
                MATCH (u1:FirebaseUser)-[:LIKES_INFO_FB]->(p:FirebaseInfoss)<-[:POSTED_FB]-(u2:FirebaseUser)
                WHERE u1.id <> u2.id
                RETURN u1.id AS source_id, coalesce(u1.nama, u1.username, 'Unknown') AS source_name,
                       u2.id AS target_id, coalesce(u2.nama, u2.username, 'Unknown') AS target_name,
                       1 AS w, 'LIKE' AS t
                UNION ALL
                MATCH (u1:FirebaseUser)-[:WROTE_FB]->(c:FirebaseKawanSSComment)-[:COMMENTED_ON_FB]->(p:FirebaseKawanSS)<-[:POSTED_FB]-(u2:FirebaseUser)
                WHERE u1.id <> u2.id
                RETURN u1.id AS source_id, coalesce(u1.nama, u1.username, 'Unknown') AS source_name,
                       u2.id AS target_id, coalesce(u2.nama, u2.username, 'Unknown') AS target_name,
                       3 AS w, 'COMMENT' AS t
                UNION ALL
                MATCH (u1:FirebaseUser)-[:WROTE_FB]->(c:FirebaseInfossComment)-[:COMMENTED_ON_FB]->(p:FirebaseInfoss)<-[:POSTED_FB]-(u2:FirebaseUser)
                WHERE u1.id <> u2.id
                RETURN u1.id AS source_id, coalesce(u1.nama, u1.username, 'Unknown') AS source_name,
                       u2.id AS target_id, coalesce(u2.nama, u2.username, 'Unknown') AS target_name,
                       3 AS w, 'COMMENT' AS t
            }
            WITH source_id, source_name, target_id, target_name, sum(w) AS total_weight, collect(DISTINCT t) AS rel_types
            ORDER BY total_weight DESC
            LIMIT $limit
            RETURN source_id, source_name, target_id, target_name, total_weight AS weight, rel_types
            """
            records = session.run(query, limit=limit).data()
            for r in records:
                s_id, t_id = f"user_{r['source_id']}", f"user_{r['target_id']}"
                if not G.has_node(s_id): G.add_node(s_id, type="user", name=r['source_name'], label=r['source_name'])
                if not G.has_node(t_id): G.add_node(t_id, type="user", name=r['target_name'], label=r['target_name'])
                G.add_edge(s_id, t_id, relation=", ".join(r['rel_types']), weight=r['weight'])

        elif mode == 2:
            # Mode 2: Multi-modal Komprehensif (User, Post, Comment, Like, Hashtag)
            query_posts = """
            MATCH (u:FirebaseUser)-[:POSTED_FB]->(p)
            WHERE p:FirebaseKawanSS OR p:FirebaseInfoss
            RETURN u.id AS uid, coalesce(u.nama, u.username, 'Unknown') AS uname,
                   p.id AS pid, coalesce(p.deskripsi, p.judul, p.detail, p.title, '') AS text,
                   labels(p) AS p_labels, coalesce(toInteger(p.jumlahLike), 0) AS likes
            LIMIT $limit
            """
            query_comments = """
            MATCH (u:FirebaseUser)-[:WROTE_FB]->(c)-[:COMMENTED_ON_FB]->(p)
            WHERE c:FirebaseKawanSSComment OR c:FirebaseInfossComment
            RETURN u.id AS uid, coalesce(u.nama, u.username, 'Unknown') AS uname,
                   c.id AS cid, coalesce(c.komentar, c.text, '') AS text, labels(c) AS c_labels,
                   p.id AS target_id
            LIMIT $limit
            """
            query_likes = """
            MATCH (u:FirebaseUser)-[r]->(p)
            WHERE type(r) IN ['LIKES_KAWAN_FB', 'LIKES_INFO_FB']
            RETURN u.id AS uid, p.id AS target_id, type(r) AS rel_type
            LIMIT $limit
            """
            
            posts_data = session.run(query_posts, limit=limit).data()
            comments_data = session.run(query_comments, limit=limit).data()
            likes_data = session.run(query_likes, limit=limit).data()
            
            for r in posts_data:
                u_id, p_id = f"user_{r['uid']}", f"post_{r['pid']}"
                text = r['text']
                p_type = 'post_infoss' if 'FirebaseInfoss' in r['p_labels'] else 'post_kawanss'
                
                if not G.has_node(u_id): G.add_node(u_id, type="user", label=r['uname'])
                if not G.has_node(p_id): G.add_node(p_id, type=p_type, label=text[:20]+"...", full_text=text, likes=r['likes'])
                G.add_edge(u_id, p_id, relation="AUTHORED", weight=5)
                
                # Ekstraksi Hashtags
                hashtags = set(HASHTAG_REGEX.findall(text.lower()))
                for tag in hashtags:
                    h_id = f"tag_{tag}"
                    if not G.has_node(h_id): G.add_node(h_id, type="hashtag", label=f"#{tag}")
                    G.add_edge(p_id, h_id, relation="HAS_HASHTAG", weight=2)
                    
            for r in comments_data:
                u_id, c_id, target_id = f"user_{r['uid']}", f"comment_{r['cid']}", f"post_{r['target_id']}"
                text = r['text']
                c_type = 'comment_infoss' if 'FirebaseInfossComment' in r['c_labels'] else 'comment_kawanss'
                
                if not G.has_node(u_id): G.add_node(u_id, type="user", label=r['uname'])
                if not G.has_node(c_id): G.add_node(c_id, type=c_type, label=text[:20]+"...", full_text=text)
                if G.has_node(target_id): # Konek jika post ada di visualisasi
                    G.add_edge(u_id, c_id, relation="WROTE", weight=3)
                    G.add_edge(c_id, target_id, relation="COMMENTED_ON", weight=3)
                
                    hashtags = set(HASHTAG_REGEX.findall(text.lower()))
                    for tag in hashtags:
                        h_id = f"tag_{tag}"
                        if not G.has_node(h_id): G.add_node(h_id, type="hashtag", label=f"#{tag}")
                        G.add_edge(c_id, h_id, relation="HAS_HASHTAG", weight=2)

            for r in likes_data:
                u_id, target_id = f"user_{r['uid']}", f"post_{r['target_id']}"
                if G.has_node(u_id) and G.has_node(target_id):
                    G.add_edge(u_id, target_id, relation="LIKED", weight=1)

    # Bersihkan sisa node tanpa relasi
    G.remove_nodes_from(list(nx.isolates(G)))
    
    # --- CLUSTERING (LEIDEN) ---
    if G.number_of_nodes() > 0:
        mapping = {node: i for i, node in enumerate(G.nodes())}
        reverse_mapping = {i: node for node, i in mapping.items()}
        
        ig_G = ig.Graph(directed=True)
        ig_G.add_vertices(len(G.nodes()))
        ig_G.add_edges([(mapping[u], mapping[v]) for u, v in G.edges()])
        
        if nx.is_weighted(G):
            ig_G.es['weight'] = [G[u][v]['weight'] for u, v in G.edges()]

        partition = la.find_partition(
            ig_G, 
            la.ModularityVertexPartition,
            weights=ig_G.es['weight'] if 'weight' in ig_G.es.attributes() else None,
            n_iterations=-1
        )

        for comm_id, members in enumerate(partition):
            for idx in members:
                G.nodes[reverse_mapping[idx]]['community'] = comm_id
                
    return G

async def visualize_graph_from_neo4j(limit: int = 1000, mode: int = 1):
    try:
        from pyvis.network import Network
        G = _build_neo4j_graph_internal(limit, mode)
        
        if G.number_of_nodes() == 0:
            return HTMLResponse("<h1>Graf Kosong</h1><p>Belum ada data relasi di Neo4j.</p>")

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
            elif node_type in ['post_kawanss', 'post_infoss']:
                shape = 'square'
                color = "#FF5733" if node_type == 'post_infoss' else "#33C1FF" # Berbeda warna Post Kawan vs Infoss
            elif node_type in ['comment_kawanss', 'comment_infoss']:
                shape = 'triangle'
                color = "#FFC300" # Warna Kuning komentar
            elif node_type == 'hashtag':
                shape = 'star'
                color = "#9C33FF" # Ungu untuk hashtag
                
            label = data.get('label') or str(node)
            title_html = f"<b>{str(node_type).upper()}:</b> {label}<br><b>Cluster:</b> {group}"
            if 'likes' in data:
                title_html += f"<br><b>Total Likes:</b> {data['likes']}"
                
            if color:
                net.add_node(node, label=str(label)[:15], title=title_html, group=group, size=size, shape=shape, color=color)
            else:
                net.add_node(node, label=str(label)[:15], title=title_html, group=group, size=size, shape=shape)
            
        for u, v, data in G.edges(data=True):
            weight = data.get('weight', 1)
            relation = data.get('relation', 'Interaction')
            net.add_edge(u, v, value=weight, title=f"Relasi: {relation}<br>Bobot Total: {weight}")

        net.toggle_physics(True)
        output_path = f"{OUTPUT_HTML_DIR}/snagraph_mode_{mode}.html"
            
        html_content = net.generate_html(output_path)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(html_content)

        return HTMLResponse(content=html_content, status_code=200)
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Gagal memvisualisasikan: {str(e)}")

async def create_graph_from_neo4j(limit: int = 1000, mode: int = 1):
    """ Endpoint API (Mengembalikan JSON) """
    try:
        G = _build_neo4j_graph_internal(limit, mode)
        
        if G.number_of_nodes() == 0:
            raise HTTPException(status_code=404, detail="Tidak ada relasi data yang ditemukan di Neo4j.")

        for u, v, d in G.edges(data=True):
            w = d.get('weight', 1)
            d['distance'] = 1.0 / w if w > 0 else 1.0

        degree_cent = nx.degree_centrality(G)
        
        total_nodes = G.number_of_nodes()
        k_samples = min(200, total_nodes) if total_nodes > 500 else None
        betweenness_cent = nx.betweenness_centrality(G, weight='distance', k=k_samples)
        
        closeness_cent = nx.closeness_centrality(G, distance='distance', wf_improved=True)
        
        try:
            eigenvector_cent = nx.eigenvector_centrality(G, weight='weight', max_iter=2000, tol=1e-06)
        except nx.PowerIterationFailedConvergence:
            try:
                eigenvector_cent = nx.eigenvector_centrality_numpy(G, weight='weight')
            except Exception:
                eigenvector_cent = nx.pagerank(G, weight='weight', max_iter=200)


        nodes_output = []
        for n in G.nodes():
            attr = G.nodes[n].copy()
            nodes_output.append({
                "id": n,
                "attributes": attr,
                "metrics": {
                    "degree": degree_cent.get(n, 0.0),
                    "betweenness": betweenness_cent.get(n, 0.0),
                    "closeness": closeness_cent.get(n, 0.0),
                    "eigenvector": eigenvector_cent.get(n, 0.0) 
                }
            })

        edges_output = []
        for u, v, data in G.edges(data=True):
            edges_output.append({
                "source": u,
                "target": v,
                "weight": data.get('weight', 1),
                "attributes": {
                    "relation": data.get('relation', 'INTERACTION')
                }
            })

        return {
            "message": f"Graf SNA berhasil dibuat dari database Neo4j (Limit: {limit} pasang interaksi, Mode: {mode}).",
            "graph_info": {
                "nodes_count": G.number_of_nodes(),
                "edges_count": G.number_of_edges(),
                "nodes": nodes_output,
                "edges": edges_output
            }
        }
    except Exception as e:
        if isinstance(e, HTTPException): raise e
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Gagal memproses graf Neo4j: {str(e)}")
        
async def visualize_graph_from_neo4j(limit: int = 1000, mode: int = 1):
    """ Endpoint Visualisasi HTML PyVis """
    try:
        from pyvis.network import Network
        G = _build_neo4j_graph_internal(limit, mode)
        
        if G.number_of_nodes() == 0:
            return HTMLResponse("<h1>Graf Kosong</h1><p>Belum ada data relasi di Neo4j.</p>")

        degree_cent = nx.degree_centrality(G)
        net = Network(height="100vh", width="100%", bgcolor="#1e1e1e", font_color="white", cdn_resources='in_line')
        
        for node, data in G.nodes(data=True):
            group = data.get('community', 0)
            score = degree_cent.get(node, 0)
            size = 15 + (score * 60)
            
            shape = 'dot'
            if mode == 2 and data.get('type') in ['post_kawanss', 'post_infoss']:
                shape = 'square'
                
            label = data.get('name') or data.get('title') or str(node)
            title_html = f"<b>{str(data.get('type')).upper()}:</b> {label}<br><b>Cluster:</b> {group}"
            
            # Warnai secara berbeda jika itu adalah berita Infoss
            color = "#FF5733" if data.get('type') == 'post_infoss' else None
                
            if color:
                net.add_node(node, label=str(label)[:15], title=title_html, group=group, size=size, shape=shape, color=color)
            else:
                net.add_node(node, label=str(label)[:15], title=title_html, group=group, size=size, shape=shape)
            
        for u, v, data in G.edges(data=True):
            weight = data.get('weight', 1)
            relation = data.get('relation', 'Interaction')
            net.add_edge(u, v, value=weight, title=f"Relasi: {relation}<br>Bobot Total: {weight}")

        net.toggle_physics(True)
        output_path = f"{OUTPUT_HTML_DIR}/snagraph_mode_{mode}.html"
        # net.save_graph(output_path)
        
        # with open(output_path, "r", encoding="utf-8") as f:
        #     html_content = f.read()
            
        html_content = net.generate_html(output_path)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(html_content)

        return HTMLResponse(content=html_content, status_code=200)
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Gagal memvisualisasikan: {str(e)}")