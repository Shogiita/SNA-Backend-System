import networkx as nx
import leidenalg as la
import igraph as ig
import os
from fastapi import HTTPException
from fastapi.responses import HTMLResponse
from app.database import neo4j_driver

OUTPUT_HTML_DIR = "generated_graphs"
os.makedirs(OUTPUT_HTML_DIR, exist_ok=True)

def _build_neo4j_graph_internal(limit: int, mode: int):
    """
    Query yang diperbarui untuk mencakup KawanSS & Infoss.
    Semua aggregasi bobot (SUM) dilakukan langsung oleh Database Neo4j.
    """
    G = nx.DiGraph()
    
    with neo4j_driver.session() as session:
        if mode == 1:
            # --- 1-MODE: USER TO USER (Semua Interaksi) ---
            query = """
            CALL {
                // Relasi via Like
                MATCH (u1:User)-[r:LIKES_KAWAN|LIKES_INFOSS|LIKES]->(p)<-[:POSTED|AUTHORED]-(u2:User)
                WHERE u1.id <> u2.id AND (p:KawanSS OR p:Infoss)
                RETURN u1, u2, 1 AS w, 'LIKE' AS t
                UNION ALL
                // Relasi via Comment (Lewat node KawanssComment)
                MATCH (u1:User)-[:WROTE|COMMENTED]->(c)-[:COMMENTED_ON]->(p)<-[:POSTED|AUTHORED]-(u2:User)
                WHERE u1.id <> u2.id AND (p:KawanSS OR p:Infoss)
                RETURN u1, u2, 3 AS w, 'COMMENT' AS t
                UNION ALL
                // Relasi via Comment (Direct - Jika ada)
                MATCH (u1:User)-[:COMMENTED_ON]->(p)<-[:POSTED|AUTHORED]-(u2:User)
                WHERE u1.id <> u2.id AND (p:KawanSS OR p:Infoss)
                RETURN u1, u2, 3 AS w, 'COMMENT' AS t
            }
            WITH u1, u2, sum(w) AS total_weight, collect(DISTINCT t) AS rel_types
            ORDER BY total_weight DESC
            LIMIT $limit
            RETURN u1.id AS source_id, coalesce(u1.nama, u1.username, 'Unknown') AS source_name,
                   u2.id AS target_id, coalesce(u2.nama, u2.username, 'Unknown') AS target_name,
                   total_weight AS weight, rel_types
            """
            records = session.run(query, limit=limit).data()
            
            for r in records:
                s_id = f"user_{r['source_id']}"
                t_id = f"user_{r['target_id']}"
                
                if not G.has_node(s_id): G.add_node(s_id, type="user", name=r['source_name'], neo4j_id=r['source_id'])
                if not G.has_node(t_id): G.add_node(t_id, type="user", name=r['target_name'], neo4j_id=r['target_id'])
                
                relation_str = ", ".join(r['rel_types'])
                G.add_edge(s_id, t_id, relation=relation_str, weight=r['weight'])

        elif mode == 2:
            # --- 2-MODE: USER TO POST (KawanSS & Infoss) ---
            query = """
            CALL {
                MATCH (u:User)-[:POSTED|AUTHORED]->(p)
                WHERE (p:KawanSS OR p:Infoss)
                RETURN u, p, 5 AS w, 'POSTED' AS t
                UNION ALL
                MATCH (u:User)-[:LIKES_KAWAN|LIKES_INFOSS|LIKES]->(p)
                WHERE (p:KawanSS OR p:Infoss)
                RETURN u, p, 1 AS w, 'LIKE' AS t
                UNION ALL
                MATCH (u:User)-[:WROTE|COMMENTED]->(c)-[:COMMENTED_ON]->(p)
                WHERE (p:KawanSS OR p:Infoss)
                RETURN u, p, 3 AS w, 'COMMENT' AS t
                UNION ALL
                MATCH (u:User)-[:COMMENTED_ON]->(p)
                WHERE (p:KawanSS OR p:Infoss)
                RETURN u, p, 3 AS w, 'COMMENT' AS t
            }
            WITH u, p, sum(w) AS total_weight, collect(DISTINCT t) AS rel_types
            ORDER BY total_weight DESC
            LIMIT $limit
            RETURN u.id AS source_id, coalesce(u.nama, u.username, 'Unknown') AS source_name,
                   p.id AS target_id, coalesce(p.title, p.judul, 'Postingan') AS target_name, labels(p) AS p_labels,
                   total_weight AS weight, rel_types
            """
            records = session.run(query, limit=limit).data()
            
            for r in records:
                s_id = f"user_{r['source_id']}"
                t_id = f"post_{r['target_id']}"
                
                # Membedakan Post biasa dan Info/Berita
                post_type = "post_infoss" if "Infoss" in r['p_labels'] else "post_kawanss"
                
                if not G.has_node(s_id): G.add_node(s_id, type="user", name=r['source_name'], neo4j_id=r['source_id'])
                if not G.has_node(t_id): G.add_node(t_id, type=post_type, title=r['target_name'], neo4j_id=r['target_id'])
                
                relation_str = ", ".join(r['rel_types'])
                G.add_edge(s_id, t_id, relation=relation_str, weight=r['weight'])

    # Bersihkan node tanpa relasi
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