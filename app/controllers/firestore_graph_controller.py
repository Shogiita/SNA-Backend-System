import asyncio
import networkx as nx
from fastapi import HTTPException, Response
from . import user_controller, post_controller

def _format_to_pajek(graph: nx.Graph) -> str:
    """Helper function to convert a NetworkX graph to Pajek .net format string."""
    pajek_str = f"*Vertices {graph.number_of_nodes()}\n"
    node_to_id = {node: i + 1 for i, node in enumerate(graph.nodes())}
    for node, node_id in node_to_id.items():
        pajek_str += f'{node_id} "{node}"\n'
    pajek_str += "*Arcs\n" if isinstance(graph, nx.DiGraph) else "*Edges\n"
    for u, v in graph.edges():
        pajek_str += f"{node_to_id[u]} {node_to_id[v]}\n"
    return pajek_str

async def create_graph_from_firestore(user_limit: int = 100, post_limit: int = 500):
    try:
        # 1. Ambil Post dari database dengan Limit
        posts = await post_controller.get_all_posts_from_db(limit=post_limit)

        if not posts:
            raise HTTPException(status_code=404, detail="Data post tidak ditemukan.")

        active_authors = {
            p.get('accountName') for p in posts 
            if p.get('accountName') and p.get('accountName').strip().lower() != "unknown user"
        }

        # 2. Ambil User
        all_users = await user_controller.get_all_users_from_db(limit=None)

        # 3. Filtering Cepat
        matched_users = []
        for user in all_users:
            if user.get('nama') in active_authors:
                matched_users.append(user)
                if len(matched_users) >= user_limit:
                    break

        # 4. Bangun Graf
        G = nx.DiGraph()
        user_lookup = {} 
        
        for user in matched_users:
            u_name = user.get('nama')
            node_id = f"user_{u_name}"
            user_lookup[u_name] = node_id
            G.add_node(node_id, type="user", name=u_name, firestore_id=user.get('id'))

        edges_to_add = []
        post_nodes = set()

        for post in posts:
            pid = post.get('id')
            if not pid: continue
            
            author = post.get('accountName')
            p_node_id = f"post_{pid}"
            post_nodes.add(p_node_id)
            
            G.add_node(p_node_id, type="post", author=author, title=post.get('title'))
            
            if author in user_lookup:
                edges_to_add.append((user_lookup[author], p_node_id, {"relation": "AUTHORED"}))

        for post in posts:
            pid = post.get('id')
            p_node_id = f"post_{pid}"
            reply_id = post.get('reply_to_id')
            
            if reply_id:
                target_pid = f"post_{reply_id}"
                if target_pid in post_nodes:
                    edges_to_add.append((p_node_id, target_pid, {"relation": "REPLIED_TO"}))

        G.add_edges_from(edges_to_add)

        # 5. Bersihkan sisa node tanpa relasi
        G.remove_nodes_from(list(nx.isolates(G)))

        if G.number_of_nodes() == 0:
            return {"message": "Tidak ada relasi edge yang terbentuk."}

        # === 6. PERHITUNGAN CENTRALITY METRICS ===
        
        degree_cent = nx.degree_centrality(G)
        betweenness_cent = nx.betweenness_centrality(G)
        closeness_cent = nx.closeness_centrality(G)
        
        # Eigenvector Centrality kadang gagal konvergen (Error) jika graf tidak terhubung sempurna 
        # (misal grafnya berbentuk pohon/terputus). Kita gunakan try-except untuk mencegah API crash.
        try:
            eigenvector_cent = nx.eigenvector_centrality(G, max_iter=1000)
        except nx.PowerIterationFailedConvergence:
            eigenvector_cent = {n: 0.0 for n in G.nodes()} # Default 0 jika gagal konvergen

        # Susun ulang JSON Node agar menyertakan object "metrics"
        nodes_output = []
        for n in G.nodes():
            nodes_output.append({
                "id": n,
                "attributes": G.nodes[n],
                "metrics": {
                    "degree": degree_cent.get(n, 0.0),
                    "betweenness": betweenness_cent.get(n, 0.0),
                    "closeness": closeness_cent.get(n, 0.0),
                    "eigenvector": eigenvector_cent.get(n, 0.0)
                }
            })

        return {
            "message": f"Graf berhasil dibuat (Limit: {user_limit} User, {post_limit} Post).",
            "graph_info": {
                "nodes_count": G.number_of_nodes(),
                "edges_count": G.number_of_edges(),
                "nodes": nodes_output,
                "edges": [{"source": u, "target": v, "attributes": G.edges[u, v]} for u, v in G.edges()]
            }
        }
        
    except HTTPException:
        raise

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
        