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

async def create_graph_from_firestore():
    try:
        # Fetch data sinkron (tunggu sampai selesai sesuai proposal)
        users_task = user_controller.get_all_users_from_db()
        posts_task = post_controller.get_all_posts_from_db()
        users, posts = await asyncio.gather(users_task, posts_task)

        if not posts:
            raise HTTPException(status_code=404, detail="Data tidak ditemukan.")

        G = nx.DiGraph()
        
        # 1. Proses Node User dengan Filter "Unknown User"
        user_lookup = {} 
        for user in users:
            u_name = user.get('nama')
            
            # Pengecekan: Abaikan jika nama kosong atau "Unknown User" (case-insensitive)
            if u_name and u_name.strip().lower() != "unknown user":
                node_id = f"user_{u_name}"
                user_lookup[u_name] = node_id
                G.add_node(node_id, type="user", name=u_name, firestore_id=user.get('id'))

        edges_to_add = []
        post_nodes = set()

        # 2. Proses Node Post dan Relasi AUTHORED
        for post in posts:
            pid = post.get('id')
            if not pid: continue
            
            author = post.get('accountName')
            
            # Pengecekan: Jangan masukkan post ke dalam graf jika author "Unknown User" atau kosong
            if not author or author.strip().lower() == "unknown user":
                continue
                
            p_node_id = f"post_{pid}"
            post_nodes.add(p_node_id)
            G.add_node(p_node_id, type="post", author=author, title=post.get('title'))
            
            # Tambahkan relasi hanya jika author ada di daftar user yang valid
            if author in user_lookup:
                edges_to_add.append((user_lookup[author], p_node_id, {"relation": "AUTHORED"}))

        # 3. Proses Relasi REPLIED_TO
        for post in posts:
            pid = post.get('id')
            p_node_id = f"post_{pid}"
            
            # Lewati proses reply jika post ini sebelumnya sudah diabaikan (karena Unknown User)
            if p_node_id not in post_nodes:
                continue
                
            reply_id = post.get('reply_to_id')
            if reply_id:
                target_pid = f"post_{reply_id}"
                # Pastikan post yang dibalas juga merupakan post yang valid di dalam graf
                if target_pid in post_nodes:
                    edges_to_add.append((p_node_id, target_pid, {"relation": "REPLIED_TO"}))

        G.add_edges_from(edges_to_add)

        return {
            "message": "Graf berhasil dibuat secara sinkron (Unknown User telah difilter)",
            "graph_info": {
                "nodes_count": G.number_of_nodes(),
                "edges_count": G.number_of_edges(),
                "nodes": [{"id": n, "attributes": G.nodes[n]} for n in G.nodes()],
                "edges": [{"source": u, "target": v, "attributes": G.edges[u, v]} for u, v in G.edges()]
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
        
async def create_graph_from_firestore_pajek():
    """
    Logika untuk membangun graf dari koleksi 'users' dan 'kawanss' di Firestore
    dan mengembalikannya dalam format teks Pajek (.net).
    """
    try:
        users_task = user_controller.get_all_users_from_db()
        posts_task = post_controller.get_all_posts_from_db()
        users, posts = await asyncio.gather(users_task, posts_task)

        if not posts:
            raise HTTPException(status_code=404, detail="Tidak ada data post di koleksi 'kawanss' untuk membuat graf.")

        G = nx.DiGraph()

        for user in users:
            user_name = user.get('nama')
            if user_name:
                G.add_node(f"user_{user_name}", type="user", name=user_name)

        for post in posts:
            post_id = post.get('id')
            author_name = post.get('accountName')
            if post_id:
                G.add_node(f"post_{post_id}", type="post", author=author_name)
                if author_name and G.has_node(f"user_{author_name}"):
                    G.add_edge(f"user_{author_name}", f"post_{post_id}")
                
                reply_to_id = post.get('reply_to_id')
                if reply_to_id and G.has_node(f"post_{reply_to_id}"):
                    G.add_edge(f"post_{post_id}", f"post_{reply_to_id}")

        pajek_output = _format_to_pajek(G)

        return Response(content=pajek_output, media_type="text/plain")

    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=f"Gagal memproses graf dari Firestore untuk Pajek: {str(e)}")