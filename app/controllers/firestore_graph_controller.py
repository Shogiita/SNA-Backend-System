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
    """
    Logika untuk membangun graf dari koleksi 'users' dan 'kawanss' di Firestore.
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
                G.add_node(f"user_{user_name}", type="user", name=user_name, firestore_id=user.get('id'))

        for post in posts:
            post_id = post.get('id')
            author_name = post.get('accountName')
            
            if post_id:
                G.add_node(f"post_{post_id}", type="post", author=author_name, title=post.get('title'))
                
                if author_name and G.has_node(f"user_{author_name}"):
                    G.add_edge(f"user_{author_name}", f"post_{post_id}", relation="AUTHORED")
                
                reply_to_id = post.get('reply_to_id') 
                if reply_to_id and G.has_node(f"post_{reply_to_id}"):
                    G.add_edge(f"post_{post_id}", f"post_{reply_to_id}", relation="REPLIED_TO")

        nodes_for_json = [{"id": n, "attributes": G.nodes[n]} for n in G.nodes()]
        edges_for_json = [{"source": u, "target": v, "attributes": G.edges[u, v]} for u, v in G.edges()]

        return {
            "message": "Graf dari data Firestore 'users' dan 'kawanss' berhasil dibuat",
            "source_collections": ["users", "kawanss"],
            "graph_info": {
                "nodes_count": G.number_of_nodes(),
                "edges_count": G.number_of_edges(),
                "nodes": nodes_for_json,
                "edges": edges_for_json
            }
        }
    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=f"Gagal memproses graf dari Firestore: {str(e)}")

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