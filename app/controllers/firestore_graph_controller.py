import asyncio
import networkx as nx
from fastapi import HTTPException
from . import user_controller, post_controller

async def create_graph_from_firestore():
    """
    Logika untuk membangun graf dari koleksi 'users' dan 'kawanss' di Firestore.
    """
    try:
        # 1. Ambil semua data dari Firestore
        users_task = user_controller.get_all_users_from_db()
        posts_task = post_controller.get_all_posts_from_db()
        users, posts = await asyncio.gather(users_task, posts_task)

        if not posts:
            raise HTTPException(status_code=404, detail="Tidak ada data post di koleksi 'kawanss' untuk membuat graf.")

        # 2. Buat objek Graf Terarah (DiGraph)
        G = nx.DiGraph()

        # 3. Tambahkan Nodes dan Edges sesuai struktur data Anda
        
        # Tambahkan node untuk setiap user dari koleksi 'users'
        for user in users:
            # Menggunakan field 'nama' sebagai nama pengguna
            user_name = user.get('nama')
            if user_name:
                G.add_node(f"user_{user_name}", type="user", name=user_name, firestore_id=user.get('id'))

        # Tambahkan node untuk setiap post dari koleksi 'kawanss'
        for post in posts:
            post_id = post.get('id')
            # Menggunakan field 'accountName' sebagai nama author post
            author_name = post.get('accountName')
            
            if post_id:
                G.add_node(f"post_{post_id}", type="post", author=author_name, title=post.get('title'))
                
                # Edge: Pengguna -> Post (AUTHORED)
                # Pastikan node pengguna sudah ada sebelum membuat hubungan
                if author_name and G.has_node(f"user_{author_name}"):
                    G.add_edge(f"user_{author_name}", f"post_{post_id}", relation="AUTHORED")
                
                # Edge: Post -> Post (REPLIED_TO) - Ini adalah asumsi
                # Ganti 'reply_to_id' jika nama field di database Anda berbeda
                reply_to_id = post.get('reply_to_id') 
                if reply_to_id and G.has_node(f"post_{reply_to_id}"):
                    G.add_edge(f"post_{post_id}", f"post_{reply_to_id}", relation="REPLIED_TO")

        # 4. Format output JSON
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