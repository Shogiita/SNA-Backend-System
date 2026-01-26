import networkx as nx
from fastapi import HTTPException, Response
import leidenalg as la
import igraph as ig
import random

def _generate_large_dummy_data():
    """Helper function to generate a larger, CONSISTENT set of dummy data."""
    
    random.seed(42)

    first_names = [
        "Adi", "Bima", "Citra", "Dewi", "Eko", "Fajar", "Gita", "Hana", 
        "Indra", "Joko", "Kiki", "Lina", "Maya", "Nino", "Olga", "Putri", 
        "Rian", "Sari", "Tono", "Utami", "Vino", "Wati", "Yudi", "Zahra", "Budi"
    ]
    users = [{"user_id": i + 1, "username": name} for i, name in enumerate(first_names)]
    usernames = [user["username"] for user in users]

    posts = []
    for i in range(150):
        post_id = 301 + i
        author = random.choice(usernames)
        post = {"post_id": post_id, "author": author, "content": f"Ini adalah isi dari post #{post_id}."}

        if i > 0 and random.random() < 0.8:
            reply_to_post = random.choice(posts[-30:])
            post["reply_to_post_id"] = reply_to_post["post_id"]

        posts.append(post)

    return users, posts

async def create_social_graph():
    try:
        users, posts = _generate_large_dummy_data()

        G = nx.Graph()

        for user in users:
            G.add_node(f"user_{user['username']}", type="user", name=user['username'])
        for post in posts:
            G.add_node(f"post_{post['post_id']}", type="post", author=post['author'])
            G.add_edge(f"user_{post['author']}", f"post_{post['post_id']}", relation="AUTHORED")
            if "reply_to_post_id" in post:
                G.add_edge(f"post_{post['post_id']}", f"post_{post['reply_to_post_id']}", relation="REPLIED_TO")

        centrality = nx.betweenness_centrality(G)
        sorted_centrality = sorted(centrality.items(), key=lambda item: item[1], reverse=True)

        if G.number_of_nodes() > 0:
            ig_graph = ig.Graph.from_networkx(G)
            partition = la.find_partition(ig_graph, la.ModularityVertexPartition)
            node_names = ig_graph.vs["_nx_name"]
            leiden_communities = {}
            for community_id, community in enumerate(partition):
                leiden_communities[community_id] = [node_names[node_index] for node_index in community]
        else:
            leiden_communities = {}

        nodes_for_json = [{"id": n, "attributes": G.nodes[n]} for n in G.nodes()]
        edges_for_json = [{"source": u, "target": v, "attributes": G.edges[u, v]} for u, v in G.edges()]

        return {
            "message": "Graf besar KONSISTEN berhasil dianalisis dengan Centrality dan Komunitas Leiden",
            "graph_info": {
                "nodes_count": G.number_of_nodes(),
                "edges_count": G.number_of_edges(),
                "nodes": nodes_for_json,
                "edges": edges_for_json
            },
            "analysis_results": {
                "betweenness_centrality": dict(sorted_centrality),
                "leiden_communities": leiden_communities
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Gagal memproses graf: {str(e)}")

def _format_to_pajek(graph: nx.Graph) -> str:
    pajek_str = f"*Vertices {graph.number_of_nodes()}\n"
    
    node_to_id = {node: i + 1 for i, node in enumerate(graph.nodes())}
    
    for node, node_id in node_to_id.items():
        pajek_str += f'{node_id} "{node}"\n'

    pajek_str += "*Edges\n"
        
    for u, v in graph.edges():
        pajek_str += f"{node_to_id[u]} {node_to_id[v]}\n"
        
    return pajek_str

async def create_social_graph_pajek():
    try:
        users, posts = _generate_large_dummy_data()

        G = nx.Graph()

        for user in users:
            G.add_node(f"user_{user['username']}", type="user", name=user['username'])
        for post in posts:
            G.add_node(f"post_{post['post_id']}", type="post", author=post['author'])
            G.add_edge(f"user_{post['author']}", f"post_{post['post_id']}", relation="AUTHORED")
            if "reply_to_post_id" in post:
                G.add_edge(f"post_{post['post_id']}", f"post_{post['reply_to_post_id']}", relation="REPLIED_TO")

        pajek_output = _format_to_pajek(G)

        return Response(content=pajek_output, media_type="text/plain")
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Gagal memproses graf untuk Pajek: {str(e)}")