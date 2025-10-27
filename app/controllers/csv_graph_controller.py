import networkx as nx
from fastapi import HTTPException, Response
import pandas as pd
from typing import Literal

CSV_FILE_PATH = "twitter_dataset.csv"

async def create_graph_from_csv(output_format: Literal['json', 'pajek'] = 'json'):
    """
    Logika untuk membangun graf dari dataset twitter_dataset.csv.
    Mendukung output dalam format JSON atau Pajek.
    """
    try:
        try:
            df = pd.read_csv(CSV_FILE_PATH, nrows=500)
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail=f"Dataset tidak ditemukan di path: {CSV_FILE_PATH}")

        G = nx.DiGraph()

        for index, row in df.iterrows():
            username = row['Username']
            tweet_id = row['Tweet_ID']

            user_node_id = f"user_{username}"
            if not G.has_node(user_node_id):
                G.add_node(user_node_id, type="user", name=username)

            post_node_id = f"post_{tweet_id}"
            G.add_node(post_node_id, type="post", author=username, text=row['Text'], likes=row['Likes'])

            G.add_edge(user_node_id, post_node_id, relation="AUTHORED")

        if output_format == 'pajek':
            pajek_str = f"*Vertices {G.number_of_nodes()}\n"
            node_to_id = {node: i + 1 for i, node in enumerate(G.nodes())}
            for node, i in node_to_id.items():
                pajek_str += f'{i} "{node}"\n'
            
            pajek_str += "*Arcs\n"
            for u, v in G.edges():
                pajek_str += f"{node_to_id[u]} {node_to_id[v]}\n"
            
            return Response(content=pajek_str, media_type="text/plain")

        nodes_for_json = [{"id": n, **G.nodes[n]} for n in G.nodes()]
        edges_for_json = [{"source": u, "target": v, **G.edges[u, v]} for u, v in G.edges()]

        return {
            "message": "Graf dari twitter_dataset.csv berhasil dibuat",
            "source_file": CSV_FILE_PATH,
            "graph": {
                "nodes": nodes_for_json,
                "edges": edges_for_json
            }
        }
    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=f"Ggagal memproses graf dari CSV: {str(e)}")
