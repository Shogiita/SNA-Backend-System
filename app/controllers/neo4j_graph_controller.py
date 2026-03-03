import networkx as nx
from fastapi import HTTPException
from app.database import neo4j_driver

async def create_graph_from_neo4j(limit: int = 1000):
    """
    Mengambil data graf (Nodes & Edges) dari Neo4j 
    dan menghitung metrik Centrality menggunakan NetworkX.
    """
    try:
        # Query Cypher untuk mengambil Node User, Node KawanSS, dan Relasi POSTED
        query = """
        MATCH (u:User)-[r:POSTED]->(k:KawanSS)
        RETURN u.id AS user_id, u.nama AS user_name, k.id AS post_id, k.title AS post_title
        LIMIT $limit
        """
        
        with neo4j_driver.session() as session:
            result = session.run(query, limit=limit)
            records = list(result)
            
        if not records:
            raise HTTPException(status_code=404, detail="Tidak ada relasi data (POSTED) yang ditemukan di Neo4j.")

        G = nx.DiGraph()
        
        # 1. Bangun Graf dari Hasil Query Neo4j
        for record in records:
            u_id = record['user_id']
            u_name = record['user_name'] or "Unknown"
            p_id = record['post_id']
            p_title = record['post_title'] or "No Title"
            
            u_node = f"user_{u_id}"
            p_node = f"post_{p_id}"
            
            # Tambahkan Node User
            if not G.has_node(u_node):
                G.add_node(u_node, type="user", name=u_name, neo4j_id=u_id)
                
            # Tambahkan Node Post
            if not G.has_node(p_node):
                G.add_node(p_node, type="post", title=p_title, neo4j_id=p_id)
                
            # Tambahkan Relasi (Edge)
            G.add_edge(u_node, p_node, relation="POSTED")

        # 2. Perhitungan Centrality Metrics
        degree_cent = nx.degree_centrality(G)
        betweenness_cent = nx.betweenness_centrality(G)
        closeness_cent = nx.closeness_centrality(G)
        
        try:
            eigenvector_cent = nx.eigenvector_centrality(G, max_iter=1000)
        except nx.PowerIterationFailedConvergence:
            eigenvector_cent = {n: 0.0 for n in G.nodes()}

        # 3. Format Output JSON
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
            "message": f"Graf SNA berhasil dibuat dari database Neo4j (Limit: {limit} relasi).",
            "graph_info": {
                "nodes_count": G.number_of_nodes(),
                "edges_count": G.number_of_edges(),
                "nodes": nodes_output,
                "edges": [{"source": u, "target": v, "attributes": G.edges[u, v]} for u, v in G.edges()]
            }
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Gagal memproses graf Neo4j: {str(e)}")