import networkx as nx
import igraph as ig
import leidenalg as la


def detect_leiden_communities(
    graph: nx.Graph | nx.DiGraph,
    weight_attr: str = "weight",
) -> dict:
    if graph.number_of_nodes() == 0:
        return {}

    if graph.number_of_edges() == 0:
        return {
            node: community_id
            for community_id, node in enumerate(graph.nodes())
        }

    import igraph as ig
    import leidenalg as la

    working_graph = graph.to_undirected() if graph.is_directed() else graph.copy()

    node_list = list(working_graph.nodes())
    node_to_index = {
        node: index
        for index, node in enumerate(node_list)
    }

    ig_graph = ig.Graph()
    ig_graph.add_vertices(len(node_list))

    edges = []
    weights = []

    for source, target, data in working_graph.edges(data=True):
        edges.append((
            node_to_index[source],
            node_to_index[target]
        ))

        try:
            weights.append(float(data.get(weight_attr, 1)))
        except (TypeError, ValueError):
            weights.append(1.0)

    ig_graph.add_edges(edges)
    ig_graph.es["weight"] = weights

    partition = la.find_partition(
        ig_graph,
        la.ModularityVertexPartition,
        weights=ig_graph.es["weight"],
        seed=42
    )

    community_map = {}

    for community_id, members in enumerate(partition):
        for node_index in members:
            community_map[node_list[node_index]] = community_id

    return community_map

def apply_leiden_communities(
    graph: nx.Graph | nx.DiGraph,
    weight_attr: str = "weight",
    community_attr: str = "community",
) -> dict:
    community_map = detect_leiden_communities(
        graph=graph,
        weight_attr=weight_attr
    )

    nx.set_node_attributes(
        graph,
        community_map,
        community_attr
    )

    return community_map


def get_leiden_communities(
    graph: nx.Graph | nx.DiGraph,
    weight_attr: str = "weight",
) -> dict[int, list]:
    community_map = detect_leiden_communities(
        graph=graph,
        weight_attr=weight_attr
    )

    communities: dict[int, list] = {}

    for node, community_id in community_map.items():
        communities.setdefault(community_id, []).append(node)

    return communities


def _fallback_greedy_modularity(
    graph: nx.Graph | nx.DiGraph,
    weight_attr: str = "weight",
) -> dict:
    """
    Fallback jika igraph / leidenalg belum ter-install.
    Ini bukan Leiden, tetapi digunakan agar program tetap berjalan.
    """

    if graph.number_of_nodes() == 0:
        return {}

    if graph.number_of_edges() == 0:
        return {
            node: community_id
            for community_id, node in enumerate(graph.nodes())
        }

    working_graph = graph.to_undirected() if graph.is_directed() else graph.copy()

    communities = nx.community.greedy_modularity_communities(
        working_graph,
        weight=weight_attr
    )

    community_map = {}

    for community_id, members in enumerate(communities):
        for node in members:
            community_map[node] = community_id

    return community_map