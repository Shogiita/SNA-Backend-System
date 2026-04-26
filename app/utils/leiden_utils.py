import networkx as nx
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

    working_graph = graph.to_undirected() if graph.is_directed() else graph.copy()

    communities = nx.community.louvain_communities(
        working_graph,
        weight=weight_attr,
        seed=42
    )

    community_map = {}

    for community_id, members in enumerate(communities):
        for node in members:
            community_map[node] = community_id

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