import re
import networkx as nx


DEFAULT_IGNORED_HASHTAGS = {
    "newss",
    "suarasurabayamedia",
    "ssemenit",
    "suarasurabaya",
    "beritasuarasurabaya",
    "infografiss",
}


DEFAULT_IGNORED_APP_USERS = {
    "",
    "unknown",
    "unknown user",
    "none",
    "null",
    "nan",
    "user_unknown_user",
    "user_unknown",
}


DEFAULT_IGNORED_INSTAGRAM_USERS = {
    "suarasurabayamedia",
    "suara_surabaya",
    "suarasurabaya",
    "suara_surabaya_official",
    "suarasurabayaofficial",
}


HASHTAG_NORMALIZE_REGEX = re.compile(r"[^a-z0-9_]")


def normalize_text(value: str | None) -> str:
    if value is None:
        return ""

    return str(value).strip().lower()


def normalize_hashtag(tag: str | None) -> str:
    """
    Normalize hashtag:
    - lowercase
    - remove leading #
    - strip spaces
    - keep only a-z, 0-9, underscore
    """
    tag = normalize_text(tag)

    if tag.startswith("#"):
        tag = tag[1:]

    tag = HASHTAG_NORMALIZE_REGEX.sub("", tag)

    return tag


def is_ignored_hashtag(tag: str | None) -> bool:
    normalized = normalize_hashtag(tag)

    return (
        not normalized
        or normalized in DEFAULT_IGNORED_HASHTAGS
        or len(normalized) <= 1
    )


def is_ignored_app_user(username: str | None) -> bool:
    normalized = normalize_text(username)
    normalized = normalized.replace("@", "")

    return (
        not normalized
        or normalized in DEFAULT_IGNORED_APP_USERS
        or normalized.startswith("unknown")
        or normalized.startswith("user_unknown")
    )

def is_ignored_instagram_user(username: str | None) -> bool:
    normalized = normalize_text(username).replace("@", "")

    return (
        not normalized
        or normalized in DEFAULT_IGNORED_INSTAGRAM_USERS
    )


def is_ignored_node(
    node_id: str,
    node_data: dict,
    source: str,
) -> bool:
    node_type = normalize_text(node_data.get("type"))
    label = normalize_text(
        node_data.get("label")
        or node_data.get("name")
        or node_data.get("username")
        or node_id
    )

    clean_label = label.replace("@", "")

    if node_type != "user":
        return False

    if source == "app":
        return is_ignored_app_user(clean_label)

    if source == "instagram":
        return is_ignored_instagram_user(clean_label)

    return False


def clean_graph_nodes(
    graph: nx.Graph | nx.DiGraph,
    source: str,
) -> nx.Graph | nx.DiGraph:
    """
    Remove ignored user nodes before visualization and centrality.
    """
    nodes_to_remove = [
        node
        for node, data in graph.nodes(data=True)
        if is_ignored_node(str(node), data, source)
    ]

    graph.remove_nodes_from(nodes_to_remove)
    graph.remove_nodes_from(list(nx.isolates(graph)))

    return graph


def prepare_graph_for_centrality(graph: nx.Graph | nx.DiGraph):
    """
    Set distance = 1 / weight.
    Weighted centrality path should use distance,
    not raw weight, because higher weight means stronger/closer relation.
    """
    for _, _, data in graph.edges(data=True):
        weight = data.get("weight", 1)

        try:
            weight = float(weight)
        except Exception:
            weight = 1.0

        if weight <= 0:
            weight = 1.0

        data["weight"] = weight
        data["distance"] = 1.0 / weight

    return graph


def calculate_centrality(graph: nx.Graph | nx.DiGraph) -> dict:
    """
    Centrality dihitung setelah node unwanted dibersihkan.

    degree:
        untuk directed graph memakai total degree centrality.
    in_degree/out_degree:
        khusus directed graph.
    betweenness:
        memakai distance = 1 / weight.
    closeness:
        memakai distance = 1 / weight.
    eigenvector:
        fallback ke pagerank jika tidak konvergen.
    """
    if graph.number_of_nodes() == 0:
        return {
            "degree": {},
            "in_degree": {},
            "out_degree": {},
            "betweenness": {},
            "closeness": {},
            "eigenvector": {},
            "pagerank": {},
        }

    prepare_graph_for_centrality(graph)

    degree = nx.degree_centrality(graph)

    if graph.is_directed():
        in_degree = nx.in_degree_centrality(graph)
        out_degree = nx.out_degree_centrality(graph)
    else:
        in_degree = {}
        out_degree = {}

    total_nodes = graph.number_of_nodes()
    k_samples = min(200, total_nodes) if total_nodes > 500 else None

    betweenness = nx.betweenness_centrality(
        graph,
        weight="distance",
        k=k_samples,
        normalized=True,
        seed=42 if k_samples else None,
    )

    closeness = nx.closeness_centrality(
        graph,
        distance="distance",
        wf_improved=True,
    )

    try:
        eigenvector = nx.eigenvector_centrality(
            graph,
            weight="weight",
            max_iter=2000,
            tol=1e-06,
        )
    except Exception:
        try:
            eigenvector = nx.eigenvector_centrality_numpy(
                graph,
                weight="weight",
            )
        except Exception:
            eigenvector = {
                node: 0.0
                for node in graph.nodes()
            }

    try:
        pagerank = nx.pagerank(
            graph,
            weight="weight",
            max_iter=200,
        )
    except Exception:
        pagerank = {
            node: 0.0
            for node in graph.nodes()
        }

    return {
        "degree": degree,
        "in_degree": in_degree,
        "out_degree": out_degree,
        "betweenness": betweenness,
        "closeness": closeness,
        "eigenvector": eigenvector,
        "pagerank": pagerank,
    }