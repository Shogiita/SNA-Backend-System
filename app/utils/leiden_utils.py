import re
from collections import Counter, defaultdict
from typing import Any

import networkx as nx
import igraph as ig
import leidenalg as la


HASHTAG_REGEX = re.compile(r"#[\w_]+", re.UNICODE)
MENTION_REGEX = re.compile(r"@[A-Za-z0-9_.]+")


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

    try:
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
                node_to_index[target],
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
            seed=42,
        )

        community_map = {}

        for community_id, members in enumerate(partition):
            for node_index in members:
                community_map[node_list[node_index]] = community_id

        return community_map

    except Exception:
        return _fallback_greedy_modularity(
            graph=graph,
            weight_attr=weight_attr,
        )


def apply_leiden_communities(
    graph: nx.Graph | nx.DiGraph,
    weight_attr: str = "weight",
    community_attr: str = "community",
) -> dict:
    community_map = detect_leiden_communities(
        graph=graph,
        weight_attr=weight_attr,
    )

    nx.set_node_attributes(
        graph,
        community_map,
        community_attr,
    )

    enrich_graph_with_community_labels(
        graph=graph,
        community_map=community_map,
        community_attr=community_attr,
    )

    return community_map


def get_leiden_communities(
    graph: nx.Graph | nx.DiGraph,
    weight_attr: str = "weight",
) -> dict[int, list]:
    community_map = detect_leiden_communities(
        graph=graph,
        weight_attr=weight_attr,
    )

    communities: dict[int, list] = {}

    for node, community_id in community_map.items():
        communities.setdefault(community_id, []).append(node)

    return communities


def enrich_graph_with_community_labels(
    graph: nx.Graph | nx.DiGraph,
    community_map: dict | None = None,
    community_attr: str = "community",
) -> list[dict[str, Any]]:
    if community_map is None:
        community_map = {
            node: data.get(community_attr)
            for node, data in graph.nodes(data=True)
            if data.get(community_attr) is not None
        }

    community_summaries = build_community_summaries(
        graph=graph,
        community_map=community_map,
    )

    summary_by_id = {
        summary["id"]: summary
        for summary in community_summaries
    }

    for node, community_id in community_map.items():
        if node not in graph.nodes:
            continue

        summary = summary_by_id.get(community_id)

        if summary is None:
            continue

        graph.nodes[node]["community_label"] = summary["label"]
        graph.nodes[node]["community_description"] = summary["description"]
        graph.nodes[node]["top_hashtags"] = summary["top_hashtags"]
        graph.nodes[node]["top_posts"] = summary["top_posts"]
        graph.nodes[node]["top_mentions"] = summary["top_mentions"]
        graph.nodes[node]["dominant_type"] = summary["dominant_type"]

    graph.graph["communities"] = community_summaries
    graph.graph["community_summaries"] = community_summaries
    graph.graph["total_communities"] = len(community_summaries)

    return community_summaries


def build_community_summaries(
    graph: nx.Graph | nx.DiGraph,
    community_map: dict,
) -> list[dict[str, Any]]:
    grouped_nodes: dict[Any, list[Any]] = defaultdict(list)

    for node, community_id in community_map.items():
        if community_id is None:
            continue

        grouped_nodes[community_id].append(node)

    summaries = []

    for community_id, nodes in grouped_nodes.items():
        summary = _build_single_community_summary(
            graph=graph,
            community_id=community_id,
            nodes=nodes,
        )
        summaries.append(summary)

    summaries.sort(key=lambda item: _sort_community_key(item["id"]))

    return summaries


def _build_single_community_summary(
    graph: nx.Graph | nx.DiGraph,
    community_id: Any,
    nodes: list[Any],
) -> dict[str, Any]:
    hashtag_counter: Counter[str] = Counter()
    post_counter: Counter[str] = Counter()
    mention_counter: Counter[str] = Counter()
    type_counter: Counter[str] = Counter()

    for node in nodes:
        node_data = dict(graph.nodes[node]) if node in graph.nodes else {}
        node_type = _extract_node_type(node=node, node_data=node_data)

        if node_type:
            type_counter[_normalize_text(node_type).lower()] += 1

        for hashtag in _extract_hashtags(node=node, node_data=node_data, node_type=node_type):
            hashtag_counter[hashtag] += 1

        for mention in _extract_mentions(node=node, node_data=node_data):
            mention_counter[mention] += 1

        post_label = _extract_post_label(
            node=node,
            node_data=node_data,
            node_type=node_type,
        )

        if post_label:
            post_counter[post_label] += 1

    top_hashtags = _top_items(hashtag_counter, limit=3)
    top_posts = _top_items(post_counter, limit=1)
    top_mentions = _top_items(mention_counter, limit=3)
    dominant_types = _top_items(type_counter, limit=1)

    label, description = _build_label_and_description(
        community_id=community_id,
        top_hashtags=top_hashtags,
        top_posts=top_posts,
        top_mentions=top_mentions,
        dominant_types=dominant_types,
    )

    return {
        "id": community_id,
        "label": label,
        "description": description,
        "top_hashtags": top_hashtags,
        "top_posts": top_posts,
        "top_mentions": top_mentions,
        "dominant_type": dominant_types[0] if dominant_types else None,
        "count": len(nodes),
    }


def _build_label_and_description(
    community_id: Any,
    top_hashtags: list[str],
    top_posts: list[str],
    top_mentions: list[str],
    dominant_types: list[str],
) -> tuple[str, str]:
    if top_posts and top_hashtags:
        label = f"Post: {top_posts[0]}\n{' '.join(top_hashtags)}"
        description = (
            "Community ini dominan terbentuk dari interaksi pada post "
            f"dengan hashtag utama {', '.join(top_hashtags)}."
        )
        return label, description

    if top_posts:
        label = f"Post: {top_posts[0]}"
        description = "Community ini dominan terbentuk dari interaksi pada satu post."
        return label, description

    if top_hashtags:
        label = f"Hashtag: {' '.join(top_hashtags)}"
        description = (
            "Community ini memiliki hashtag dominan "
            f"{', '.join(top_hashtags)}."
        )
        return label, description

    if top_mentions:
        label = f"Mention: {', '.join(top_mentions)}"
        description = (
            "Community ini dominan terbentuk dari aktivitas mention "
            f"{', '.join(top_mentions)}."
        )
        return label, description

    if dominant_types:
        readable_type = _to_readable_type(dominant_types[0])
        label = f"Community {readable_type}"
        description = f"Community ini didominasi oleh node bertipe {readable_type}."
        return label, description

    community_number = _get_community_display_number(community_id)
    label = f"Community {community_number}"
    description = "Community hasil deteksi Leiden."
    return label, description


def _extract_node_type(
    node: Any,
    node_data: dict[str, Any],
) -> str:
    values = [
        node_data.get("type"),
        node_data.get("node_type"),
        node_data.get("label_type"),
        node_data.get("category"),
        node_data.get("group"),
        node_data.get("labels"),
        node_data.get("label_name"),
        node_data.get("source_type"),
    ]

    text = _first_text(values)

    if text:
        return text

    node_text = str(node).lower()

    if "hashtag" in node_text or node_text.startswith("#"):
        return "hashtag"

    if "post" in node_text or "content" in node_text:
        return "post"

    if "comment" in node_text or "komentar" in node_text:
        return "comment"

    if "mention" in node_text or node_text.startswith("@"):
        return "mention"

    if "user" in node_text:
        return "user"

    return ""


def _extract_hashtags(
    node: Any,
    node_data: dict[str, Any],
    node_type: str,
) -> list[str]:
    hashtags: set[str] = set()

    explicit_fields = [
        node_data.get("hashtags"),
        node_data.get("hashtag"),
        node_data.get("tags"),
        node_data.get("tag"),
        node_data.get("top_hashtags"),
    ]

    for field in explicit_fields:
        for value in _flatten_values(field):
            text = _value_to_text(value)

            found_hashtags = _find_hashtags(text)

            if found_hashtags:
                hashtags.update(found_hashtags)
            elif text.strip():
                normalized = _normalize_hashtag(text)

                if normalized:
                    hashtags.add(normalized)

    text_fields = [
        node_data.get("label"),
        node_data.get("name"),
        node_data.get("title"),
        node_data.get("caption"),
        node_data.get("content"),
        node_data.get("text"),
        node_data.get("description"),
        node_data.get("message"),
        node_data.get("body"),
        node_data.get("post_caption"),
        node_data.get("post_content"),
        node_data.get("post_text"),
    ]

    for field in text_fields:
        text = _value_to_text(field)
        hashtags.update(_find_hashtags(text))

    node_text = str(node)

    if "hashtag" in node_type.lower() or node_text.startswith("#"):
        hashtag_text = _first_text([
            node_data.get("label"),
            node_data.get("name"),
            node_data.get("title"),
            node_data.get("id"),
            node,
        ])

        normalized = _normalize_hashtag(hashtag_text)

        if normalized:
            hashtags.add(normalized)

    return sorted(hashtags)


def _extract_mentions(
    node: Any,
    node_data: dict[str, Any],
) -> list[str]:
    mentions: set[str] = set()

    explicit_fields = [
        node_data.get("mentions"),
        node_data.get("mention"),
        node_data.get("mentioned_users"),
        node_data.get("top_mentions"),
    ]

    for field in explicit_fields:
        for value in _flatten_values(field):
            mention = _normalize_mention(_value_to_text(value))

            if mention:
                mentions.add(mention)

    text_fields = [
        node_data.get("label"),
        node_data.get("name"),
        node_data.get("username"),
        node_data.get("caption"),
        node_data.get("content"),
        node_data.get("text"),
        node_data.get("description"),
        node_data.get("message"),
        node_data.get("body"),
    ]

    for field in text_fields:
        text = _value_to_text(field)

        for match in MENTION_REGEX.findall(text):
            mention = _normalize_mention(match)

            if mention:
                mentions.add(mention)

    node_text = str(node)

    if node_text.startswith("@"):
        mention = _normalize_mention(node_text)

        if mention:
            mentions.add(mention)

    return sorted(mentions)


def _extract_post_label(
    node: Any,
    node_data: dict[str, Any],
    node_type: str,
) -> str:
    lowered_type = node_type.lower()
    node_text = str(node).lower()

    is_post_node = (
        "post" in lowered_type
        or "content" in lowered_type
        or "konten" in lowered_type
        or node_text.startswith("post")
        or "post:" in node_text
        or "post_" in node_text
    )

    has_post_specific_field = any(
        value is not None and str(value).strip() != ""
        for value in [
            node_data.get("post_title"),
            node_data.get("post_caption"),
            node_data.get("post_content"),
            node_data.get("post_text"),
            node_data.get("post_id"),
        ]
    )

    if not is_post_node and not has_post_specific_field:
        return ""

    post_text = _first_text([
        node_data.get("post_title"),
        node_data.get("post_caption"),
        node_data.get("post_content"),
        node_data.get("post_text"),
        node_data.get("caption"),
        node_data.get("title"),
        node_data.get("content"),
        node_data.get("text"),
        node_data.get("description"),
        node_data.get("message"),
        node_data.get("body"),
        node_data.get("label"),
        node_data.get("name"),
    ])

    if post_text:
        return _truncate_text(post_text, max_length=55)

    post_id = _first_text([
        node_data.get("post_id"),
        node_data.get("id"),
        node,
    ])

    if post_id:
        return f"Post {post_id}"

    return ""


def _find_hashtags(text: str) -> list[str]:
    if not text:
        return []

    return sorted({
        _normalize_hashtag(match)
        for match in HASHTAG_REGEX.findall(text)
        if match
    })


def _normalize_hashtag(value: str) -> str:
    text = _normalize_text(value)

    if not text:
        return ""

    text = text.strip("#")
    text = re.sub(r"[^\w_]+", "", text, flags=re.UNICODE)

    if not text:
        return ""

    return f"#{text.lower()}"


def _normalize_mention(value: str) -> str:
    text = _normalize_text(value)

    if not text:
        return ""

    if not text.startswith("@"):
        text = f"@{text}"

    text = re.sub(r"[^A-Za-z0-9_@.]+", "", text)

    if len(text) <= 1:
        return ""

    return text.lower()


def _top_items(counter: Counter[str], limit: int) -> list[str]:
    return [
        key
        for key, _ in sorted(
            counter.items(),
            key=lambda item: (-item[1], item[0]),
        )[:limit]
    ]


def _first_text(values: list[Any]) -> str:
    for value in values:
        text = _value_to_text(value)

        if text.strip():
            return _normalize_text(text)

    return ""


def _value_to_text(value: Any) -> str:
    if value is None:
        return ""

    if isinstance(value, str):
        return value

    if isinstance(value, (int, float, bool)):
        return str(value)

    if isinstance(value, list):
        return " ".join(_value_to_text(item) for item in value)

    if isinstance(value, tuple):
        return " ".join(_value_to_text(item) for item in value)

    if isinstance(value, set):
        return " ".join(_value_to_text(item) for item in value)

    if isinstance(value, dict):
        return " ".join(_value_to_text(item) for item in value.values())

    return str(value)


def _flatten_values(value: Any) -> list[Any]:
    if value is None:
        return []

    if isinstance(value, list):
        result: list[Any] = []

        for item in value:
            result.extend(_flatten_values(item))

        return result

    if isinstance(value, tuple):
        result: list[Any] = []

        for item in value:
            result.extend(_flatten_values(item))

        return result

    if isinstance(value, set):
        result: list[Any] = []

        for item in value:
            result.extend(_flatten_values(item))

        return result

    if isinstance(value, dict):
        result: list[Any] = []

        for item in value.values():
            result.extend(_flatten_values(item))

        return result

    return [value]


def _normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", str(value)).strip()


def _truncate_text(text: str, max_length: int) -> str:
    cleaned = _normalize_text(text)

    if len(cleaned) <= max_length:
        return cleaned

    return f"{cleaned[:max_length].strip()}..."


def _to_readable_type(value: str) -> str:
    cleaned = (
        value.replace("_", " ")
        .replace("-", " ")
        .replace(":", " ")
        .strip()
    )

    cleaned = re.sub(r"\s+", " ", cleaned)

    if not cleaned:
        return "Node"

    return " ".join(word.capitalize() for word in cleaned.split(" "))


def _get_community_display_number(community_id: Any) -> Any:
    try:
        return int(community_id) + 1
    except Exception:
        return community_id


def _sort_community_key(value: Any) -> tuple[int, Any]:
    try:
        return 0, int(value)
    except Exception:
        return 1, str(value)


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
        weight=weight_attr,
    )

    community_map = {}

    for community_id, members in enumerate(communities):
        for node in members:
            community_map[node] = community_id

    return community_map