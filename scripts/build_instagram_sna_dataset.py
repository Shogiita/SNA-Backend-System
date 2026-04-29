import argparse
import csv
import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

DATE_FIELDS = [
    "created_at",
    "createdAt",
    "timestamp",
    "taken_at",
    "takenAt",
    "posted_at",
    "postedAt",
    "date",
]

POST_ID_FIELDS = [
    "post_id",
    "postId",
    "shortcode",
    "code",
    "media_id",
    "mediaId",
    "id",
    "_id",
]

TEXT_FIELDS = ["caption", "text", "content", "comment", "message", "body"]

HASHTAG_REGEX = re.compile(r"#([A-Za-z0-9_]+)")
MENTION_REGEX = re.compile(r"@([A-Za-z0-9_.]+)")
URL_REGEX = re.compile(r"https?://\S+")


def parse_date(value: Any) -> Optional[datetime]:
    if value is None:
        return None

    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    if isinstance(value, (int, float)):
        try:
            if value > 10_000_000_000:
                value = value / 1000
            return datetime.fromtimestamp(value, tz=timezone.utc)
        except Exception:
            return None

    raw = str(value).strip()
    if not raw:
        return None

    if raw.isdigit():
        try:
            number = int(raw)
            if number > 10_000_000_000:
                number = number / 1000
            return datetime.fromtimestamp(number, tz=timezone.utc)
        except Exception:
            pass

    try:
        raw_iso = raw.replace("Z", "+00:00")
        dt = datetime.fromisoformat(raw_iso)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        pass

    for fmt in [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%d-%m-%Y",
        "%m/%d/%Y",
    ]:
        try:
            return datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc)
        except Exception:
            continue

    return None


def first_value(doc: Dict[str, Any], fields: Iterable[str], default: Any = None) -> Any:
    for field in fields:
        value = doc.get(field)
        if value not in [None, ""]:
            return value
    return default


def normalize_id(value: Any, prefix: str) -> Optional[str]:
    if value is None:
        return None
    value = str(value).strip()
    if not value:
        return None
    value = value.replace("ObjectId(", "").replace(")", "").replace("'", "")
    return f"{prefix}:{value}"


def normalize_username(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, dict):
        value = first_value(value, ["username", "user_name", "name", "id"])
    value = str(value).strip().lower().replace("@", "")
    if not value:
        return None
    return f"user:{value}"


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value)
    text = URL_REGEX.sub("", text)
    return " ".join(text.split())


def extract_hashtags(text: str) -> List[str]:
    return sorted({f"hashtag:{tag.lower()}" for tag in HASHTAG_REGEX.findall(text or "")})


def extract_mentions(text: str) -> List[str]:
    return sorted({f"user:{mention.lower()}" for mention in MENTION_REGEX.findall(text or "")})


def get_doc_date(doc: Dict[str, Any]) -> Optional[datetime]:
    for field in DATE_FIELDS:
        dt = parse_date(doc.get(field))
        if dt:
            return dt
    return None


def read_csv(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as file:
        return [dict(row) for row in csv.DictReader(file)]


def read_json(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8") as file:
        data = json.load(file)

    if isinstance(data, list):
        return [row for row in data if isinstance(row, dict)]

    if isinstance(data, dict):
        for key in ["data", "items", "results", "posts", "comments", "replies", "likes"]:
            value = data.get(key)
            if isinstance(value, list):
                return [row for row in value if isinstance(row, dict)]
        return [data]

    return []


def read_table(path_value: Optional[str]) -> List[Dict[str, Any]]:
    if not path_value:
        return []

    path = Path(path_value)
    suffix = path.suffix.lower()

    if suffix == ".csv":
        return read_csv(path)
    if suffix == ".json":
        return read_json(path)

    raise SystemExit(f"Format file tidak didukung: {path}. Gunakan .csv atau .json")


def filter_last_days(rows: List[Dict[str, Any]], days: int) -> List[Dict[str, Any]]:
    since = datetime.now(timezone.utc) - timedelta(days=days)
    filtered = []

    for row in rows:
        dt = get_doc_date(row)
        if not dt or dt >= since:
            filtered.append(row)

    return filtered


def add_node(nodes: Dict[str, Dict[str, Any]], node_id: Optional[str], node_type: str, label: str, **attrs: Any) -> None:
    if not node_id:
        return
    if node_id not in nodes:
        nodes[node_id] = {
            "id": node_id,
            "type": node_type,
            "label": label or node_id,
            **attrs,
        }
    else:
        nodes[node_id].update({k: v for k, v in attrs.items() if v not in [None, ""]})


def add_edge(
    edge_counter: Counter,
    edge_attrs: Dict[Tuple[str, str, str], Dict[str, Any]],
    source: Optional[str],
    target: Optional[str],
    relation: str,
    **attrs: Any,
) -> None:
    if not source or not target or source == target:
        return

    key = (source, target, relation)
    edge_counter[key] += 1

    if key not in edge_attrs:
        edge_attrs[key] = attrs
    else:
        for attr_key, attr_value in attrs.items():
            if attr_value not in [None, ""]:
                edge_attrs[key][attr_key] = attr_value


def resolve_post_id(doc: Dict[str, Any]) -> Optional[str]:
    return normalize_id(first_value(doc, POST_ID_FIELDS), "post")


def resolve_post_owner(doc: Dict[str, Any]) -> Optional[str]:
    owner = first_value(
        doc,
        [
            "owner_username",
            "ownerUsername",
            "account_username",
            "accountUsername",
            "user_username",
            "userUsername",
            "author_username",
            "authorUsername",
            "username",
            "owner",
            "author",
            "user",
        ],
    )
    return normalize_username(owner)


def resolve_actor(doc: Dict[str, Any]) -> Optional[str]:
    actor = first_value(
        doc,
        [
            "username",
            "user_username",
            "userUsername",
            "author_username",
            "authorUsername",
            "commenter_username",
            "commenterUsername",
            "liker_username",
            "likerUsername",
            "from_username",
            "fromUsername",
            "actor_username",
            "actorUsername",
            "user",
            "author",
        ],
    )
    return normalize_username(actor)


def resolve_text(doc: Dict[str, Any]) -> str:
    return clean_text(first_value(doc, TEXT_FIELDS, ""))


def resolve_related_post_id(doc: Dict[str, Any]) -> Optional[str]:
    return normalize_id(
        first_value(
            doc,
            [
                "post_id",
                "postId",
                "media_id",
                "mediaId",
                "shortcode",
                "code",
                "target_post_id",
                "targetPostId",
            ],
        ),
        "post",
    )


def write_csv(path: Path, rows: List[Dict[str, Any]], fieldnames: List[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def build_dataset(args: argparse.Namespace) -> Dict[str, Any]:
    posts = filter_last_days(read_table(args.posts), args.days)
    comments = filter_last_days(read_table(args.comments), args.days)
    replies = filter_last_days(read_table(args.replies), args.days)
    likes = filter_last_days(read_table(args.likes), args.days)

    if not posts:
        raise SystemExit(
            "Data posts kosong atau file tidak ditemukan. Gunakan --posts path/to/posts.csv atau --posts path/to/posts.json"
        )

    nodes: Dict[str, Dict[str, Any]] = {}
    edge_counter: Counter = Counter()
    edge_attrs: Dict[Tuple[str, str, str], Dict[str, Any]] = {}

    post_owner_map: Dict[str, str] = {}
    comment_author_map: Dict[str, str] = {}
    user_posts: Dict[str, List[str]] = defaultdict(list)
    hashtag_posts: Dict[str, List[str]] = defaultdict(list)

    for post in posts:
        post_id = resolve_post_id(post)
        if not post_id:
            continue

        text = resolve_text(post)
        owner = resolve_post_owner(post)
        dt = get_doc_date(post)
        shortcode = str(first_value(post, ["shortcode", "code"], ""))
        label = shortcode or post_id.replace("post:", "")

        add_node(
            nodes,
            post_id,
            "post",
            label,
            created_at=dt.isoformat() if dt else "",
            caption=text[:250],
            source="instagram",
        )

        if owner:
            add_node(nodes, owner, "user", owner.replace("user:", ""), source="instagram")
            add_edge(edge_counter, edge_attrs, owner, post_id, "created_post", source="instagram")
            post_owner_map[post_id] = owner
            user_posts[owner].append(post_id)

        for hashtag in extract_hashtags(text):
            add_node(nodes, hashtag, "hashtag", hashtag.replace("hashtag:", "#"), source="instagram")
            add_edge(edge_counter, edge_attrs, post_id, hashtag, "post_has_hashtag", source="instagram")
            add_edge(edge_counter, edge_attrs, hashtag, post_id, "hashtag_used_in_post", source="instagram")
            hashtag_posts[hashtag].append(post_id)

            if owner:
                add_edge(edge_counter, edge_attrs, owner, hashtag, "user_used_hashtag", source="instagram")

        for mentioned_user in extract_mentions(text):
            add_node(nodes, mentioned_user, "user", mentioned_user.replace("user:", ""), source="instagram")
            add_edge(edge_counter, edge_attrs, post_id, mentioned_user, "post_mentions_user", source="instagram")
            if owner:
                add_edge(edge_counter, edge_attrs, owner, mentioned_user, "mentions", source="instagram")

    for comment in comments:
        actor = resolve_actor(comment)
        post_id = resolve_related_post_id(comment)
        comment_id = normalize_id(first_value(comment, ["comment_id", "commentId", "id", "_id"]), "comment")
        text = resolve_text(comment)

        if actor:
            add_node(nodes, actor, "user", actor.replace("user:", ""), source="instagram")
        if comment_id:
            comment_author_map[comment_id] = actor or ""

        if post_id:
            add_edge(edge_counter, edge_attrs, actor, post_id, "commented_on_post", source="instagram", text=text[:250])
            owner = post_owner_map.get(post_id)
            if owner and actor:
                add_edge(edge_counter, edge_attrs, actor, owner, "comment_interaction", source="instagram")

        for hashtag in extract_hashtags(text):
            add_node(nodes, hashtag, "hashtag", hashtag.replace("hashtag:", "#"), source="instagram")
            add_edge(edge_counter, edge_attrs, actor, hashtag, "user_used_hashtag_in_comment", source="instagram")
            if post_id:
                add_edge(edge_counter, edge_attrs, hashtag, post_id, "hashtag_appears_in_comment_on_post", source="instagram")

        for mentioned_user in extract_mentions(text):
            add_node(nodes, mentioned_user, "user", mentioned_user.replace("user:", ""), source="instagram")
            add_edge(edge_counter, edge_attrs, actor, mentioned_user, "mentions", source="instagram")

    for reply in replies:
        actor = resolve_actor(reply)
        post_id = resolve_related_post_id(reply)
        parent_comment_id = normalize_id(
            first_value(reply, ["parent_comment_id", "parentCommentId", "comment_id", "commentId"]),
            "comment",
        )
        replied_to = normalize_username(
            first_value(reply, ["reply_to_username", "replyToUsername", "parent_username", "parentUsername"])
        )
        text = resolve_text(reply)

        if not replied_to and parent_comment_id:
            replied_to = comment_author_map.get(parent_comment_id)

        if actor:
            add_node(nodes, actor, "user", actor.replace("user:", ""), source="instagram")

        if replied_to:
            add_node(nodes, replied_to, "user", replied_to.replace("user:", ""), source="instagram")
            add_edge(edge_counter, edge_attrs, actor, replied_to, "reply_interaction", source="instagram", text=text[:250])

        if post_id:
            add_edge(edge_counter, edge_attrs, actor, post_id, "replied_on_post", source="instagram", text=text[:250])
            owner = post_owner_map.get(post_id)
            if owner and actor:
                add_edge(edge_counter, edge_attrs, actor, owner, "reply_to_post_owner", source="instagram")

    for like in likes:
        actor = resolve_actor(like)
        post_id = resolve_related_post_id(like)

        if actor:
            add_node(nodes, actor, "user", actor.replace("user:", ""), source="instagram")

        if post_id:
            add_edge(edge_counter, edge_attrs, actor, post_id, "liked_post", source="instagram")
            owner = post_owner_map.get(post_id)
            if owner and actor:
                add_edge(edge_counter, edge_attrs, actor, owner, "like_interaction", source="instagram")

    for _, post_ids in user_posts.items():
        sorted_posts = sorted(set(post_ids))
        for index, source_post in enumerate(sorted_posts):
            for target_post in sorted_posts[index + 1 : index + 1 + args.max_post_links_per_group]:
                add_edge(edge_counter, edge_attrs, source_post, target_post, "same_author", source="instagram")
                add_edge(edge_counter, edge_attrs, target_post, source_post, "same_author", source="instagram")

    for hashtag, post_ids in hashtag_posts.items():
        sorted_posts = sorted(set(post_ids))
        for index, source_post in enumerate(sorted_posts):
            for target_post in sorted_posts[index + 1 : index + 1 + args.max_post_links_per_group]:
                add_edge(
                    edge_counter,
                    edge_attrs,
                    source_post,
                    target_post,
                    "shared_hashtag",
                    source="instagram",
                    hashtag=hashtag,
                )
                add_edge(
                    edge_counter,
                    edge_attrs,
                    target_post,
                    source_post,
                    "shared_hashtag",
                    source="instagram",
                    hashtag=hashtag,
                )

    edge_rows = []
    for (source, target, relation), weight in edge_counter.items():
        attrs = edge_attrs.get((source, target, relation), {})
        edge_rows.append(
            {
                "source": source,
                "target": target,
                "relation": relation,
                "weight": weight,
                "source_platform": attrs.get("source", "instagram"),
                "hashtag": attrs.get("hashtag", ""),
                "text": attrs.get("text", ""),
            }
        )

    node_rows = list(nodes.values())
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    write_csv(
        out_dir / "instagram_sna_nodes.csv",
        node_rows,
        ["id", "type", "label", "source", "created_at", "caption"],
    )
    write_csv(
        out_dir / "instagram_sna_edges.csv",
        edge_rows,
        ["source", "target", "relation", "weight", "source_platform", "hashtag", "text"],
    )

    graph_json = {
        "metadata": {
            "source": "instagram",
            "range_days": args.days,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "input_files": {
                "posts": args.posts,
                "comments": args.comments,
                "replies": args.replies,
                "likes": args.likes,
            },
            "raw_counts": {
                "posts": len(posts),
                "comments": len(comments),
                "replies": len(replies),
                "likes": len(likes),
            },
            "graph_counts": {
                "nodes": len(node_rows),
                "edges": len(edge_rows),
            },
        },
        "nodes": node_rows,
        "edges": edge_rows,
    }

    with (out_dir / "instagram_sna_graph.json").open("w", encoding="utf-8") as file:
        json.dump(graph_json, file, ensure_ascii=False, indent=2, default=str)

    return {
        "output_dir": str(out_dir),
        "nodes": len(node_rows),
        "edges": len(edge_rows),
        "node_summary": dict(Counter(node["type"] for node in node_rows)),
        "relation_summary": dict(Counter(edge["relation"] for edge in edge_rows)),
        "metadata": graph_json["metadata"],
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build dataset CSV Social Network Analysis dari file export Instagram 1 tahun terakhir."
    )
    parser.add_argument("--posts", required=True, help="Path file posts Instagram (.csv atau .json)")
    parser.add_argument("--comments", default=None, help="Path file comments Instagram (.csv atau .json)")
    parser.add_argument("--replies", default=None, help="Path file replies Instagram (.csv atau .json)")
    parser.add_argument("--likes", default=None, help="Path file likes Instagram (.csv atau .json)")
    parser.add_argument("--output-dir", default="storage/datasets/instagram_sna")
    parser.add_argument("--days", type=int, default=365)
    parser.add_argument("--max-post-links-per-group", type=int, default=30)
    args = parser.parse_args()

    result = build_dataset(args)
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
