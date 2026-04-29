import argparse
import csv
import json
import os
import re
import time
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

GRAPH_API_VERSION = "v19.0"
GRAPH_BASE_URL = f"https://graph.facebook.com/{GRAPH_API_VERSION}"

HASHTAG_REGEX = re.compile(r"#([A-Za-z0-9_]+)")
MENTION_REGEX = re.compile(r"@([A-Za-z0-9_.]+)")
URL_REGEX = re.compile(r"https?://\S+")

ENV_TOKEN_KEYS = [
    "INSTAGRAM_ACCESS_TOKEN",
    "IG_ACCESS_TOKEN",
    "META_ACCESS_TOKEN",
    "FACEBOOK_ACCESS_TOKEN",
]

ENV_IG_ID_KEYS = [
    "INSTAGRAM_BUSINESS_ACCOUNT_ID",
    "IG_BUSINESS_ACCOUNT_ID",
    "INSTAGRAM_ACCOUNT_ID",
]


class InstagramApiError(Exception):
    pass


def load_env_file(path: str = ".env") -> None:
    env_path = Path(path)
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()

        if len(value) >= 2 and value[0] == value[-1] and value[0] in ["'", '"']:
            value = value[1:-1]

        value = value.replace("\\n", "\n")
        os.environ.setdefault(key, value)


def env_first(keys: Iterable[str], default: Optional[str] = None) -> Optional[str]:
    for key in keys:
        value = os.getenv(key)
        if value:
            return value
    return default


def iso_to_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        value = value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def clean_text(value: Optional[str]) -> str:
    if not value:
        return ""
    text = URL_REGEX.sub("", value)
    return " ".join(text.split())


def extract_hashtags(text: str) -> List[str]:
    return sorted({tag.lower() for tag in HASHTAG_REGEX.findall(text or "")})


def extract_mentions(text: str) -> List[str]:
    return sorted({mention.lower() for mention in MENTION_REGEX.findall(text or "")})


def node_id(node_type: str, raw_id: Any) -> str:
    value = str(raw_id or "").strip().lower()
    value = value.replace("@", "")
    return f"{node_type}:{value}"


def request_json(url: str, params: Optional[Dict[str, Any]] = None, retries: int = 3) -> Dict[str, Any]:
    final_url = url
    if params:
        query = urllib.parse.urlencode(params)
        final_url = f"{url}?{query}"

    last_error = None
    for attempt in range(retries):
        try:
            request = urllib.request.Request(final_url, headers={"User-Agent": "SNA-Instagram-Exporter/1.0"})
            with urllib.request.urlopen(request, timeout=60) as response:
                payload = response.read().decode("utf-8")
                data = json.loads(payload)

            if "error" in data:
                raise InstagramApiError(json.dumps(data["error"], ensure_ascii=False))
            return data
        except Exception as exc:
            last_error = exc
            if attempt < retries - 1:
                time.sleep(2 * (attempt + 1))
            else:
                break

    raise InstagramApiError(str(last_error))


def paginate(url: str, params: Dict[str, Any], limit: Optional[int] = None) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    next_url: Optional[str] = url
    next_params: Optional[Dict[str, Any]] = params

    while next_url:
        data = request_json(next_url, next_params)
        batch = data.get("data", [])
        if isinstance(batch, list):
            rows.extend(batch)

        if limit and len(rows) >= limit:
            return rows[:limit]

        next_url = data.get("paging", {}).get("next")
        next_params = None

    return rows


def fetch_instagram_posts(ig_user_id: str, access_token: str, since: datetime, max_posts: Optional[int]) -> List[Dict[str, Any]]:
    fields = ",".join(
        [
            "id",
            "caption",
            "timestamp",
            "permalink",
            "media_type",
            "media_product_type",
            "like_count",
            "comments_count",
        ]
    )
    url = f"{GRAPH_BASE_URL}/{ig_user_id}/media"
    params = {
        "fields": fields,
        "limit": 100,
        "access_token": access_token,
    }

    posts = []
    for post in paginate(url, params, max_posts):
        timestamp = iso_to_datetime(post.get("timestamp"))
        if timestamp and timestamp < since:
            continue
        posts.append(post)

    return posts


def fetch_comments(media_id: str, access_token: str, max_comments_per_post: Optional[int]) -> List[Dict[str, Any]]:
    fields = "id,text,username,timestamp,like_count,replies{id,text,username,timestamp,like_count}"
    url = f"{GRAPH_BASE_URL}/{media_id}/comments"
    params = {
        "fields": fields,
        "limit": 100,
        "access_token": access_token,
    }
    return paginate(url, params, max_comments_per_post)


def fetch_replies(comment_id: str, access_token: str) -> List[Dict[str, Any]]:
    fields = "id,text,username,timestamp,like_count"
    url = f"{GRAPH_BASE_URL}/{comment_id}/replies"
    params = {
        "fields": fields,
        "limit": 100,
        "access_token": access_token,
    }
    return paginate(url, params)


def make_edge_key(source: str, target: str, relation: str, context_id: str = "") -> Tuple[str, str, str, str]:
    return source, target, relation, context_id


def add_edge(
    edges: Dict[Tuple[str, str, str, str], Dict[str, Any]],
    source: str,
    target: str,
    source_type: str,
    target_type: str,
    relation: str,
    context_type: str = "",
    context_id: str = "",
    post_id: str = "",
    post_shortcode: str = "",
    created_at: str = "",
    weight: int = 1,
    metadata: Optional[Dict[str, Any]] = None,
) -> None:
    if not source or not target or source == target:
        return

    key = make_edge_key(source, target, relation, context_id)
    if key not in edges:
        edges[key] = {
            "source": source,
            "source_type": source_type,
            "target": target,
            "target_type": target_type,
            "relation": relation,
            "weight": 0,
            "platform": "instagram",
            "context_type": context_type,
            "context_id": context_id,
            "post_id": post_id,
            "post_shortcode": post_shortcode,
            "created_at": created_at,
            "metadata": {},
        }

    edges[key]["weight"] += weight
    if metadata:
        edges[key]["metadata"].update(metadata)


def build_sna_edges(
    posts: List[Dict[str, Any]],
    access_token: str,
    ig_username: str,
    since: datetime,
    max_comments_per_post: Optional[int],
    include_replies_endpoint: bool,
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    edges: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}
    counters = Counter()
    account_node = node_id("user", ig_username)
    hashtag_to_posts: Dict[str, List[str]] = defaultdict(list)
    author_to_posts: Dict[str, List[str]] = defaultdict(list)
    commenter_by_comment_id: Dict[str, str] = {}

    for index, post in enumerate(posts, start=1):
        media_id = str(post.get("id", "")).strip()
        if not media_id:
            continue

        post_node = node_id("post", media_id)
        timestamp = post.get("timestamp", "")
        caption = clean_text(post.get("caption", ""))
        permalink = post.get("permalink", "")
        like_count = int(post.get("like_count") or 0)
        comments_count = int(post.get("comments_count") or 0)
        shortcode = permalink.rstrip("/").split("/")[-1] if permalink else media_id

        counters["posts"] += 1
        author_to_posts[account_node].append(post_node)

        add_edge(
            edges,
            account_node,
            post_node,
            "user",
            "post",
            "created_post",
            context_type="post",
            context_id=media_id,
            post_id=media_id,
            post_shortcode=shortcode,
            created_at=timestamp,
            metadata={
                "caption": caption[:500],
                "permalink": permalink,
                "media_type": post.get("media_type", ""),
                "media_product_type": post.get("media_product_type", ""),
                "like_count": like_count,
                "comments_count": comments_count,
            },
        )

        for tag in extract_hashtags(caption):
            hashtag_node = node_id("hashtag", tag)
            hashtag_to_posts[hashtag_node].append(post_node)

            add_edge(
                edges,
                account_node,
                hashtag_node,
                "user",
                "hashtag",
                "user_used_hashtag",
                context_type="post",
                context_id=media_id,
                post_id=media_id,
                post_shortcode=shortcode,
                created_at=timestamp,
            )
            add_edge(
                edges,
                hashtag_node,
                post_node,
                "hashtag",
                "post",
                "hashtag_to_post",
                context_type="post",
                context_id=media_id,
                post_id=media_id,
                post_shortcode=shortcode,
                created_at=timestamp,
            )
            add_edge(
                edges,
                post_node,
                hashtag_node,
                "post",
                "hashtag",
                "post_has_hashtag",
                context_type="post",
                context_id=media_id,
                post_id=media_id,
                post_shortcode=shortcode,
                created_at=timestamp,
            )

        for mention in extract_mentions(caption):
            mentioned_user = node_id("user", mention)
            add_edge(
                edges,
                account_node,
                mentioned_user,
                "user",
                "user",
                "mentioned_user_in_caption",
                context_type="post",
                context_id=media_id,
                post_id=media_id,
                post_shortcode=shortcode,
                created_at=timestamp,
            )
            add_edge(
                edges,
                post_node,
                mentioned_user,
                "post",
                "user",
                "post_mentions_user",
                context_type="post",
                context_id=media_id,
                post_id=media_id,
                post_shortcode=shortcode,
                created_at=timestamp,
            )

        if like_count > 0:
            add_edge(
                edges,
                account_node,
                post_node,
                "user",
                "post",
                "received_likes_count",
                context_type="post",
                context_id=f"{media_id}:likes_count",
                post_id=media_id,
                post_shortcode=shortcode,
                created_at=timestamp,
                weight=like_count,
                metadata={
                    "note": "Instagram Graph API generally exposes like_count, not the list of users who liked the post."
                },
            )

        comments = fetch_comments(media_id, access_token, max_comments_per_post)
        counters["comments"] += len(comments)

        for comment in comments:
            comment_id = str(comment.get("id", "")).strip()
            comment_text = clean_text(comment.get("text", ""))
            comment_username = str(comment.get("username", "")).strip().lower()
            comment_timestamp = comment.get("timestamp", timestamp)
            comment_dt = iso_to_datetime(comment_timestamp)

            if comment_dt and comment_dt < since:
                continue
            if not comment_username:
                continue

            commenter_node = node_id("user", comment_username)
            commenter_by_comment_id[comment_id] = commenter_node

            add_edge(
                edges,
                commenter_node,
                post_node,
                "user",
                "post",
                "commented_on_post",
                context_type="comment",
                context_id=comment_id,
                post_id=media_id,
                post_shortcode=shortcode,
                created_at=comment_timestamp,
                metadata={"text": comment_text[:500], "comment_like_count": int(comment.get("like_count") or 0)},
            )
            add_edge(
                edges,
                commenter_node,
                account_node,
                "user",
                "user",
                "commented_to_post_owner",
                context_type="comment",
                context_id=comment_id,
                post_id=media_id,
                post_shortcode=shortcode,
                created_at=comment_timestamp,
                metadata={"text": comment_text[:500]},
            )

            for tag in extract_hashtags(comment_text):
                hashtag_node = node_id("hashtag", tag)
                add_edge(
                    edges,
                    commenter_node,
                    hashtag_node,
                    "user",
                    "hashtag",
                    "user_used_hashtag_in_comment",
                    context_type="comment",
                    context_id=comment_id,
                    post_id=media_id,
                    post_shortcode=shortcode,
                    created_at=comment_timestamp,
                )
                add_edge(
                    edges,
                    hashtag_node,
                    post_node,
                    "hashtag",
                    "post",
                    "hashtag_to_post_from_comment",
                    context_type="comment",
                    context_id=comment_id,
                    post_id=media_id,
                    post_shortcode=shortcode,
                    created_at=comment_timestamp,
                )

            for mention in extract_mentions(comment_text):
                mentioned_user = node_id("user", mention)
                add_edge(
                    edges,
                    commenter_node,
                    mentioned_user,
                    "user",
                    "user",
                    "mentioned_user_in_comment",
                    context_type="comment",
                    context_id=comment_id,
                    post_id=media_id,
                    post_shortcode=shortcode,
                    created_at=comment_timestamp,
                    metadata={"text": comment_text[:500]},
                )

            embedded_replies = comment.get("replies", {}).get("data", []) if isinstance(comment.get("replies"), dict) else []
            replies = embedded_replies
            if include_replies_endpoint and comment_id and not replies:
                try:
                    replies = fetch_replies(comment_id, access_token)
                except Exception:
                    replies = []

            counters["replies"] += len(replies)
            for reply in replies:
                reply_id = str(reply.get("id", "")).strip()
                reply_username = str(reply.get("username", "")).strip().lower()
                reply_text = clean_text(reply.get("text", ""))
                reply_timestamp = reply.get("timestamp", comment_timestamp)
                reply_dt = iso_to_datetime(reply_timestamp)

                if reply_dt and reply_dt < since:
                    continue
                if not reply_username:
                    continue

                replier_node = node_id("user", reply_username)
                add_edge(
                    edges,
                    replier_node,
                    commenter_node,
                    "user",
                    "user",
                    "replied_to_user",
                    context_type="reply",
                    context_id=reply_id or comment_id,
                    post_id=media_id,
                    post_shortcode=shortcode,
                    created_at=reply_timestamp,
                    metadata={"text": reply_text[:500], "parent_comment_id": comment_id},
                )
                add_edge(
                    edges,
                    replier_node,
                    post_node,
                    "user",
                    "post",
                    "replied_on_post",
                    context_type="reply",
                    context_id=reply_id or comment_id,
                    post_id=media_id,
                    post_shortcode=shortcode,
                    created_at=reply_timestamp,
                    metadata={"text": reply_text[:500], "parent_comment_id": comment_id},
                )

                for mention in extract_mentions(reply_text):
                    mentioned_user = node_id("user", mention)
                    add_edge(
                        edges,
                        replier_node,
                        mentioned_user,
                        "user",
                        "user",
                        "mentioned_user_in_reply",
                        context_type="reply",
                        context_id=reply_id or comment_id,
                        post_id=media_id,
                        post_shortcode=shortcode,
                        created_at=reply_timestamp,
                        metadata={"text": reply_text[:500]},
                    )

    for _, post_nodes in author_to_posts.items():
        unique_posts = sorted(set(post_nodes))
        for source_index, source_post in enumerate(unique_posts):
            for target_post in unique_posts[source_index + 1 : source_index + 31]:
                add_edge(
                    edges,
                    source_post,
                    target_post,
                    "post",
                    "post",
                    "same_author",
                    context_type="post_similarity",
                    context_id=f"{source_post}|{target_post}|same_author",
                )
                add_edge(
                    edges,
                    target_post,
                    source_post,
                    "post",
                    "post",
                    "same_author",
                    context_type="post_similarity",
                    context_id=f"{target_post}|{source_post}|same_author",
                )

    for hashtag_node, post_nodes in hashtag_to_posts.items():
        unique_posts = sorted(set(post_nodes))
        for source_index, source_post in enumerate(unique_posts):
            for target_post in unique_posts[source_index + 1 : source_index + 31]:
                add_edge(
                    edges,
                    source_post,
                    target_post,
                    "post",
                    "post",
                    "shared_hashtag",
                    context_type="post_similarity",
                    context_id=f"{source_post}|{target_post}|{hashtag_node}",
                    metadata={"hashtag": hashtag_node},
                )
                add_edge(
                    edges,
                    target_post,
                    source_post,
                    "post",
                    "post",
                    "shared_hashtag",
                    context_type="post_similarity",
                    context_id=f"{target_post}|{source_post}|{hashtag_node}",
                    metadata={"hashtag": hashtag_node},
                )

    rows = []
    for edge in edges.values():
        edge["metadata"] = json.dumps(edge.get("metadata", {}), ensure_ascii=False)
        rows.append(edge)

    return rows, dict(counters)


def write_single_csv(output_path: Path, rows: List[Dict[str, Any]]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "source",
        "source_type",
        "target",
        "target_type",
        "relation",
        "weight",
        "platform",
        "context_type",
        "context_id",
        "post_id",
        "post_shortcode",
        "created_at",
        "metadata",
    ]

    with output_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export dataset Social Network Analysis Instagram dari Instagram Graph API ke satu file CSV."
    )
    parser.add_argument("--env", default=".env", help="Path file .env")
    parser.add_argument("--output", default="storage/datasets/instagram_sna/instagram_sna_dataset.csv")
    parser.add_argument("--days", type=int, default=365, help="Rentang data yang diambil dalam hari")
    parser.add_argument("--max-posts", type=int, default=None, help="Batas jumlah post. Kosongkan untuk semua post dalam rentang hari.")
    parser.add_argument("--max-comments-per-post", type=int, default=None, help="Batas komentar per post. Kosongkan untuk semua komentar yang tersedia.")
    parser.add_argument("--include-replies-endpoint", action="store_true", help="Coba request endpoint replies jika replies tidak ikut muncul di response comments.")
    args = parser.parse_args()

    load_env_file(args.env)

    access_token = env_first(ENV_TOKEN_KEYS)
    ig_user_id = env_first(ENV_IG_ID_KEYS)
    ig_username = os.getenv("INSTAGRAM_USERNAME", "instagram_account")

    if not access_token:
        raise SystemExit("INSTAGRAM_ACCESS_TOKEN tidak ditemukan di .env")
    if not ig_user_id:
        raise SystemExit("INSTAGRAM_BUSINESS_ACCOUNT_ID tidak ditemukan di .env")

    since = datetime.now(timezone.utc) - timedelta(days=args.days)

    print(f"Mengambil data Instagram @{ig_username} selama {args.days} hari terakhir...")
    posts = fetch_instagram_posts(ig_user_id, access_token, since, args.max_posts)
    print(f"Post ditemukan: {len(posts)}")

    rows, counters = build_sna_edges(
        posts=posts,
        access_token=access_token,
        ig_username=ig_username,
        since=since,
        max_comments_per_post=args.max_comments_per_post,
        include_replies_endpoint=args.include_replies_endpoint,
    )

    output_path = Path(args.output)
    write_single_csv(output_path, rows)

    relation_summary = Counter(row["relation"] for row in rows)
    type_summary = Counter(f"{row['source_type']}->{row['target_type']}" for row in rows)

    print("Export selesai.")
    print(json.dumps({
        "output": str(output_path),
        "rows": len(rows),
        "raw_counts": counters,
        "relation_summary": dict(relation_summary),
        "type_summary": dict(type_summary),
        "note": "Instagram Graph API umumnya hanya menyediakan like_count, bukan daftar user yang melakukan like. Karena itu like disimpan sebagai received_likes_count pada relasi user->post dengan weight sesuai like_count."
    }, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
