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

ENV_TOKEN_KEYS = ["INSTAGRAM_ACCESS_TOKEN", "IG_ACCESS_TOKEN", "META_ACCESS_TOKEN", "FACEBOOK_ACCESS_TOKEN"]
ENV_IG_ID_KEYS = ["INSTAGRAM_BUSINESS_ACCOUNT_ID", "IG_BUSINESS_ACCOUNT_ID", "INSTAGRAM_ACCOUNT_ID"]


class InstagramApiError(Exception):
    pass


def log(message: str) -> None:
    now = datetime.now().strftime("%H:%M:%S")
    print(f"[{now}] {message}", flush=True)


def format_duration(seconds: float) -> str:
    seconds = int(max(seconds, 0))
    minutes, sec = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}h {minutes}m {sec}s"
    if minutes:
        return f"{minutes}m {sec}s"
    return f"{sec}s"


def load_env_file(path: str = ".env") -> None:
    env_path = Path(path)
    if not env_path.exists():
        log(f"File .env tidak ditemukan di {env_path.resolve()}, lanjut memakai environment variable sistem.")
        return

    log(f"Membaca konfigurasi dari {env_path.resolve()}")
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
    value = str(raw_id or "").strip().lower().replace("@", "")
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
                log(f"Request gagal, retry {attempt + 2}/{retries}: {exc}")
                time.sleep(2 * (attempt + 1))
            else:
                break
    raise InstagramApiError(str(last_error))


def paginate(url: str, params: Dict[str, Any], limit: Optional[int] = None, label: str = "data") -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    next_url: Optional[str] = url
    next_params: Optional[Dict[str, Any]] = params
    page = 1

    while next_url:
        data = request_json(next_url, next_params)
        batch = data.get("data", [])
        batch_count = len(batch) if isinstance(batch, list) else 0
        if isinstance(batch, list):
            rows.extend(batch)

        log(f"Fetch {label}: page {page}, batch {batch_count}, total sementara {len(rows)}")

        if limit and len(rows) >= limit:
            log(f"Fetch {label}: mencapai limit {limit}")
            return rows[:limit]

        next_url = data.get("paging", {}).get("next")
        next_params = None
        page += 1

    return rows


def fetch_instagram_posts(ig_user_id: str, access_token: str, since: datetime, max_posts: Optional[int]) -> List[Dict[str, Any]]:
    fields = ",".join(["id", "caption", "timestamp", "permalink", "media_type", "media_product_type", "like_count", "comments_count"])
    url = f"{GRAPH_BASE_URL}/{ig_user_id}/media"
    params = {"fields": fields, "limit": 100, "access_token": access_token}

    log(f"Mulai mengambil daftar post dari Instagram Graph API sejak {since.date().isoformat()}")
    raw_posts = paginate(url, params, max_posts, label="posts")

    posts = []
    skipped = 0
    newest_date = None
    oldest_date = None
    for post in raw_posts:
        timestamp = iso_to_datetime(post.get("timestamp"))
        if timestamp:
            newest_date = max(newest_date, timestamp) if newest_date else timestamp
            oldest_date = min(oldest_date, timestamp) if oldest_date else timestamp
        if timestamp and timestamp < since:
            skipped += 1
            continue
        posts.append(post)

    log(
        "Selesai mengambil post. "
        f"Total mentah: {len(raw_posts)}, dipakai: {len(posts)}, dilewati karena di luar range: {skipped}, "
        f"tanggal terbaru: {newest_date.date().isoformat() if newest_date else '-'}, "
        f"tanggal terlama: {oldest_date.date().isoformat() if oldest_date else '-'}"
    )
    return posts


def fetch_comments(media_id: str, access_token: str, max_comments_per_post: Optional[int], post_index: int, total_posts: int) -> List[Dict[str, Any]]:
    fields = "id,text,username,timestamp,like_count,replies{id,text,username,timestamp,like_count}"
    url = f"{GRAPH_BASE_URL}/{media_id}/comments"
    params = {"fields": fields, "limit": 100, "access_token": access_token}
    return paginate(url, params, max_comments_per_post, label=f"comments post {post_index}/{total_posts}")


def fetch_replies(comment_id: str, access_token: str) -> List[Dict[str, Any]]:
    fields = "id,text,username,timestamp,like_count"
    url = f"{GRAPH_BASE_URL}/{comment_id}/replies"
    params = {"fields": fields, "limit": 100, "access_token": access_token}
    return paginate(url, params, label=f"replies comment {comment_id}")


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
    total_posts = len(posts)
    start_time = time.time()

    log(f"Mulai membentuk dataset SNA dari {total_posts} post.")

    for index, post in enumerate(posts, start=1):
        post_start = time.time()
        media_id = str(post.get("id", "")).strip()
        if not media_id:
            log(f"[POST {index}/{total_posts}] dilewati karena media_id kosong")
            continue

        post_node = node_id("post", media_id)
        timestamp = post.get("timestamp", "")
        dt = iso_to_datetime(timestamp)
        date_label = dt.date().isoformat() if dt else timestamp or "tanpa tanggal"
        caption = clean_text(post.get("caption", ""))
        permalink = post.get("permalink", "")
        like_count = int(post.get("like_count") or 0)
        comments_count = int(post.get("comments_count") or 0)
        shortcode = permalink.rstrip("/").split("/")[-1] if permalink else media_id
        hashtags = extract_hashtags(caption)
        mentions = extract_mentions(caption)

        log(
            f"[POST {index}/{total_posts}] tanggal={date_label} id={media_id} "
            f"likes={like_count} comments_count={comments_count} hashtags={len(hashtags)} mentions={len(mentions)}"
        )

        counters["posts"] += 1
        author_to_posts[account_node].append(post_node)

        add_edge(edges, account_node, post_node, "user", "post", "created_post", "post", media_id, media_id, shortcode, timestamp, metadata={
            "caption": caption[:500],
            "permalink": permalink,
            "media_type": post.get("media_type", ""),
            "media_product_type": post.get("media_product_type", ""),
            "like_count": like_count,
            "comments_count": comments_count,
        })

        for tag in hashtags:
            hashtag_node = node_id("hashtag", tag)
            hashtag_to_posts[hashtag_node].append(post_node)
            add_edge(edges, account_node, hashtag_node, "user", "hashtag", "user_used_hashtag", "post", media_id, media_id, shortcode, timestamp)
            add_edge(edges, hashtag_node, post_node, "hashtag", "post", "hashtag_to_post", "post", media_id, media_id, shortcode, timestamp)
            add_edge(edges, post_node, hashtag_node, "post", "hashtag", "post_has_hashtag", "post", media_id, media_id, shortcode, timestamp)

        for mention in mentions:
            mentioned_user = node_id("user", mention)
            add_edge(edges, account_node, mentioned_user, "user", "user", "mentioned_user_in_caption", "post", media_id, media_id, shortcode, timestamp)
            add_edge(edges, post_node, mentioned_user, "post", "user", "post_mentions_user", "post", media_id, media_id, shortcode, timestamp)

        if like_count > 0:
            add_edge(edges, account_node, post_node, "user", "post", "received_likes_count", "post", f"{media_id}:likes_count", media_id, shortcode, timestamp, weight=like_count, metadata={
                "note": "Instagram Graph API generally exposes like_count, not the list of users who liked the post."
            })

        comments = fetch_comments(media_id, access_token, max_comments_per_post, index, total_posts)
        counters["comments"] += len(comments)
        log(f"   ├─ komentar berhasil diambil: {len(comments)}")

        valid_comments = 0
        skipped_old_comments = 0
        reply_total_for_post = 0

        for comment in comments:
            comment_id = str(comment.get("id", "")).strip()
            comment_text = clean_text(comment.get("text", ""))
            comment_username = str(comment.get("username", "")).strip().lower()
            comment_timestamp = comment.get("timestamp", timestamp)
            comment_dt = iso_to_datetime(comment_timestamp)

            if comment_dt and comment_dt < since:
                skipped_old_comments += 1
                continue
            if not comment_username:
                continue

            valid_comments += 1
            commenter_node = node_id("user", comment_username)
            add_edge(edges, commenter_node, post_node, "user", "post", "commented_on_post", "comment", comment_id, media_id, shortcode, comment_timestamp, metadata={"text": comment_text[:500], "comment_like_count": int(comment.get("like_count") or 0)})
            add_edge(edges, commenter_node, account_node, "user", "user", "commented_to_post_owner", "comment", comment_id, media_id, shortcode, comment_timestamp, metadata={"text": comment_text[:500]})

            for tag in extract_hashtags(comment_text):
                hashtag_node = node_id("hashtag", tag)
                add_edge(edges, commenter_node, hashtag_node, "user", "hashtag", "user_used_hashtag_in_comment", "comment", comment_id, media_id, shortcode, comment_timestamp)
                add_edge(edges, hashtag_node, post_node, "hashtag", "post", "hashtag_to_post_from_comment", "comment", comment_id, media_id, shortcode, comment_timestamp)

            for mention in extract_mentions(comment_text):
                mentioned_user = node_id("user", mention)
                add_edge(edges, commenter_node, mentioned_user, "user", "user", "mentioned_user_in_comment", "comment", comment_id, media_id, shortcode, comment_timestamp, metadata={"text": comment_text[:500]})

            embedded_replies = comment.get("replies", {}).get("data", []) if isinstance(comment.get("replies"), dict) else []
            replies = embedded_replies
            if include_replies_endpoint and comment_id and not replies:
                try:
                    replies = fetch_replies(comment_id, access_token)
                except Exception as exc:
                    log(f"   │  ├─ gagal mengambil replies untuk comment {comment_id}: {exc}")
                    replies = []

            counters["replies"] += len(replies)
            reply_total_for_post += len(replies)
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
                add_edge(edges, replier_node, commenter_node, "user", "user", "replied_to_user", "reply", reply_id or comment_id, media_id, shortcode, reply_timestamp, metadata={"text": reply_text[:500], "parent_comment_id": comment_id})
                add_edge(edges, replier_node, post_node, "user", "post", "replied_on_post", "reply", reply_id or comment_id, media_id, shortcode, reply_timestamp, metadata={"text": reply_text[:500], "parent_comment_id": comment_id})

                for mention in extract_mentions(reply_text):
                    mentioned_user = node_id("user", mention)
                    add_edge(edges, replier_node, mentioned_user, "user", "user", "mentioned_user_in_reply", "reply", reply_id or comment_id, media_id, shortcode, reply_timestamp, metadata={"text": reply_text[:500]})

        elapsed = time.time() - start_time
        avg = elapsed / index
        eta = avg * (total_posts - index)
        log(
            f"   └─ selesai post {index}/{total_posts}: valid_comments={valid_comments}, "
            f"old_comments_skipped={skipped_old_comments}, replies={reply_total_for_post}, "
            f"edges_sementara={len(edges)}, durasi_post={format_duration(time.time() - post_start)}, ETA={format_duration(eta)}"
        )

    log("Membuat relasi post->post berdasarkan same_author dan shared_hashtag...")
    for _, post_nodes in author_to_posts.items():
        unique_posts = sorted(set(post_nodes))
        for source_index, source_post in enumerate(unique_posts):
            for target_post in unique_posts[source_index + 1 : source_index + 31]:
                add_edge(edges, source_post, target_post, "post", "post", "same_author", "post_similarity", f"{source_post}|{target_post}|same_author")
                add_edge(edges, target_post, source_post, "post", "post", "same_author", "post_similarity", f"{target_post}|{source_post}|same_author")

    for hashtag_node, post_nodes in hashtag_to_posts.items():
        unique_posts = sorted(set(post_nodes))
        for source_index, source_post in enumerate(unique_posts):
            for target_post in unique_posts[source_index + 1 : source_index + 31]:
                add_edge(edges, source_post, target_post, "post", "post", "shared_hashtag", "post_similarity", f"{source_post}|{target_post}|{hashtag_node}", metadata={"hashtag": hashtag_node})
                add_edge(edges, target_post, source_post, "post", "post", "shared_hashtag", "post_similarity", f"{target_post}|{source_post}|{hashtag_node}", metadata={"hashtag": hashtag_node})

    rows = []
    for edge in edges.values():
        edge["metadata"] = json.dumps(edge.get("metadata", {}), ensure_ascii=False)
        rows.append(edge)

    log(f"Dataset selesai dibentuk: rows={len(rows)}, total durasi={format_duration(time.time() - start_time)}")
    return rows, dict(counters)


def write_single_csv(output_path: Path, rows: List[Dict[str, Any]]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["source", "source_type", "target", "target_type", "relation", "weight", "platform", "context_type", "context_id", "post_id", "post_shortcode", "created_at", "metadata"]
    log(f"Menulis CSV ke {output_path.resolve()}")
    with output_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    log(f"CSV berhasil dibuat: {output_path.resolve()} ({len(rows)} rows)")


def main() -> None:
    parser = argparse.ArgumentParser(description="Export dataset Social Network Analysis Instagram dari Instagram Graph API ke satu file CSV.")
    parser.add_argument("--env", default=".env", help="Path file .env")
    parser.add_argument("--output", default="storage/datasets/instagram_sna/instagram_sna_dataset.csv")
    parser.add_argument("--days", type=int, default=365, help="Rentang data yang diambil dalam hari")
    parser.add_argument("--max-posts", type=int, default=None, help="Batas jumlah post. Kosongkan untuk semua post dalam rentang hari.")
    parser.add_argument("--max-comments-per-post", type=int, default=None, help="Batas komentar per post. Kosongkan untuk semua komentar yang tersedia.")
    parser.add_argument("--include-replies-endpoint", action="store_true", help="Coba request endpoint replies jika replies tidak ikut muncul di response comments.")
    args = parser.parse_args()

    total_start = time.time()
    load_env_file(args.env)

    access_token = env_first(ENV_TOKEN_KEYS)
    ig_user_id = env_first(ENV_IG_ID_KEYS)
    ig_username = os.getenv("INSTAGRAM_USERNAME", "instagram_account")

    if not access_token:
        raise SystemExit("INSTAGRAM_ACCESS_TOKEN tidak ditemukan di .env")
    if not ig_user_id:
        raise SystemExit("INSTAGRAM_BUSINESS_ACCOUNT_ID tidak ditemukan di .env")

    since = datetime.now(timezone.utc) - timedelta(days=args.days)
    log(f"Mulai export Instagram SNA untuk @{ig_username}")
    log(f"Range data: {since.date().isoformat()} sampai {datetime.now(timezone.utc).date().isoformat()}")
    log(f"Output: {args.output}")

    posts = fetch_instagram_posts(ig_user_id, access_token, since, args.max_posts)
    if not posts:
        log("Tidak ada post yang ditemukan pada rentang tanggal tersebut.")

    rows, counters = build_sna_edges(posts, access_token, ig_username, since, args.max_comments_per_post, args.include_replies_endpoint)
    output_path = Path(args.output)
    write_single_csv(output_path, rows)

    relation_summary = Counter(row["relation"] for row in rows)
    type_summary = Counter(f"{row['source_type']}->{row['target_type']}" for row in rows)

    log("Export selesai.")
    print(json.dumps({
        "output": str(output_path),
        "rows": len(rows),
        "raw_counts": counters,
        "relation_summary": dict(relation_summary),
        "type_summary": dict(type_summary),
        "duration": format_duration(time.time() - total_start),
        "note": "Instagram Graph API umumnya hanya menyediakan like_count, bukan daftar user yang melakukan like. Karena itu like disimpan sebagai received_likes_count pada relasi user->post dengan weight sesuai like_count."
    }, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
