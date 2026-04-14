from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import re
import time
from html import unescape
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import ParseResult, quote_plus, urlparse, urlunparse

import requests


REDDIT_HOSTS = {"reddit.com", "www.reddit.com", "old.reddit.com", "np.reddit.com", "m.reddit.com", "redd.it", "www.redd.it"}
COMMENT_PATH_RE = re.compile(r"/comments/([^/]+)/[^/]+/([^/?#]+)/?")
POST_PATH_RE = re.compile(r"/comments/([^/]+)/?")
DEFAULT_JSON_LIMIT = 500
USER_AGENT = "reddit-link-status-checker/1.1"


@dataclass
class RedditLinkStatus:
    input_url: str
    normalized_url: str
    link_kind: str
    status: str
    subreddit: str
    author: str
    created_utc: str
    title: str
    score: int
    num_comments: int
    id: str
    permalink: str
    body_excerpt: str
    error: str


@dataclass
class PostMetadata:
    """Rich post (submission) fields from Reddit t3 JSON (public endpoint)."""

    input_url: str
    post_id: str
    title: str
    author: str
    subreddit: str
    created_utc: str
    score: int
    ups: int
    downs: int
    upvote_ratio: float
    num_comments: int
    is_self: bool
    domain: str
    url: str
    selftext: str
    selftext_html: str
    link_flair_text: str
    permalink: str
    thumbnail: str
    over_18: bool
    spoiler: bool
    locked: bool
    stickied: bool
    edited: str
    comments_returned: int
    comments_omitted_more: int
    note: str


@dataclass
class CommentRecord:
    """One flattened comment row (thread walk, depth 0 = top-level)."""

    post_id: str
    depth: int
    parent_fullname: str
    comment_id: str
    author: str
    body: str
    score: int
    created_utc: str
    permalink: str
    status: str
    stickied: str
    edited: str
    distinguished: str


@dataclass
class ThreadExtraction:
    """Full thread scrape for a post (or comment URL pointing at a thread)."""

    status: RedditLinkStatus
    post: PostMetadata | None
    comments: list[CommentRecord] = field(default_factory=list)

@dataclass
class SubredditRecord:
    """Best-effort subreddit metadata without credentials."""

    input_name: str
    normalized_name: str
    subreddit_url: str
    title: str
    public_description: str
    subscribers: int
    active_user_count: int
    created_utc: str
    over18: bool
    rules_markdown: str
    moderators: str
    widgets: str
    error: str

@dataclass
class SearchResultRecord:
    query: str
    subreddit: str
    post_id: str
    title: str
    author: str
    created_utc: str
    score: int
    num_comments: int
    upvote_ratio: float
    permalink: str
    post_url: str
    selftext_excerpt: str
    lead_score: int
    lead_reasons: str
    duplicate_group: str

@dataclass
class PostHealthRecord:
    input_url: str
    status: str
    subreddit: str
    title: str
    post_created_utc: str
    post_author: str
    locked: bool
    stickied: bool
    over_18: bool
    num_comments_reported: int
    comments_in_payload: int
    last_comment_utc: str
    last_comment_live: bool
    mod_comment_count: int
    bot_comment_count: int
    removed_deleted_comment_count: int
    post_ups: int
    post_downs: int
    post_upvote_ratio: float
    post_body_excerpt: str
    top_comments_excerpt: str
    subreddit_description_excerpt: str
    subreddit_rules_excerpt: str
    can_post_new_comment: str
    rationale: str


@dataclass
class RedditUserRecord:
    input_username: str
    username: str
    profile_url: str
    status: str
    account_created_utc: str
    total_karma: int
    link_karma: int
    comment_karma: int
    is_mod: bool
    is_gold: bool
    verified_email: bool
    has_subscribed: bool
    icon_img: str
    recent_posts_count: int
    recent_comments_count: int
    latest_activity_utc: str
    recent_post_titles: str
    recent_comment_excerpts: str
    error: str


def _normalize_reddit_url(url: str) -> str:
    parsed = urlparse(url.strip())
    if not parsed.scheme:
        parsed = urlparse(f"https://{url.strip()}")

    # Reddit share links like /s/<token> and redd.it short links require redirect resolution.
    is_short_share = bool(re.search(r"/s/[A-Za-z0-9]+/?$", parsed.path or ""))
    is_redd_it = parsed.netloc in {"redd.it", "www.redd.it"}
    if (parsed.netloc in REDDIT_HOSTS) and (is_short_share or is_redd_it):
        try:
            resolved = requests.get(
                urlunparse(parsed),
                timeout=20,
                allow_redirects=True,
                headers={"User-Agent": USER_AGENT},
            )
            parsed = urlparse(resolved.url)
        except Exception:
            # Keep original URL shape if resolution fails; caller will still get a clear status.
            pass

    if parsed.netloc not in REDDIT_HOSTS:
        raise ValueError("URL is not a reddit.com link")
    clean = ParseResult(
        scheme="https",
        netloc="www.reddit.com",
        path=parsed.path.rstrip("/"),
        params="",
        query="",
        fragment="",
    )
    return urlunparse(clean)


def _detect_kind(path: str, forced_kind: str) -> str:
    if forced_kind in {"post", "comment"}:
        return forced_kind
    if COMMENT_PATH_RE.search(path):
        return "comment"
    if POST_PATH_RE.search(path):
        return "post"
    return "unknown"


def _to_iso(utc_seconds: float | int | None) -> str:
    if utc_seconds is None:
        return ""
    return dt.datetime.fromtimestamp(float(utc_seconds), tz=dt.timezone.utc).isoformat()


def _iso_from_html_datetime(attr: str) -> str:
    """Normalize Reddit HTML <time datetime=\"...\"> to UTC ISO (matches _to_iso style)."""
    if not attr:
        return ""
    s = attr.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        d = dt.datetime.fromisoformat(s)
    except ValueError:
        return ""
    if d.tzinfo is None:
        d = d.replace(tzinfo=dt.timezone.utc)
    return d.astimezone(dt.timezone.utc).isoformat()


def _get_json_endpoint(normalized_url: str, limit: int = DEFAULT_JSON_LIMIT) -> str:
    base = normalized_url.rstrip("/")
    return f"{base}.json?raw_json=1&limit={limit}"


def _reddit_thread_url_with_host(normalized_www_url: str, host: str) -> str:
    """``normalized_www_url`` is https://www.reddit.com/...; switch to old.reddit.com when host is old."""
    u = normalized_www_url.strip()
    if host == "old":
        return u.replace("https://www.reddit.com", "https://old.reddit.com", 1)
    return u

def _get_subreddit_name(raw: str) -> str:
    s = (raw or "").strip()
    if not s:
        return ""
    s = s.replace("https://www.reddit.com/r/", "").replace("http://www.reddit.com/r/", "")
    s = s.replace("https://reddit.com/r/", "").replace("http://reddit.com/r/", "")
    s = s.replace("r/", "")
    s = s.strip().strip("/")
    if "?" in s:
        s = s.split("?", 1)[0]
    if "#" in s:
        s = s.split("#", 1)[0]
    # keep only first path segment
    if "/" in s:
        s = s.split("/", 1)[0]
    return s

def _reddit_get(url: str, timeout: int = 20, max_retries: int = 0) -> tuple[requests.Response | None, str]:
    """GET from Reddit with optional backoff (429 / 503 / transient errors)."""
    attempts_total = max(1, max_retries + 1)
    for attempt in range(attempts_total):
        try:
            resp = requests.get(url, timeout=timeout, headers={"User-Agent": USER_AGENT})
            if resp.status_code == 404:
                return None, "HTTP 404"
            if resp.status_code == 403:
                return None, "HTTP 403"
            if resp.status_code == 429:
                if attempt < attempts_total - 1:
                    ra = resp.headers.get("Retry-After")
                    try:
                        sleep_s = min(90.0, float(ra)) if ra else min(60.0, 2.0 ** (attempt + 1))
                    except ValueError:
                        sleep_s = min(60.0, 2.0 ** (attempt + 1))
                    time.sleep(sleep_s)
                    continue
                return None, "HTTP 429 (rate limited)"
            if resp.status_code == 503:
                if attempt < attempts_total - 1:
                    time.sleep(1.0 * (attempt + 1))
                    continue
                return None, "HTTP 503"
            resp.raise_for_status()
            return resp, ""
        except Exception as exc:  # noqa: BLE001
            if attempt < attempts_total - 1:
                time.sleep(0.75 * (attempt + 1))
                continue
            return None, str(exc)
    return None, "request failed"


def _fetch_json(url: str, timeout: int = 20, max_retries: int = 0) -> tuple[Any | None, str]:
    """GET JSON from Reddit. Set max_retries>0 to back off on rate limits (batch fetches)."""
    resp, err = _reddit_get(url, timeout=timeout, max_retries=max_retries)
    if err or resp is None:
        return None, err
    try:
        return resp.json(), ""
    except Exception as exc:  # noqa: BLE001
        return None, str(exc)


_OLD_RE_KARMA_POST = re.compile(
    r'<span class="karma">([\d,]+)</span>\s*(?:&#32;|\s)+post karma',
    re.I,
)
_OLD_RE_KARMA_COMMENT = re.compile(
    r'<span class="karma comment-karma">([\d,]+)</span>\s*(?:&#32;|\s)+comment karma',
    re.I,
)
_OLD_RE_ACCOUNT_START = re.compile(r'redditor for\s*(?:&#32;|\s)*<time[^>]*datetime="([^"]+)"', re.S | re.I)
_THING_DIV = '<div class=" thing'


def _karma_int(s: str) -> int:
    t = (s or "").replace(",", "").strip()
    return int(t) if t else 0


def _iter_old_reddit_things(html: str) -> Iterable[tuple[str, str]]:
    pos = 0
    while True:
        i = html.find(_THING_DIV, pos)
        if i < 0:
            break
        j = html.find(_THING_DIV, i + len(_THING_DIV))
        chunk = html[i:j] if j >= 0 else html[i:]
        if 'data-type="link"' in chunk:
            yield "link", chunk
        elif 'data-type="comment"' in chunk:
            yield "comment", chunk
        pos = i + len(_THING_DIV)


def _old_reddit_link_title(chunk: str) -> str:
    m = re.search(r'<a[^>]*class="title(?:\s[^"]*)?"[^>]*>([^<]*)</a>', chunk)
    if m:
        return _compact_text(unescape(m.group(1)))
    m = re.search(r'data-crosspost-root-title="([^"]+)"', chunk)
    return _compact_text(unescape(m.group(1))) if m else ""


def _old_reddit_comment_excerpt(chunk: str, max_len: int = 120) -> str:
    m = re.search(r'<div class="md">(.*?)</div>\s*</div>\s*</form>', chunk, re.S)
    if not m:
        return ""
    raw = re.sub(r"<[^>]+>", " ", m.group(1))
    return _extract_excerpt(unescape(_compact_text(raw)), max_len=max_len)


def _latest_time_in_chunk(chunk: str, current: str) -> str:
    best = current
    for m in re.finditer(r'<time[^>]*datetime="([^"]+)"', chunk):
        ts = m.group(1)
        if not best or ts > best:
            best = ts
    return best


def _parse_old_reddit_user_overview(
    html: str,
    sample_limit: int,
) -> tuple[
    str,
    int,
    int,
    list[str],
    list[str],
    str,
    str,
    str,
]:
    """Parse old.reddit.com user overview HTML. Returns (created_iso, link_k, comment_k, titles, excerpts, latest_iso, status_hint, parse_note)."""
    parse_notes: list[str] = []
    low = html.lower()
    if "this account has been suspended" in low or "permanently banned" in low:
        return "", 0, 0, [], [], "", "restricted", ""
    if "this profile is not available" in low or "nobody on reddit goes by" in low:
        return "", 0, 0, [], [], "", "not_found", ""

    mp = _OLD_RE_KARMA_POST.search(html)
    mc = _OLD_RE_KARMA_COMMENT.search(html)
    ma = _OLD_RE_ACCOUNT_START.search(html)
    link_k = _karma_int(mp.group(1)) if mp else 0
    comment_k = _karma_int(mc.group(1)) if mc else 0
    created = _iso_from_html_datetime(ma.group(1)) if ma else ""

    if not mp and not mc and "titlebox" in low:
        parse_notes.append("karma not found in HTML (layout may have changed)")

    post_titles: list[str] = []
    comment_excerpts: list[str] = []
    latest = ""
    scanned = 0
    for kind, chunk in _iter_old_reddit_things(html):
        scanned += 1
        if scanned > max(50, sample_limit * 8):
            break
        latest = _latest_time_in_chunk(chunk, latest)
        if kind == "link" and len(post_titles) < sample_limit:
            title = _old_reddit_link_title(chunk)
            if title:
                post_titles.append(title)
        elif kind == "comment" and len(comment_excerpts) < sample_limit:
            ex = _old_reddit_comment_excerpt(chunk)
            if ex:
                comment_excerpts.append(ex)
        if len(post_titles) >= sample_limit and len(comment_excerpts) >= sample_limit:
            break

    if latest:
        latest = _iso_from_html_datetime(latest)

    status_hint = ""
    note = "; ".join(parse_notes)
    return created, link_k, comment_k, post_titles, comment_excerpts, latest, status_hint, note

def _compact_text(s: str) -> str:
    return " ".join((s or "").split()).strip()

def _tokenize(s: str) -> list[str]:
    return re.findall(r"[a-z0-9]+", (s or "").lower())

def _normalized_title_key(title: str) -> str:
    tokens = [t for t in _tokenize(title) if len(t) > 2 and t not in {"the", "and", "for", "with", "this", "that"}]
    if not tokens:
        return ""
    return " ".join(tokens[:14])

def _lead_score(title: str, body: str, num_comments: int, score: int) -> tuple[int, list[str]]:
    text = f"{title} {body}".lower()
    reasons: list[str] = []
    pts = 0
    intent_terms = ["recommend", "looking for", "need", "struggling", "problem", "help", "alternative", "best ", "tool", "software"]
    pain_terms = ["stuck", "frustrated", "issue", "pain", "hard", "difficult", "waste", "expensive"]
    question_terms = ["how do", "what is", "anyone", "where can", "can someone", "?"]
    if any(t in text for t in intent_terms):
        pts += 30
        reasons.append("intent")
    if any(t in text for t in pain_terms):
        pts += 20
        reasons.append("pain")
    if any(t in text for t in question_terms):
        pts += 20
        reasons.append("question")
    if num_comments >= 10:
        pts += 10
        reasons.append("discussion")
    if score >= 10:
        pts += 10
        reasons.append("engagement")
    if "b2b" in text or "saas" in text:
        pts += 10
        reasons.append("fit")
    return max(0, min(100, pts)), reasons

def _is_bot_author(author: str) -> bool:
    a = (author or "").lower()
    return a == "automoderator" or a.endswith("bot") or "bot" in a

def _is_mod_author(author: str) -> bool:
    a = (author or "").lower()
    return "mod" in a and len(a) <= 24


def _pick_post(data: list[dict]) -> dict | None:
    try:
        children = data[0]["data"]["children"]
        if not children:
            return None
        return children[0]["data"]
    except (KeyError, IndexError, TypeError):
        return None


def _pick_comment(data: list[dict], comment_id_from_path: str | None) -> dict | None:
    try:
        children = data[1]["data"]["children"]
    except (KeyError, IndexError, TypeError):
        return None
    if not children:
        return None
    if not comment_id_from_path:
        first = children[0]
        if first.get("kind") == "t1":
            return first.get("data")
        return None
    for child in children:
        if child.get("kind") == "t1" and child.get("data", {}).get("id") == comment_id_from_path:
            return child.get("data")
    first = children[0]
    if first.get("kind") == "t1":
        return first.get("data")
    return None


def _extract_excerpt(text: str, max_len: int = 180) -> str:
    clean = " ".join((text or "").split())
    return clean if len(clean) <= max_len else f"{clean[: max_len - 3]}..."


def _infer_live_status(link_kind: str, payload: dict) -> str:
    if link_kind == "comment":
        body = (payload.get("body") or "").strip()
        if body == "[removed]":
            return "removed"
        if body == "[deleted]":
            return "deleted"
        if not body:
            return "unknown"
        return "live"
    self_text = (payload.get("selftext") or "").strip()
    if self_text == "[removed]":
        return "removed"
    if self_text == "[deleted]":
        return "deleted"
    return "live"


def _comment_body_status(body: str) -> str:
    b = (body or "").strip()
    if b == "[removed]":
        return "removed"
    if b == "[deleted]":
        return "deleted"
    if not b:
        return "unknown"
    return "live"


def _count_more_in_children(children: list[dict]) -> int:
    n = 0
    for ch in children:
        if not isinstance(ch, dict):
            continue
        if ch.get("kind") == "more":
            try:
                n += int(ch["data"].get("count") or 0)
            except (KeyError, TypeError, ValueError):
                n += 1
        elif ch.get("kind") == "t1":
            data = ch.get("data") or {}
            replies = data.get("replies")
            if isinstance(replies, dict):
                try:
                    sub = replies["data"]["children"]
                    if isinstance(sub, list):
                        n += _count_more_in_children(sub)
                except (KeyError, TypeError):
                    pass
    return n


def _walk_comments_flat(
    children: list[dict],
    post_id: str,
    *,
    depth: int = 0,
    parent_fullname: str = "",
    out: list[CommentRecord] | None = None,
) -> list[CommentRecord]:
    if out is None:
        out = []
    for ch in children:
        if not isinstance(ch, dict):
            continue
        kind = ch.get("kind")
        if kind == "more":
            continue
        if kind != "t1":
            continue
        data = ch.get("data") or {}
        cid = data.get("id") or ""
        body = data.get("body") or ""
        perm = data.get("permalink") or ""
        perm_url = f"https://www.reddit.com{perm}" if perm else ""
        edited_raw = data.get("edited")
        if edited_raw is False or edited_raw is None:
            edited_s = ""
        else:
            edited_s = _to_iso(edited_raw) if isinstance(edited_raw, (int, float)) else str(edited_raw)
        out.append(
            CommentRecord(
                post_id=post_id,
                depth=depth,
                parent_fullname=parent_fullname,
                comment_id=cid,
                author=data.get("author") or "",
                body=body,
                score=int(data.get("score") or 0),
                created_utc=_to_iso(data.get("created_utc")),
                permalink=perm_url,
                status=_comment_body_status(body),
                stickied="yes" if data.get("stickied") else "",
                edited=edited_s,
                distinguished=str(data.get("distinguished") or ""),
            )
        )
        name = data.get("name") or ""
        replies = data.get("replies")
        if isinstance(replies, dict):
            try:
                sub_children = replies["data"]["children"]
                if isinstance(sub_children, list) and sub_children:
                    _walk_comments_flat(
                        sub_children,
                        post_id,
                        depth=depth + 1,
                        parent_fullname=name,
                        out=out,
                    )
            except (KeyError, TypeError):
                pass
    return out


def _build_post_metadata(
    *,
    input_url: str,
    post: dict,
    comments_flat: list[CommentRecord],
    more_omitted: int,
) -> PostMetadata:
    edited_raw = post.get("edited")
    if edited_raw is False or edited_raw is None:
        edited_s = ""
    else:
        edited_s = _to_iso(edited_raw) if isinstance(edited_raw, (int, float)) else str(edited_raw)
    perm = post.get("permalink") or ""
    perm_url = f"https://www.reddit.com{perm}" if perm else ""
    note_parts: list[str] = []
    if more_omitted:
        note_parts.append(
            f"Reddit returned 'more' placeholders (~{more_omitted} replies not expanded in this JSON). "
            "Use the official API for full threads."
        )
    reported = int(post.get("num_comments") or 0)
    if len(comments_flat) < reported and not more_omitted:
        note_parts.append(
            f"Only {len(comments_flat)} comments in payload vs {reported} reported (normal for large threads)."
        )
    return PostMetadata(
        input_url=input_url,
        post_id=post.get("id") or "",
        title=post.get("title") or "",
        author=post.get("author") or "",
        subreddit=post.get("subreddit") or "",
        created_utc=_to_iso(post.get("created_utc")),
        score=int(post.get("score") or 0),
        ups=int(post.get("ups") or 0),
        downs=int(post.get("downs") or 0),
        upvote_ratio=float(post.get("upvote_ratio") or 0.0),
        num_comments=reported,
        is_self=bool(post.get("is_self")),
        domain=post.get("domain") or "",
        url=post.get("url") or "",
        selftext=post.get("selftext") or "",
        selftext_html=post.get("selftext_html") or "",
        link_flair_text=post.get("link_flair_text") or "",
        permalink=perm_url,
        thumbnail=post.get("thumbnail") or "",
        over_18=bool(post.get("over_18")),
        spoiler=bool(post.get("spoiler")),
        locked=bool(post.get("locked")),
        stickied=bool(post.get("stickied")),
        edited=edited_s,
        comments_returned=len(comments_flat),
        comments_omitted_more=more_omitted,
        note=" ".join(note_parts),
    )


def _fetch_thread_json(
    normalized_url: str,
    timeout: int,
    limit: int,
    max_retries: int = 0,
    *,
    thread_host: str = "www",
) -> tuple[list[dict[Any, Any]] | None, str]:
    base = _reddit_thread_url_with_host(normalized_url, thread_host)
    url = _get_json_endpoint(base, limit=limit)
    data, err = _fetch_json(url, timeout=timeout, max_retries=max_retries)
    if err or data is None:
        return None, err
    if not isinstance(data, list):
        return None, "unexpected JSON shape (expected thread list)"
    return data, ""


def _fetch_thread_json_with_mode(
    normalized_url: str,
    timeout: int,
    limit: int,
    fetch_retries: int,
    thread_fetch_mode: str,
) -> tuple[list[dict[Any, Any]] | None, str]:
    mode = (thread_fetch_mode or "www").strip().lower()
    if mode not in ("www", "old", "www_then_old"):
        mode = "www_then_old"

    if mode == "old":
        return _fetch_thread_json(normalized_url, timeout, limit, fetch_retries, thread_host="old")
    if mode == "www":
        return _fetch_thread_json(normalized_url, timeout, limit, fetch_retries, thread_host="www")

    data, err = _fetch_thread_json(normalized_url, timeout, limit, fetch_retries, thread_host="www")
    if data is not None:
        return data, err
    if err == "HTTP 404":
        return None, err
    data_o, err_o = _fetch_thread_json(normalized_url, timeout, limit, fetch_retries, thread_host="old")
    if data_o is not None:
        return data_o, ""
    merged = err
    if err_o:
        merged = f"{err} | old.reddit fallback: {err_o}"
    return None, merged


def _status_from_thread(
    url: str,
    normalized: str,
    kind: str,
    data: list[dict],
) -> RedditLinkStatus:
    post = _pick_post(data)
    if not post:
        return RedditLinkStatus(
            input_url=url,
            normalized_url=normalized,
            link_kind=kind,
            status="not_found",
            subreddit="",
            author="",
            created_utc="",
            title="",
            score=0,
            num_comments=0,
            id="",
            permalink="",
            body_excerpt="",
            error="No post payload found",
        )

    comment_match = COMMENT_PATH_RE.search(urlparse(normalized).path)
    comment_id = comment_match.group(2) if comment_match else None
    payload = _pick_comment(data, comment_id) if kind == "comment" else post
    if kind == "comment" and not payload:
        return RedditLinkStatus(
            input_url=url,
            normalized_url=normalized,
            link_kind=kind,
            status="not_found",
            subreddit=post.get("subreddit", ""),
            author="",
            created_utc="",
            title=post.get("title", ""),
            score=0,
            num_comments=int(post.get("num_comments") or 0),
            id="",
            permalink="",
            body_excerpt="",
            error="Comment not found in thread payload",
        )

    status = _infer_live_status(kind, payload)
    excerpt_source = payload.get("body", "") if kind == "comment" else payload.get("selftext", "")
    permalink = payload.get("permalink", "")
    permalink_url = f"https://www.reddit.com{permalink}" if permalink else ""

    return RedditLinkStatus(
        input_url=url,
        normalized_url=normalized,
        link_kind=kind,
        status=status,
        subreddit=payload.get("subreddit", ""),
        author=payload.get("author", ""),
        created_utc=_to_iso(payload.get("created_utc")),
        title=(post.get("title", "") if kind == "comment" else payload.get("title", "")),
        score=int(payload.get("score") or 0),
        num_comments=int(post.get("num_comments") or 0),
        id=payload.get("id", ""),
        permalink=permalink_url,
        body_excerpt=_extract_excerpt(excerpt_source),
        error="",
    )


def check_one(
    url: str,
    forced_kind: str = "auto",
    timeout: int = 20,
    json_limit: int = DEFAULT_JSON_LIMIT,
    *,
    fetch_retries: int = 3,
    thread_fetch_mode: str = "www_then_old",
) -> RedditLinkStatus:
    try:
        normalized = _normalize_reddit_url(url)
    except Exception as exc:  # noqa: BLE001
        return RedditLinkStatus(
            input_url=url,
            normalized_url="",
            link_kind=forced_kind,
            status="invalid_url",
            subreddit="",
            author="",
            created_utc="",
            title="",
            score=0,
            num_comments=0,
            id="",
            permalink="",
            body_excerpt="",
            error=str(exc),
        )

    kind = _detect_kind(urlparse(normalized).path, forced_kind)
    if kind == "unknown":
        return RedditLinkStatus(
            input_url=url,
            normalized_url=normalized,
            link_kind="unknown",
            status="unsupported_url",
            subreddit="",
            author="",
            created_utc="",
            title="",
            score=0,
            num_comments=0,
            id="",
            permalink="",
            body_excerpt="",
            error="Only post/comment URLs are supported",
        )

    data, err = _fetch_thread_json_with_mode(
        normalized,
        timeout=timeout,
        limit=json_limit,
        fetch_retries=fetch_retries,
        thread_fetch_mode=thread_fetch_mode,
    )
    if data is None:
        if err == "HTTP 404":
            return RedditLinkStatus(
                input_url=url,
                normalized_url=normalized,
                link_kind=kind,
                status="not_found",
                subreddit="",
                author="",
                created_utc="",
                title="",
                score=0,
                num_comments=0,
                id="",
                permalink="",
                body_excerpt="",
                error=err,
            )
        if err == "HTTP 403":
            return RedditLinkStatus(
                input_url=url,
                normalized_url=normalized,
                link_kind=kind,
                status="restricted",
                subreddit="",
                author="",
                created_utc="",
                title="",
                score=0,
                num_comments=0,
                id="",
                permalink="",
                body_excerpt="",
                error=err,
            )
        return RedditLinkStatus(
            input_url=url,
            normalized_url=normalized,
            link_kind=kind,
            status="fetch_error",
            subreddit="",
            author="",
            created_utc="",
            title="",
            score=0,
            num_comments=0,
            id="",
            permalink="",
            body_excerpt="",
            error=err,
        )

    return _status_from_thread(url, normalized, kind, data)


def extract_thread(
    url: str,
    forced_kind: str = "auto",
    timeout: int = 20,
    json_limit: int = DEFAULT_JSON_LIMIT,
    *,
    include_post: bool = True,
    include_comments: bool = True,
    fetch_retries: int = 3,
    thread_fetch_mode: str = "www_then_old",
) -> ThreadExtraction:
    """Fetch once; return link status plus full post metadata and flattened comments when possible."""
    try:
        normalized = _normalize_reddit_url(url)
    except Exception as exc:  # noqa: BLE001
        return ThreadExtraction(
            status=RedditLinkStatus(
                input_url=url,
                normalized_url="",
                link_kind=forced_kind,
                status="invalid_url",
                subreddit="",
                author="",
                created_utc="",
                title="",
                score=0,
                num_comments=0,
                id="",
                permalink="",
                body_excerpt="",
                error=str(exc),
            ),
            post=None,
            comments=[],
        )

    kind = _detect_kind(urlparse(normalized).path, forced_kind)
    if kind == "unknown":
        return ThreadExtraction(
            status=RedditLinkStatus(
                input_url=url,
                normalized_url=normalized,
                link_kind="unknown",
                status="unsupported_url",
                subreddit="",
                author="",
                created_utc="",
                title="",
                score=0,
                num_comments=0,
                id="",
                permalink="",
                body_excerpt="",
                error="Only post/comment URLs are supported",
            ),
            post=None,
            comments=[],
        )

    data, err = _fetch_thread_json_with_mode(
        normalized,
        timeout=timeout,
        limit=json_limit,
        fetch_retries=fetch_retries,
        thread_fetch_mode=thread_fetch_mode,
    )
    if data is None:
        if err == "HTTP 404":
            st_err = "not_found"
        elif err == "HTTP 403":
            st_err = "restricted"
        else:
            st_err = "fetch_error"
        return ThreadExtraction(
            status=RedditLinkStatus(
                input_url=url,
                normalized_url=normalized,
                link_kind=kind,
                status=st_err,
                subreddit="",
                author="",
                created_utc="",
                title="",
                score=0,
                num_comments=0,
                id="",
                permalink="",
                body_excerpt="",
                error=err,
            ),
            post=None,
            comments=[],
        )

    status = _status_from_thread(url, normalized, kind, data)
    post_d = _pick_post(data)
    if not post_d:
        return ThreadExtraction(status=status, post=None, comments=[])

    if not include_post and not include_comments:
        return ThreadExtraction(status=status, post=None, comments=[])

    post_id = post_d.get("id") or ""
    try:
        comment_children = data[1]["data"]["children"]
        if not isinstance(comment_children, list):
            comment_children = []
    except (KeyError, IndexError, TypeError):
        comment_children = []

    comments_flat: list[CommentRecord] = []
    more_omitted = 0
    if include_comments:
        more_omitted = _count_more_in_children(comment_children)
        comments_flat = _walk_comments_flat(comment_children, post_id)

    post_meta = None
    if include_post:
        post_meta = _build_post_metadata(
            input_url=url,
            post=post_d,
            comments_flat=comments_flat,
            more_omitted=more_omitted,
        )
    return ThreadExtraction(status=status, post=post_meta, comments=comments_flat)


def check_many(urls: Iterable[str], forced_kind: str = "auto") -> list[RedditLinkStatus]:
    return [check_one(url=u, forced_kind=forced_kind) for u in urls if u.strip()]


def extract_many_threads(
    urls: Iterable[str], forced_kind: str = "auto", json_limit: int = DEFAULT_JSON_LIMIT
) -> list[ThreadExtraction]:
    return [extract_thread(url=u, forced_kind=forced_kind, json_limit=json_limit) for u in urls if u.strip()]


def _subreddit_rules_markdown_from_api(rules: Any) -> str:
    """Build rules text from /r/x/about/rules.json (custom rules, description-only, site_rules)."""
    if not isinstance(rules, dict):
        return ""
    rr = rules.get("rules") or []
    lines: list[str] = []
    if isinstance(rr, list) and rr:
        for i, r in enumerate(rr, start=1):
            if not isinstance(r, dict):
                continue
            short = (r.get("short_name") or r.get("shortname") or "").strip()
            desc = (r.get("description") or "").strip()
            if short and desc:
                lines.append(f"{i}. {short}: {desc}")
            elif short:
                lines.append(f"{i}. {short}")
            elif desc:
                lines.append(f"{i}. {desc}")
    rules_md = "\n".join(lines)

    site_rules = rules.get("site_rules")
    if isinstance(site_rules, list) and site_rules:
        sr_texts: list[str] = []
        for s in site_rules:
            if isinstance(s, str) and s.strip():
                sr_texts.append(s.strip())
        if sr_texts:
            numbered = "\n".join(f"{j}. {t}" for j, t in enumerate(sr_texts, start=1))
            if rules_md:
                rules_md = rules_md + "\n\n---\nReddit site rules:\n" + numbered
            else:
                rules_md = "Reddit site rules:\n" + numbered
    return rules_md


def _wiki_rules_excerpt(wiki_page: Any, max_len: int = 6000) -> str:
    """First chunk of /r/x/wiki/rules.json content_md when API rules are empty."""
    if not isinstance(wiki_page, dict):
        return ""
    data = wiki_page.get("data") or {}
    md = (data.get("content_md") or "").strip()
    if not md:
        return ""
    if len(md) > max_len:
        return md[:max_len].rstrip() + "\n\n… (truncated)"
    return md


def extract_subreddits(
    names: Iterable[str],
    timeout: int = 20,
    *,
    fetch_retries: int = 3,
    pause_between_requests_sec: float = 0.15,
    pause_between_subreddits_sec: float = 0.0,
    subreddit_host: str = "www",
) -> list[SubredditRecord]:
    out: list[SubredditRecord] = []
    host = (subreddit_host or "www").strip().lower()
    if host not in ("www", "old"):
        host = "www"
    root = "https://old.reddit.com" if host == "old" else "https://www.reddit.com"

    first_sub = True
    for raw in names:
        name = _get_subreddit_name(raw)
        if not name:
            continue
        if not first_sub and pause_between_subreddits_sec > 0:
            time.sleep(pause_between_subreddits_sec)
        first_sub = False

        base = f"{root}/r/{name}"
        about_url = f"{base}/about.json?raw_json=1"
        rules_url = f"{base}/about/rules.json?raw_json=1"
        mods_url = f"{base}/about/moderators.json?raw_json=1"
        widgets_url = f"{base}/api/widgets?raw_json=1"

        about, about_err = _fetch_json(about_url, timeout=timeout, max_retries=fetch_retries)
        if pause_between_requests_sec > 0:
            time.sleep(pause_between_requests_sec)
        rules, rules_err = _fetch_json(rules_url, timeout=timeout, max_retries=fetch_retries)
        rules_md = _subreddit_rules_markdown_from_api(rules) if isinstance(rules, dict) else ""
        if not (rules_md or "").strip():
            if pause_between_requests_sec > 0:
                time.sleep(pause_between_requests_sec)
            wiki_url = f"https://www.reddit.com/r/{name}/wiki/rules.json?raw_json=1"
            wiki_page, _ = _fetch_json(wiki_url, timeout=timeout, max_retries=fetch_retries)
            wex = _wiki_rules_excerpt(wiki_page)
            if wex:
                rules_md = wex

        if pause_between_requests_sec > 0:
            time.sleep(pause_between_requests_sec)
        mods, mods_err = _fetch_json(mods_url, timeout=timeout, max_retries=fetch_retries)
        if pause_between_requests_sec > 0:
            time.sleep(pause_between_requests_sec)
        widgets, widgets_err = _fetch_json(widgets_url, timeout=timeout, max_retries=fetch_retries)

        title = ""
        public_description = ""
        subscribers = 0
        active_user_count = 0
        created_utc = ""
        over18 = False
        if isinstance(about, dict):
            data = about.get("data") or {}
            title = data.get("title") or ""
            public_description = data.get("public_description") or ""
            subscribers = int(data.get("subscribers") or 0)
            active_user_count = int(data.get("active_user_count") or 0)
            created_utc = _to_iso(data.get("created_utc"))
            over18 = bool(data.get("over18"))

        moderators = ""
        if isinstance(mods, dict):
            md = mods.get("data") or {}
            children = md.get("children") or []
            if isinstance(children, list) and children:
                names = []
                for c in children:
                    if isinstance(c, dict) and c.get("name"):
                        names.append(str(c["name"]))
                moderators = ", ".join(names)

        widget_names = []
        if isinstance(widgets, dict):
            # Widgets endpoint is sometimes restricted; if available, return widget titles.
            items = widgets.get("items") or {}
            if isinstance(items, dict):
                for _id, item in items.items():
                    if not isinstance(item, dict):
                        continue
                    wname = item.get("shortName") or item.get("name") or item.get("kind") or ""
                    wkind = item.get("kind") or ""
                    label = wname.strip() or wkind.strip()
                    if label:
                        widget_names.append(label)
        widgets_s = ", ".join(sorted(set(widget_names)))

        errs = [e for e in [about_err, rules_err, mods_err, widgets_err] if e]
        error = "; ".join(errs)

        out.append(
            SubredditRecord(
                input_name=str(raw),
                normalized_name=name,
                subreddit_url=f"https://www.reddit.com/r/{name}/",
                title=title,
                public_description=public_description,
                subscribers=subscribers,
                active_user_count=active_user_count,
                created_utc=created_utc,
                over18=over18,
                rules_markdown=rules_md,
                moderators=moderators if moderators else ("" if not mods_err else f"(unavailable: {mods_err})"),
                widgets=widgets_s if widgets_s else ("" if not widgets_err else f"(unavailable: {widgets_err})"),
                error=error,
            )
        )
    return out


def search_posts_no_api(
    query: str,
    *,
    subreddits: list[str] | None = None,
    sort: str = "new",
    time_filter: str = "week",
    limit: int = 25,
    timeout: int = 20,
    max_retries: int = 0,
) -> list[SearchResultRecord]:
    q = (query or "").strip()
    if not q:
        return []
    sr = [s for s in (_get_subreddit_name(x) for x in (subreddits or [])) if s]
    if sr:
        sr_expr = " OR ".join(f"subreddit:{x}" for x in sr)
        q = f"({q}) ({sr_expr})"
    url = (
        "https://www.reddit.com/search.json?"
        f"q={quote_plus(q)}&sort={quote_plus(sort)}&t={quote_plus(time_filter)}&limit={int(limit)}&raw_json=1"
    )
    payload, err = _fetch_json(url, timeout=timeout, max_retries=max_retries)
    if err or not isinstance(payload, dict):
        return []
    children = (((payload.get("data") or {}).get("children")) or [])
    out: list[SearchResultRecord] = []
    for ch in children:
        if not isinstance(ch, dict) or ch.get("kind") != "t3":
            continue
        d = ch.get("data") or {}
        title = d.get("title") or ""
        body = d.get("selftext") or ""
        lead_score, reasons = _lead_score(title, body, int(d.get("num_comments") or 0), int(d.get("score") or 0))
        perm = d.get("permalink") or ""
        post_url = f"https://www.reddit.com{perm}" if perm else ""
        out.append(
            SearchResultRecord(
                query=query,
                subreddit=d.get("subreddit") or "",
                post_id=d.get("id") or "",
                title=title,
                author=d.get("author") or "",
                created_utc=_to_iso(d.get("created_utc")),
                score=int(d.get("score") or 0),
                num_comments=int(d.get("num_comments") or 0),
                upvote_ratio=float(d.get("upvote_ratio") or 0.0),
                permalink=post_url,
                post_url=post_url,
                selftext_excerpt=_extract_excerpt(body, max_len=220),
                lead_score=lead_score,
                lead_reasons=", ".join(reasons),
                duplicate_group="",
            )
        )

    # Duplicate/repost grouping by normalized title key
    grouped: dict[str, list[int]] = {}
    for i, row in enumerate(out):
        key = _normalized_title_key(row.title)
        if not key:
            continue
        grouped.setdefault(key, []).append(i)
    for key, idxs in grouped.items():
        if len(idxs) <= 1:
            continue
        grp = f"dup:{abs(hash(key)) % 100000}"
        for i in idxs:
            out[i].duplicate_group = grp
    return out


def build_google_sheets_rows(
    search_rows: list[SearchResultRecord],
    *,
    include_fields: list[str],
) -> str:
    return dataclass_rows_to_tsv(search_rows, include_fields)


def summarize_post_health(
    url: str,
    forced_kind: str = "auto",
    json_limit: int = DEFAULT_JSON_LIMIT,
    *,
    timeout: int = 20,
    fetch_retries: int = 3,
    thread_fetch_mode: str = "www_then_old",
    pause_before_subreddit_sec: float = 0.35,
    subreddit_fetch_retries: int = 3,
    pause_between_subreddit_requests_sec: float = 0.15,
    subreddit_host: str = "www",
    skip_subreddit_meta: bool = False,
) -> tuple[PostHealthRecord, SubredditRecord | None]:
    ex = extract_thread(
        url,
        forced_kind=forced_kind,
        timeout=timeout,
        json_limit=json_limit,
        include_post=True,
        include_comments=True,
        fetch_retries=fetch_retries,
        thread_fetch_mode=thread_fetch_mode,
    )
    st = ex.status
    post = ex.post
    comments = ex.comments
    subreddit_meta = None
    if not skip_subreddit_meta and post and post.subreddit:
        if pause_before_subreddit_sec > 0:
            time.sleep(pause_before_subreddit_sec)
        ss = extract_subreddits(
            [post.subreddit],
            timeout=timeout,
            fetch_retries=subreddit_fetch_retries,
            pause_between_requests_sec=pause_between_subreddit_requests_sec,
            subreddit_host=subreddit_host,
        )
        subreddit_meta = ss[0] if ss else None

    mod_comments = 0
    bot_comments = 0
    removed_deleted = 0
    last_comment = ""
    last_comment_live = False
    for c in comments:
        if (c.distinguished or "").lower() == "moderator":
            mod_comments += 1
        if _is_bot_author(c.author):
            bot_comments += 1
        if c.status in {"removed", "deleted"}:
            removed_deleted += 1
        if c.created_utc and (not last_comment or c.created_utc > last_comment):
            last_comment = c.created_utc
            last_comment_live = c.status == "live"

    can_comment = "Unknown"
    rationale_parts: list[str] = []
    if st.status == "fetch_error":
        can_comment = "No"
        rationale_parts.append("thread fetch failed (rate limit or network); try old.reddit mode, skip subreddit snapshot, or slow down")
        if (st.error or "").strip():
            rationale_parts.append(st.error[:200])
    elif st.status == "restricted":
        can_comment = "No"
        rationale_parts.append("access restricted (HTTP 403) for thread or region")
        if (st.error or "").strip():
            rationale_parts.append(st.error[:200])
    elif st.status != "live":
        can_comment = "No"
        rationale_parts.append(f"post status={st.status}")
    elif post and post.locked:
        can_comment = "No"
        rationale_parts.append("post is locked")
    else:
        can_comment = "Likely yes"
        rationale_parts.append("post live and unlocked")
    if removed_deleted > 0:
        rationale_parts.append(f"{removed_deleted} removed/deleted comments in thread")
    if bot_comments > 0:
        rationale_parts.append(f"{bot_comments} bot comments detected")
    if mod_comments > 0:
        rationale_parts.append(f"{mod_comments} possible mod comments detected")

    top_comment_snippets = []
    live_sorted = sorted([c for c in comments if c.status == "live"], key=lambda x: x.score, reverse=True)
    for c in live_sorted[:3]:
        snippet = _extract_excerpt(c.body or "", max_len=100)
        if snippet:
            top_comment_snippets.append(f"{c.author}: {snippet}")

    subreddit_desc_excerpt = ""
    subreddit_rules_excerpt = ""
    if subreddit_meta:
        subreddit_desc_excerpt = _extract_excerpt(subreddit_meta.public_description or "", max_len=180)
        subreddit_rules_excerpt = _extract_excerpt(subreddit_meta.rules_markdown or "", max_len=500)

    post_created = (post.created_utc if post else "") or (st.created_utc if st else "")
    h = PostHealthRecord(
        input_url=url,
        status=st.status,
        subreddit=post.subreddit if post else st.subreddit,
        title=post.title if post else st.title,
        post_created_utc=post_created,
        post_author=post.author if post else st.author,
        locked=bool(post.locked) if post else False,
        stickied=bool(post.stickied) if post else False,
        over_18=bool(post.over_18) if post else False,
        num_comments_reported=int(post.num_comments) if post else int(st.num_comments),
        comments_in_payload=len(comments),
        last_comment_utc=last_comment,
        last_comment_live=last_comment_live,
        mod_comment_count=mod_comments,
        bot_comment_count=bot_comments,
        removed_deleted_comment_count=removed_deleted,
        post_ups=int(post.ups) if post else 0,
        post_downs=int(post.downs) if post else 0,
        post_upvote_ratio=float(post.upvote_ratio) if post else 0.0,
        post_body_excerpt=_extract_excerpt(post.selftext if post else "", max_len=260),
        top_comments_excerpt=" | ".join(top_comment_snippets),
        subreddit_description_excerpt=subreddit_desc_excerpt,
        subreddit_rules_excerpt=subreddit_rules_excerpt,
        can_post_new_comment=can_comment,
        rationale="; ".join(rationale_parts),
    )
    return h, subreddit_meta


def extract_reddit_users(
    usernames: Iterable[str],
    sample_limit: int = 10,
    timeout: int = 20,
    *,
    source: str = "json",
    delay_between_profiles_sec: float = 1.25,
    fetch_retries: int = 3,
    pause_between_requests_sec: float = 0.2,
) -> list[RedditUserRecord]:
    """Fetch public profile data.

    ``source``: ``json`` (three .json calls per user, richer) or ``old_reddit`` (one old.reddit HTML page per user, lighter on API limits).
    """
    src = (source or "json").strip().lower()
    if src not in ("json", "old_reddit"):
        src = "json"

    out: list[RedditUserRecord] = []
    seen_profile = False
    for raw in usernames:
        input_username = (raw or "").strip()
        if not input_username:
            continue
        username = input_username
        if username.startswith("u/"):
            username = username[2:]
        username = username.strip().strip("/")
        if "/" in username:
            username = username.split("/", 1)[0]
        if not username:
            continue

        if seen_profile and delay_between_profiles_sec > 0:
            time.sleep(delay_between_profiles_sec)
        seen_profile = True

        profile_url = f"https://www.reddit.com/user/{username}/"

        if src == "old_reddit":
            page_url = f"https://old.reddit.com/user/{username}/"
            resp, page_err = _reddit_get(page_url, timeout=timeout, max_retries=fetch_retries)
            html = (resp.text if resp else "") or ""
            head = html[: min(len(html), 80000)]

            status = "live"
            if page_err == "HTTP 404":
                status = "not_found"
            elif page_err == "HTTP 403":
                status = "restricted"
            elif page_err:
                status = "fetch_error"

            if page_err in ("HTTP 404", "HTTP 403"):
                created, link_karma, comment_karma, post_titles, comment_excerpts, latest_activity, hint, pnote = (
                    "",
                    0,
                    0,
                    [],
                    [],
                    "",
                    "",
                    "",
                )
            else:
                created, link_karma, comment_karma, post_titles, comment_excerpts, latest_activity, hint, pnote = (
                    _parse_old_reddit_user_overview(html, sample_limit) if html else ("", 0, 0, [], [], "", "", "")
                )
            if hint == "restricted":
                status = "restricted"
            elif hint == "not_found":
                status = "not_found"
            elif status == "live" and html and "titlebox" not in html.lower() and not page_err:
                status = "fetch_error"
                pnote = "; ".join(x for x in [pnote, "unexpected HTML (no titlebox)"] if x)

            total_karma = link_karma + comment_karma
            errs = [e for e in [page_err, pnote] if e]
            verified_email = "verified email" in head.lower()
            is_mod = "moderator of" in head.lower()
            is_gold = "reddit premium" in head.lower()

            out.append(
                RedditUserRecord(
                    input_username=input_username,
                    username=username,
                    profile_url=profile_url,
                    status=status,
                    account_created_utc=created,
                    total_karma=total_karma,
                    link_karma=link_karma,
                    comment_karma=comment_karma,
                    is_mod=is_mod,
                    is_gold=is_gold,
                    verified_email=verified_email,
                    has_subscribed=False,
                    icon_img="",
                    recent_posts_count=len(post_titles),
                    recent_comments_count=len(comment_excerpts),
                    latest_activity_utc=latest_activity,
                    recent_post_titles=" | ".join(post_titles[:5]),
                    recent_comment_excerpts=" | ".join(comment_excerpts[:5]),
                    error="; ".join(errs),
                )
            )
            continue

        about_url = f"https://www.reddit.com/user/{username}/about.json?raw_json=1"
        submitted_url = (
            f"https://www.reddit.com/user/{username}/submitted.json?raw_json=1&limit={int(sample_limit)}"
        )
        comments_url = (
            f"https://www.reddit.com/user/{username}/comments.json?raw_json=1&limit={int(sample_limit)}"
        )

        about, about_err = _fetch_json(about_url, timeout=timeout, max_retries=fetch_retries)
        if pause_between_requests_sec > 0:
            time.sleep(pause_between_requests_sec)
        submitted, sub_err = _fetch_json(submitted_url, timeout=timeout, max_retries=fetch_retries)
        if pause_between_requests_sec > 0:
            time.sleep(pause_between_requests_sec)
        comments, com_err = _fetch_json(comments_url, timeout=timeout, max_retries=fetch_retries)

        status = "live"
        if about_err == "HTTP 404":
            status = "not_found"
        elif about_err == "HTTP 403":
            status = "restricted"
        elif about_err:
            status = "fetch_error"

        data = (about or {}).get("data", {}) if isinstance(about, dict) else {}
        created = _to_iso(data.get("created_utc"))
        link_karma = int(data.get("link_karma") or 0)
        comment_karma = int(data.get("comment_karma") or 0)
        total_karma = int(data.get("total_karma") or (link_karma + comment_karma))

        sub_children = (((submitted or {}).get("data") or {}).get("children") or []) if isinstance(submitted, dict) else []
        com_children = (((comments or {}).get("data") or {}).get("children") or []) if isinstance(comments, dict) else []

        post_titles: list[str] = []
        latest_activity = ""
        for ch in sub_children:
            if not isinstance(ch, dict):
                continue
            d = ch.get("data") or {}
            title = _compact_text(d.get("title") or "")
            if title:
                post_titles.append(title)
            c = _to_iso(d.get("created_utc"))
            if c and (not latest_activity or c > latest_activity):
                latest_activity = c

        comment_excerpts: list[str] = []
        for ch in com_children:
            if not isinstance(ch, dict):
                continue
            d = ch.get("data") or {}
            body = _extract_excerpt(d.get("body") or "", max_len=120)
            if body:
                comment_excerpts.append(body)
            c = _to_iso(d.get("created_utc"))
            if c and (not latest_activity or c > latest_activity):
                latest_activity = c

        errs = [e for e in [about_err, sub_err, com_err] if e]
        out.append(
            RedditUserRecord(
                input_username=input_username,
                username=username,
                profile_url=profile_url,
                status=status,
                account_created_utc=created,
                total_karma=total_karma,
                link_karma=link_karma,
                comment_karma=comment_karma,
                is_mod=bool(data.get("is_mod")),
                is_gold=bool(data.get("is_gold")),
                verified_email=bool(data.get("has_verified_email")),
                has_subscribed=bool(data.get("has_subscribed")),
                icon_img=data.get("icon_img") or "",
                recent_posts_count=len(post_titles),
                recent_comments_count=len(comment_excerpts),
                latest_activity_utc=latest_activity,
                recent_post_titles=" | ".join(post_titles[:5]),
                recent_comment_excerpts=" | ".join(comment_excerpts[:5]),
                error="; ".join(errs),
            )
        )
    return out


def dataclass_rows_to_tsv(rows: list[Any], fields: list[str]) -> str:
    """TSV is easiest to paste into Google Sheets."""
    header = "\t".join(fields)
    lines = [header]
    for row in rows:
        d = asdict(row)
        vals: list[str] = []
        for f in fields:
            v = d.get(f, "")
            s = str(v).replace("\r\n", "\n").replace("\r", "\n").replace("\t", " ")
            s = s.replace("\n", " ")
            vals.append(s)
        lines.append("\t".join(vals))
    return "\n".join(lines)

def to_csv(path: Path, rows: list[RedditLinkStatus]) -> None:
    fieldnames = list(asdict(rows[0]).keys()) if rows else list(RedditLinkStatus.__dataclass_fields__.keys())
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(asdict(row))


def to_markdown(rows: list[RedditLinkStatus]) -> str:
    columns = [
        "input_url",
        "link_kind",
        "status",
        "subreddit",
        "author",
        "created_utc",
        "score",
        "num_comments",
        "permalink",
        "error",
    ]
    header = "| " + " | ".join(columns) + " |"
    sep = "| " + " | ".join(["---"] * len(columns)) + " |"
    body_lines = []
    for row in rows:
        data = asdict(row)
        body_lines.append("| " + " | ".join(str(data[c]).replace("\n", " ") for c in columns) + " |")
    return "\n".join([header, sep, *body_lines])


def post_metadata_to_markdown(p: PostMetadata) -> str:
    lines = [
        f"# {p.title}",
        "",
        f"- **Subreddit:** r/{p.subreddit}",
        f"- **Author:** u/{p.author}" if p.author else "- **Author:** (unknown)",
        f"- **Posted:** {p.created_utc}",
        f"- **Score:** {p.score} (ups {p.ups}, downs {p.downs}, ratio {p.upvote_ratio})",
        f"- **Comments (reported):** {p.num_comments}",
        f"- **Comments in this extract:** {p.comments_returned}",
        f"- **Self post:** {p.is_self}",
        f"- **Domain:** {p.domain}",
        f"- **Link URL:** {p.url}",
        f"- **Flair:** {p.link_flair_text}" if p.link_flair_text else "- **Flair:** —",
        f"- **Permalink:** {p.permalink}",
        f"- **NSFW:** {p.over_18} · **Spoiler:** {p.spoiler} · **Locked:** {p.locked} · **Stickied:** {p.stickied}",
    ]
    if p.note:
        lines.extend(["", f"> **Note:** {p.note}"])
    lines.extend(["", "## Post body", "", p.selftext or "_(empty or link-only)_", ""])
    return "\n".join(lines)


def comments_to_markdown(comments: list[CommentRecord]) -> str:
    if not comments:
        return "_No comments in payload._\n"
    lines = ["## Comments", ""]
    for c in comments:
        indent = "  " * c.depth
        head = f"{indent}- **depth {c.depth}** u/{c.author} · score {c.score} · [{c.status}] · {c.created_utc}"
        if c.stickied:
            head += " · stickied"
        lines.append(head)
        body = (c.body or "").replace("\r\n", "\n")
        for ln in body.split("\n"):
            lines.append(f"{indent}  {ln}")
        lines.append(f"{indent}  _{c.permalink}_")
        lines.append("")
    return "\n".join(lines)


def thread_extraction_to_markdown(t: ThreadExtraction) -> str:
    parts = [
        "## Link check summary",
        "",
        f"- **Your URL:** {t.status.input_url}",
        f"- **Kind:** {t.status.link_kind} · **Status:** {t.status.status}",
    ]
    if t.status.error:
        parts.append(f"- **Error:** {t.status.error}")
    parts.append("")
    if t.post:
        parts.append(post_metadata_to_markdown(t.post))
        parts.append(comments_to_markdown(t.comments))
    else:
        parts.append("_No post metadata (fetch or parse failed)._")
    return "\n".join(parts)


def thread_extractions_to_json_bytes(extractions: list[ThreadExtraction]) -> bytes:
    payload = []
    for t in extractions:
        item: dict[str, Any] = {"status": asdict(t.status)}
        if t.post:
            item["post"] = asdict(t.post)
        else:
            item["post"] = None
        item["comments"] = [asdict(c) for c in t.comments]
        payload.append(item)
    return json.dumps(payload, indent=2, ensure_ascii=False).encode("utf-8")


def _dataclass_list_to_csv_bytes(rows: list[Any]) -> bytes:
    import io as _io

    if not rows:
        return b""
    fieldnames = list(asdict(rows[0]).keys())
    buf = _io.StringIO()
    w = csv.DictWriter(buf, fieldnames=fieldnames)
    w.writeheader()
    for row in rows:
        w.writerow(asdict(row))
    return buf.getvalue().encode("utf-8")


def posts_to_csv_bytes(posts: list[PostMetadata]) -> bytes:
    return _dataclass_list_to_csv_bytes(posts)


def comments_to_csv_bytes(comments: list[CommentRecord]) -> bytes:
    return _dataclass_list_to_csv_bytes(comments)


def _load_urls_from_file(path: Path) -> list[str]:
    lines = path.read_text(encoding="utf-8").splitlines()
    return [line.strip() for line in lines if line.strip()]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Check Reddit post/comment URLs for live/removed/deleted status without API credentials."
    )
    parser.add_argument("--input-file", type=Path, help="Text file with one Reddit URL per line")
    parser.add_argument("--url", action="append", default=[], help="A Reddit URL to check. Can be repeated.")
    parser.add_argument("--type", choices=["auto", "post", "comment"], default="auto", help="Force link type")
    parser.add_argument("--csv-out", type=Path, default=Path("reddit_link_status.csv"), help="CSV output path")
    parser.add_argument("--md-out", type=Path, default=Path("reddit_link_status.md"), help="Markdown output path")
    parser.add_argument(
        "--extract-thread",
        action="store_true",
        help="Also pull full post metadata and comments (extra CSV/MD/JSON outputs).",
    )
    parser.add_argument("--posts-csv", type=Path, default=Path("reddit_posts_extract.csv"))
    parser.add_argument("--comments-csv", type=Path, default=Path("reddit_comments_extract.csv"))
    parser.add_argument("--full-md-out", type=Path, default=Path("reddit_thread_full.md"))
    parser.add_argument("--json-out", type=Path, default=Path("reddit_thread_extract.json"))
    parser.add_argument("--json-limit", type=int, default=DEFAULT_JSON_LIMIT, help="Reddit ?limit= for comment listing")
    args = parser.parse_args()

    urls = list(args.url)
    if args.input_file:
        urls.extend(_load_urls_from_file(args.input_file))
    if not urls:
        raise SystemExit("No URLs provided. Use --url or --input-file.")

    if args.extract_thread:
        extractions = extract_many_threads(urls, forced_kind=args.type, json_limit=args.json_limit)
        rows = [e.status for e in extractions]
        to_csv(args.csv_out, rows)
        args.md_out.write_text(to_markdown(rows), encoding="utf-8")
        posts = [e.post for e in extractions if e.post]
        all_comments: list[CommentRecord] = []
        for e in extractions:
            all_comments.extend(e.comments)
        if posts:
            args.posts_csv.write_bytes(posts_to_csv_bytes(posts))
        if all_comments:
            args.comments_csv.write_bytes(comments_to_csv_bytes(all_comments))
        full_md = "\n\n---\n\n".join(thread_extraction_to_markdown(e) for e in extractions)
        args.full_md_out.write_text(full_md, encoding="utf-8")
        args.json_out.write_bytes(thread_extractions_to_json_bytes(extractions))
        print(json.dumps([asdict(r) for r in rows], indent=2))
        print(f"\nSaved status CSV: {args.csv_out.resolve()}")
        print(f"Saved status MD: {args.md_out.resolve()}")
        print(f"Saved full thread MD: {args.full_md_out.resolve()}")
        print(f"Saved JSON: {args.json_out.resolve()}")
        if posts:
            print(f"Saved posts CSV: {args.posts_csv.resolve()}")
        if all_comments:
            print(f"Saved comments CSV: {args.comments_csv.resolve()}")
    else:
        rows = check_many(urls=urls, forced_kind=args.type)
        to_csv(args.csv_out, rows)
        args.md_out.write_text(to_markdown(rows), encoding="utf-8")
        print(json.dumps([asdict(r) for r in rows], indent=2))
        print(f"\nSaved CSV to: {args.csv_out.resolve()}")
        print(f"Saved Markdown to: {args.md_out.resolve()}")


if __name__ == "__main__":
    main()
