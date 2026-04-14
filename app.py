from __future__ import annotations

import json
import time
import uuid
from datetime import datetime, timezone
from dataclasses import asdict
from pathlib import Path

import streamlit as st

from reddit_status_checker import (
    CommentRecord,
    PostHealthRecord,
    RedditLinkStatus,
    SearchResultRecord,
    SubredditRecord,
    RedditUserRecord,
    check_one,
    dataclass_rows_to_tsv,
    extract_reddit_users,
    extract_subreddits,
    extract_thread,
    search_posts_no_api,
    summarize_post_health,
)


def _copy_button(label: str, text: str, key: str) -> None:
    btn_id = f"copybtn_{key}"
    status_id = f"copystatus_{key}"
    st.components.v1.html(
        f"""
        <div style="margin-top:8px">
          <button id="{btn_id}" style="padding:6px 10px; transition: all .2s ease;">{label}</button>
          <span id="{status_id}" style="margin-left:10px;font-size:12px;opacity:0;transition:opacity .2s ease;"></span>
        </div>
        <script>
          const txt = {json.dumps(text)};
          const btn = document.getElementById('{btn_id}');
          const s = document.getElementById('{status_id}');
          const originalLabel = {json.dumps(label)};
          btn.onclick = async () => {{
            try {{
              await navigator.clipboard.writeText(txt);
              btn.textContent = "Copied";
              btn.style.background = "#16a34a";
              btn.style.color = "#fff";
              btn.style.transform = "scale(0.98)";
              s.textContent = "Copied to clipboard";
              s.style.opacity = "1";
              setTimeout(() => {{
                btn.textContent = originalLabel;
                btn.style.background = "";
                btn.style.color = "";
                btn.style.transform = "";
                s.style.opacity = "0";
              }}, 1800);
            }} catch (e) {{
              btn.textContent = "Copy failed";
              btn.style.background = "#dc2626";
              btn.style.color = "#fff";
              s.textContent = "Copy failed. Select and copy manually.";
              s.style.opacity = "1";
              setTimeout(() => {{
                btn.textContent = originalLabel;
                btn.style.background = "";
                btn.style.color = "";
                s.style.opacity = "0";
              }}, 2200);
            }}
          }};
        </script>
        """,
        height=45,
    )


def _rows_to_table(rows: list, fields: list[str]) -> list[dict]:
    return [{k: asdict(r).get(k, "") for k in fields} for r in rows]


def _to_markdown_table(rows: list, fields: list[str]) -> str:
    if not rows or not fields:
        return ""
    header = "| " + " | ".join(fields) + " |"
    sep = "| " + " | ".join(["---"] * len(fields)) + " |"
    body = []
    for r in rows:
        d = asdict(r)
        body.append("| " + " | ".join(str(d.get(f, "")).replace("\n", " ") for f in fields) + " |")
    return "\n".join([header, sep, *body])


def _field_selector(
    title: str,
    fields: list[str],
    defaults: list[str],
    key_prefix: str,
    presets: dict[str, list[str]] | None = None,
) -> list[str]:
    st.markdown(f"**{title}**")
    state_key = f"{key_prefix}_selected"
    if state_key not in st.session_state:
        st.session_state[state_key] = list(defaults)
    selected_set = set(st.session_state[state_key])

    # Ensure every checkbox key is initialized from selected_set once.
    for f in fields:
        cb_key = f"{key_prefix}_{f}"
        if cb_key not in st.session_state:
            st.session_state[cb_key] = f in selected_set

    c1, c2 = st.columns([1, 1])
    if c1.button("Select all", key=f"{key_prefix}_all"):
        for f in fields:
            st.session_state[f"{key_prefix}_{f}"] = True
        st.session_state[state_key] = list(fields)
        st.rerun()
    if c2.button("Clear all", key=f"{key_prefix}_clear"):
        for f in fields:
            st.session_state[f"{key_prefix}_{f}"] = False
        st.session_state[state_key] = []
        st.rerun()

    if presets:
        preset_names = list(presets.keys())
        p1, p2 = st.columns([2, 1])
        chosen = p1.selectbox("Field preset", options=preset_names, key=f"{key_prefix}_preset")
        if p2.button("Apply preset", key=f"{key_prefix}_apply_preset"):
            preset_fields = [f for f in presets.get(chosen, []) if f in fields]
            for f in fields:
                st.session_state[f"{key_prefix}_{f}"] = f in set(preset_fields)
            st.session_state[state_key] = preset_fields
            st.rerun()

    picked: list[str] = []
    with st.expander("Choose fields (single list)", expanded=False):
        for f in fields:
            if st.checkbox(f, key=f"{key_prefix}_{f}"):
                picked.append(f)
    st.session_state[state_key] = picked
    return picked


_THREAD_FETCH_LABELS = {
    "www_then_old": "Try www.reddit, then old.reddit on errors (recommended for batches)",
    "www": "www.reddit only",
    "old": "old.reddit only (same .json API, different host)",
}

# Preserve Streamlit widget keys so existing sessions keep slider values after upgrade.
_POST_VIABILITY_FETCH_KEY_OVERRIDES = {
    "thread_mode": "health_thread_mode",
    "sub_host": "health_sub_host",
    "pause_urls": "health_pause_urls",
    "pause_before_sub": "health_pause_before_sub",
    "fetch_retries": "health_retries",
    "sub_endpoint_pause": "health_sub_pause",
}


def _reddit_fetch_rate_expander(
    prefix: str,
    *,
    key_overrides: dict[str, str] | None = None,
    caption: str | None = None,
    thread_json: bool = False,
    subreddit_meta_host: bool = False,
    pause_between_items: bool = False,
    pause_items_label: str = "Pause between posts (seconds)",
    pause_items_range: tuple[float, float, float, float] = (0.0, 5.0, 0.75, 0.25),
    pause_before_subreddit: bool = False,
    pause_before_sub_range: tuple[float, float, float, float] = (0.0, 3.0, 0.35, 0.05),
    fetch_retries: bool = True,
    fetch_retries_range: tuple[int, int, int] = (0, 6, 3),
    sub_endpoint_pause: bool = False,
    sub_endpoint_range: tuple[float, float, float, float] = (0.0, 1.0, 0.15, 0.05),
) -> dict[str, object]:
    """Shared Streamlit controls for thread JSON host, subreddit API host, retries, and pauses."""
    out: dict[str, object] = {}
    ko = key_overrides or {}

    def k(logical: str) -> str:
        return ko.get(logical, f"{prefix}_{logical}")

    with st.expander("Fetch & rate limits", expanded=False):
        if caption:
            st.caption(caption)
        if thread_json:
            out["thread_fetch_mode"] = st.radio(
                "Thread JSON fetch",
                options=["www_then_old", "www", "old"],
                format_func=lambda x: _THREAD_FETCH_LABELS[x],
                horizontal=True,
                key=k("thread_mode"),
            )
        if subreddit_meta_host:
            out["subreddit_host"] = st.selectbox(
                "Subreddit metadata host (rules/mods/widgets)",
                options=["www", "old"],
                index=0,
                key=k("sub_host"),
                help="Separate from thread host. Old can help if www subreddit endpoints are flaky.",
            )

        row_slots: list[str] = []
        if pause_between_items:
            row_slots.append("pause_items")
        if pause_before_subreddit:
            row_slots.append("pause_before_sub")
        if fetch_retries:
            row_slots.append("fetch_retries")
        if row_slots:
            cols = st.columns(len(row_slots))
            for col, slot in zip(cols, row_slots, strict=True):
                with col:
                    if slot == "pause_items":
                        mn, mx, dv, stp = pause_items_range
                        out["pause_between_items"] = st.slider(
                            pause_items_label,
                            min_value=mn,
                            max_value=mx,
                            value=dv,
                            step=stp,
                            key=k("pause_urls"),
                        )
                    elif slot == "pause_before_sub":
                        mn, mx, dv, stp = pause_before_sub_range
                        out["pause_before_subreddit_sec"] = st.slider(
                            "Pause before subreddit fetches (seconds)",
                            min_value=mn,
                            max_value=mx,
                            value=dv,
                            step=stp,
                            key=k("pause_before_sub"),
                        )
                    else:
                        mn_r, mx_r, dv_r = fetch_retries_range
                        out["fetch_retries"] = st.slider(
                            "Fetch retries (429/backoff)",
                            min_value=mn_r,
                            max_value=mx_r,
                            value=dv_r,
                            step=1,
                            key=k("fetch_retries"),
                            help="Applies to Reddit JSON requests (thread, search, subreddit endpoints, or user API as noted on each tab).",
                        )

        if sub_endpoint_pause:
            mn, mx, dv, stp = sub_endpoint_range
            out["pause_between_subreddit_requests_sec"] = st.slider(
                "Pause between subreddit endpoints (seconds)",
                min_value=mn,
                max_value=mx,
                value=dv,
                step=stp,
                key=k("sub_endpoint_pause"),
                help="Spacing between about / rules / mods / widgets (and wiki rules fallback) per subreddit.",
            )
    return out


def _render_export(rows: list, fields: list[str], base_name: str, key: str) -> None:
    if not rows or not fields:
        return
    fmt = st.selectbox(
        "Export format",
        ["TSV (Sheets)", "CSV", "Markdown", "JSON"],
        key=f"exp_fmt_{key}",
    )
    if fmt == "TSV (Sheets)":
        data = dataclass_rows_to_tsv(rows, fields).encode("utf-8")
        fname = f"{base_name}.tsv"
        mime = "text/tab-separated-values"
    elif fmt == "CSV":
        header = ",".join(fields)
        lines = [header]
        for r in rows:
            d = asdict(r)
            vals = []
            for f in fields:
                s = str(d.get(f, "")).replace('"', '""').replace("\n", " ")
                vals.append(f'"{s}"')
            lines.append(",".join(vals))
        data = "\n".join(lines).encode("utf-8")
        fname = f"{base_name}.csv"
        mime = "text/csv"
    elif fmt == "Markdown":
        header = "| " + " | ".join(fields) + " |"
        sep = "| " + " | ".join(["---"] * len(fields)) + " |"
        body = []
        for r in rows:
            d = asdict(r)
            body.append("| " + " | ".join(str(d.get(f, "")).replace("\n", " ") for f in fields) + " |")
        data = "\n".join([header, sep, *body]).encode("utf-8")
        fname = f"{base_name}.md"
        mime = "text/markdown"
    else:
        data = json.dumps([{f: asdict(r).get(f, "") for f in fields} for r in rows], indent=2).encode("utf-8")
        fname = f"{base_name}.json"
        mime = "application/json"
    st.download_button("Export file", data=data, file_name=fname, mime=mime, key=f"exp_btn_{key}")


def _table_copy_and_export(rows: list, fields: list[str], base_name: str, key: str, title: str) -> None:
    if not rows or not fields:
        return
    st.markdown(f"**{title}**")
    c1, c2, c3 = st.columns(3)
    tsv_text = dataclass_rows_to_tsv(rows, fields)
    md_text = _to_markdown_table(rows, fields)
    json_text = json.dumps([{f: asdict(r).get(f, "") for f in fields} for r in rows], indent=2)
    with c1:
        _copy_button("Copy TSV (Google Sheets / Excel)", tsv_text, f"{key}_tsv")
    with c2:
        _copy_button("Copy Markdown table", md_text, f"{key}_md")
    with c3:
        _copy_button("Copy JSON", json_text, f"{key}_json")
    _render_export(rows, fields, base_name, key)


st.set_page_config(page_title="Reddit Research Workspace", layout="wide", initial_sidebar_state="expanded")
st.title("Reddit Research Workspace")

PRESET_FILE = Path(__file__).with_name("daily_scan_presets.json")
HISTORY_FILE = Path(__file__).with_name("run_history.jsonl")


def _load_presets() -> dict:
    if not PRESET_FILE.exists():
        return {}
    try:
        return json.loads(PRESET_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_presets(presets: dict) -> None:
    PRESET_FILE.write_text(json.dumps(presets, indent=2), encoding="utf-8")


def _append_run_history(run_type: str, fields: list[str], rows: list) -> None:
    if not rows:
        return
    payload = {
        "run_id": uuid.uuid4().hex[:12],
        "run_type": run_type,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "fields": fields,
        "rows": [{f: asdict(r).get(f, "") for f in fields} for r in rows],
        "row_count": len(rows),
    }
    with HISTORY_FILE.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, ensure_ascii=False) + "\n")


def _load_run_history() -> list[dict]:
    if not HISTORY_FILE.exists():
        return []
    out: list[dict] = []
    for line in HISTORY_FILE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            out.append(json.loads(line))
        except Exception:
            continue
    return out


def _history_key_fields(run_type: str) -> list[str]:
    mapping = {
        "live_checker": ["input_url", "permalink", "status"],
        "daily_scan": ["post_id", "post_url", "title"],
        "daily_scan_comments": ["post_id", "comment_id"],
        "post_viability": ["input_url"],
        "subreddit_snapshot": ["normalized_name", "subreddit_url"],
        "subreddits": ["normalized_name", "subreddit_url"],
        "reddit_users": ["username", "profile_url"],
    }
    return mapping.get(run_type, [])


def _row_sig(row: dict, keys: list[str]) -> str:
    if not keys:
        keys = list(row.keys())
    return " | ".join(str(row.get(k, "")) for k in keys)


tabs = st.tabs([
    "Live Checker",
    "Daily Scan",
    "Post Viability",
    "Subreddit Extractor",
    "User Explorer",
    "History & Compare",
])

# Live checker first
with tabs[0]:
    st.subheader("Comment/Post Live Checker")
    st.caption("One-line use: Validate URL status and instantly copy filtered rows to Google Sheets.")
    forced_type = st.selectbox("URL type", ["auto", "post", "comment"], index=0, key="live_type", help="auto infers type from URL path; override when needed.")
    urls_text = st.text_area("URLs (one per line)", height=160, key="live_urls", help="Paste Reddit post/comment URLs to check live/removed/deleted state.")
    live_fetch = _reddit_fetch_rate_expander(
        "live_fetch",
        caption="Each URL loads thread JSON once. Pauses and retries reduce 429s on long lists.",
        thread_json=True,
        pause_between_items=True,
        pause_items_label="Pause between URLs (seconds)",
        pause_items_range=(0.0, 5.0, 0.25, 0.25),
        fetch_retries=True,
    )
    live_fields = _field_selector(
        "Required field selection",
        list(RedditLinkStatus.__dataclass_fields__.keys()),
        ["input_url", "link_kind", "status", "subreddit", "author", "created_utc", "title", "score", "num_comments", "permalink", "error"],
        "live_fields",
        presets={
            "Minimal (2 fields)": ["status", "permalink"],
            "Quick (5 fields)": ["status", "permalink", "author", "subreddit", "created_utc"],
            "Standard (10 fields)": [
                "input_url",
                "link_kind",
                "status",
                "subreddit",
                "author",
                "created_utc",
                "title",
                "score",
                "num_comments",
                "permalink",
            ],
            "Debug (include error)": [
                "input_url",
                "status",
                "permalink",
                "error",
            ],
            "Full (all fields)": list(RedditLinkStatus.__dataclass_fields__.keys()),
        },
    )
    if st.button("Run live check", type="primary", key="live_run", help="Runs status checks for all entered URLs."):
        urls = [u.strip() for u in urls_text.splitlines() if u.strip()]
        if not urls:
            st.warning("Add at least one Reddit URL.")
        else:
            with st.spinner("Checking link statuses..."):
                rows = []
                pause_u = float(live_fetch.get("pause_between_items") or 0)
                retries = int(live_fetch.get("fetch_retries") or 0)
                tmode = str(live_fetch.get("thread_fetch_mode") or "www_then_old")
                for i, u in enumerate(urls):
                    if i > 0 and pause_u > 0:
                        time.sleep(pause_u)
                    rows.append(
                        check_one(
                            u,
                            forced_kind=forced_type,
                            fetch_retries=retries,
                            thread_fetch_mode=tmode,
                        )
                    )
                for r in rows:
                    # In live-check workflows, Reddit "not_found" is treated as removed content.
                    if r.status == "not_found":
                        r.status = "removed"
            st.session_state["live_rows"] = rows
            _append_run_history("live_checker", live_fields, rows)

    rows = st.session_state.get("live_rows", [])
    if rows:
        total = len(rows)
        live = sum(1 for r in rows if r.status == "live")
        errors = sum(1 for r in rows if r.error)
        m1, m2, m3 = st.columns(3)
        m1.metric("Rows", total)
        m2.metric("Live", live)
        m3.metric("With errors", errors)
        st.dataframe(_rows_to_table(rows, live_fields), use_container_width=True)
        _table_copy_and_export(rows, live_fields, "live_checker", "live", "Copy / Export current table")

with tabs[1]:
    st.subheader("Daily Scan")
    st.caption("One-line use: Run daily query scans, score intent, detect reposts, filter, then copy/export.")
    presets = _load_presets()

    c1, c2 = st.columns([2, 1])
    with c1:
        preset_name = st.text_input("Preset name", key="ds_preset_name", help="Name to save current search settings.")
        query = st.text_input("Keyword / question", placeholder="best project management tool for saas founders", key="ds_query", help="Main Reddit search phrase.")
        subreddits_csv = st.text_input("Subreddits (comma separated)", placeholder="saas,startups,Entrepreneur", key="ds_subs", help="Optional: restrict search to these subreddits.")
        qsort = st.selectbox("Sort", ["new", "relevance", "top", "comments"], index=0, key="ds_sort", help="Reddit search sort mode.")
        qtime = st.selectbox("Time", ["day", "week", "month", "year", "all"], index=1, key="ds_time", help="Reddit search time window.")
        qlimit = st.slider("Max results", 5, 100, 30, key="ds_limit", help="Maximum results fetched from Reddit search JSON.")
    with c2:
        st.caption("Presets")
        preset_keys = ["(none)"] + sorted(presets.keys())
        selected_preset = st.selectbox("Load preset", preset_keys, key="ds_load")
        if st.button("Load", use_container_width=True, key="ds_btn_load") and selected_preset != "(none)":
            p = presets[selected_preset]
            st.session_state["ds_query"] = p.get("query", "")
            st.session_state["ds_subs"] = p.get("subs", "")
            st.session_state["ds_sort"] = p.get("sort", "new")
            st.session_state["ds_time"] = p.get("time", "week")
            st.session_state["ds_limit"] = int(p.get("limit", 30))
            st.rerun()
        if st.button("Save / Update preset", use_container_width=True, key="ds_btn_save") and preset_name.strip():
            presets[preset_name.strip()] = {"query": query, "subs": subreddits_csv, "sort": qsort, "time": qtime, "limit": qlimit}
            _save_presets(presets)
            st.success(f"Preset saved: {preset_name.strip()}")
        if st.button("Delete preset", use_container_width=True, key="ds_btn_delete") and selected_preset in presets:
            presets.pop(selected_preset, None)
            _save_presets(presets)
            st.success(f"Preset deleted: {selected_preset}")

    r1, r2, r3 = st.columns([1, 1, 2])
    min_lead = r1.slider("Min lead score", 0, 100, 30, key="ds_min_lead", help="Keep only posts with this lead-intent score or higher.")
    only_dups = r2.checkbox("Only duplicate/repost groups", value=False, key="ds_dups", help="Show only likely repost clusters.")
    extra_filter = r3.text_input("Title contains (optional)", key="ds_filter", help="Extra client-specific keyword filter for titles.")

    ds_fetch = _reddit_fetch_rate_expander(
        "ds_fetch",
        caption="Search uses one Reddit JSON request per run (retries apply there). Thread JSON mode applies when you **Extract improved comments**.",
        thread_json=True,
        fetch_retries=True,
    )

    scan_fields = _field_selector(
        "Required field selection",
        list(SearchResultRecord.__dataclass_fields__.keys()),
        ["query", "subreddit", "title", "author", "created_utc", "score", "num_comments", "upvote_ratio", "lead_score", "lead_reasons", "duplicate_group", "post_url"],
        "scan_fields",
        presets={
            "Minimal (4 fields)": ["subreddit", "title", "lead_score", "post_url"],
            "Outreach (6 fields)": ["subreddit", "title", "author", "lead_score", "lead_reasons", "post_url"],
            "Research (10 fields)": ["query", "subreddit", "title", "author", "created_utc", "score", "num_comments", "upvote_ratio", "lead_score", "post_url"],
            "Full": list(SearchResultRecord.__dataclass_fields__.keys()),
        },
    )

    if st.button("Run Daily Scan", type="primary", key="ds_run", help="Run search + lead scoring + duplicate grouping."):
        subs = [s.strip() for s in subreddits_csv.split(",") if s.strip()]
        if not query.strip():
            st.warning("Enter a keyword/question before running Daily Scan.")
        else:
            with st.spinner("Running daily scan and lead scoring..."):
                rows = search_posts_no_api(
                    query,
                    subreddits=subs,
                    sort=qsort,
                    time_filter=qtime,
                    limit=qlimit,
                    max_retries=int(ds_fetch.get("fetch_retries") or 0),
                )
                rows = [r for r in rows if r.lead_score >= min_lead]
                if only_dups:
                    rows = [r for r in rows if r.duplicate_group]
                if extra_filter.strip():
                    rows = [r for r in rows if extra_filter.lower() in (r.title or "").lower()]
            st.session_state["scan_rows"] = rows
            _append_run_history("daily_scan", scan_fields, rows)

    scan_rows: list[SearchResultRecord] = st.session_state.get("scan_rows", [])
    if scan_rows:
        total = len(scan_rows)
        dups = sum(1 for r in scan_rows if r.duplicate_group)
        avg_lead = round(sum(r.lead_score for r in scan_rows) / max(total, 1), 1)
        m1, m2, m3 = st.columns(3)
        m1.metric("Rows", total)
        m2.metric("Duplicate rows", dups)
        m3.metric("Avg lead score", avg_lead)
        st.dataframe(_rows_to_table(scan_rows, scan_fields), use_container_width=True)
        _table_copy_and_export(scan_rows, scan_fields, "daily_scan", "scan", "Copy / Export scan table")

        st.subheader("Comment extraction improvements")
        pick = st.selectbox("Pick a post", options=range(len(scan_rows)), format_func=lambda i: f"{scan_rows[i].subreddit}: {scan_rows[i].title[:90]}", key="scan_pick", help="Select a scanned post to extract comments from.")
        top_n = st.slider("Top comments by score", 5, 100, 25, key="scan_top", help="Limit number of comments returned after filters.")
        only_live = st.checkbox("Only live comments", value=True, key="scan_live", help="Exclude removed/deleted comments.")
        min_comment_score = st.number_input("Min comment score", value=0, key="scan_minscore", help="Keep comments with score >= this value.")
        exclude_bot = st.checkbox("Exclude bot-like authors", value=True, key="scan_exbot", help="Filters comments from bot-looking usernames.")
        exclude_mod = st.checkbox("Exclude mod-like authors", value=False, key="scan_exmod", help="Filters comments from mod-looking usernames.")

        comment_fields = _field_selector(
            "Comment fields",
            list(CommentRecord.__dataclass_fields__.keys()),
            ["post_id", "depth", "comment_id", "author", "score", "status", "created_utc", "body", "permalink"],
            "scan_comment_fields",
            presets={
                "Minimal (4 fields)": ["author", "body", "score", "permalink"],
                "Moderation (6 fields)": ["author", "status", "score", "distinguished", "created_utc", "permalink"],
                "Thread map": ["post_id", "depth", "comment_id", "parent_fullname", "author", "status", "permalink"],
                "Full": list(CommentRecord.__dataclass_fields__.keys()),
            },
        )

        if st.button("Extract improved comments", key="scan_comments_run", help="Fetch and filter comments for selected post."):
            with st.spinner("Extracting and filtering comments..."):
                ex = extract_thread(
                    scan_rows[pick].post_url,
                    include_post=True,
                    include_comments=True,
                    fetch_retries=int(ds_fetch.get("fetch_retries") or 0),
                    thread_fetch_mode=str(ds_fetch.get("thread_fetch_mode") or "www_then_old"),
                )
                comments = sorted(ex.comments, key=lambda c: c.score, reverse=True)
                if only_live:
                    comments = [c for c in comments if c.status == "live"]
                comments = [c for c in comments if c.score >= int(min_comment_score)]
                if exclude_bot:
                    comments = [c for c in comments if "bot" not in (c.author or "").lower()]
                if exclude_mod:
                    comments = [c for c in comments if "mod" not in (c.author or "").lower()]
                comments = comments[:top_n]
            st.session_state["scan_comments"] = comments
            _append_run_history("daily_scan_comments", comment_fields, comments)

        comments = st.session_state.get("scan_comments", [])
        if comments:
            st.dataframe(_rows_to_table(comments, comment_fields), use_container_width=True)
            _table_copy_and_export(comments, comment_fields, "daily_scan_comments", "scan_comments", "Copy / Export filtered comments")

with tabs[2]:
    st.subheader("Post Viability Validator")
    st.caption(
        "Checks post lock state, thread signals, and optional subreddit rules. "
        "Skip subreddit snapshot when you only need thread viability."
    )
    forced_type = st.selectbox("URL type", ["auto", "post", "comment"], index=0, key="health_type", help="auto infers type from URL path.")
    post_urls = st.text_area("Post URLs (one per line)", height=160, key="health_urls", help="Paste post URLs to check viability conditions.")

    fetch_opts = _reddit_fetch_rate_expander(
        "health",
        key_overrides=_POST_VIABILITY_FETCH_KEY_OVERRIDES,
        caption=(
            "Batch runs hit many Reddit endpoints (1 thread JSON + up to 4 subreddit calls per URL). "
            "Use pauses and www→old thread fallback to reduce fetch_error / 429s."
        ),
        thread_json=True,
        subreddit_meta_host=True,
        pause_between_items=True,
        pause_before_subreddit=True,
        fetch_retries=True,
        sub_endpoint_pause=True,
    )
    health_fetch_mode = str(fetch_opts["thread_fetch_mode"])
    health_sub_host = str(fetch_opts["subreddit_host"])
    health_pause_urls = float(fetch_opts["pause_between_items"])
    health_pause_before_sub = float(fetch_opts["pause_before_subreddit_sec"])
    health_retries = int(fetch_opts["fetch_retries"])
    health_sub_pause = float(fetch_opts["pause_between_subreddit_requests_sec"])

    health_skip_sub = st.checkbox(
        "Skip subreddit rules/mods/widgets (saves 4 API calls per post)",
        value=False,
        key="health_skip_sub",
    )

    health_fields = _field_selector(
        "Health fields",
        list(PostHealthRecord.__dataclass_fields__.keys()),
        [
            "input_url",
            "status",
            "subreddit",
            "title",
            "post_created_utc",
            "locked",
            "num_comments_reported",
            "comments_in_payload",
            "last_comment_utc",
            "last_comment_live",
            "mod_comment_count",
            "bot_comment_count",
            "removed_deleted_comment_count",
            "can_post_new_comment",
            "rationale",
        ],
        "health_fields",
        presets={
            "Essential viability": [
                "input_url",
                "status",
                "title",
                "post_created_utc",
                "locked",
                "num_comments_reported",
                "last_comment_utc",
                "can_post_new_comment",
                "rationale",
            ],
            "Required post data": [
                "title",
                "post_created_utc",
                "post_body_excerpt",
                "post_ups",
                "post_downs",
                "post_upvote_ratio",
                "num_comments_reported",
                "comments_in_payload",
            ],
            "Post + comments + subreddit context": [
                "input_url",
                "status",
                "subreddit",
                "title",
                "post_created_utc",
                "post_body_excerpt",
                "post_ups",
                "post_upvote_ratio",
                "num_comments_reported",
                "top_comments_excerpt",
                "subreddit_description_excerpt",
                "subreddit_rules_excerpt",
                "can_post_new_comment",
                "rationale",
            ],
            "Full": list(PostHealthRecord.__dataclass_fields__.keys()),
        },
    )

    if st.button("Validate posts", type="primary", key="health_run", help="Evaluate posting viability and thread health signals."):
        urls = [u.strip() for u in post_urls.splitlines() if u.strip()]
        if not urls:
            st.warning("Add at least one post URL.")
        else:
            with st.spinner("Validating post viability..."):
                health_rows: list[PostHealthRecord] = []
                sub_rows: list[SubredditRecord] = []
                for i, u in enumerate(urls):
                    if i > 0 and health_pause_urls > 0:
                        time.sleep(float(health_pause_urls))
                    h, s = summarize_post_health(
                        u,
                        forced_kind=forced_type,
                        fetch_retries=int(health_retries),
                        thread_fetch_mode=str(health_fetch_mode),
                        pause_before_subreddit_sec=float(health_pause_before_sub),
                        subreddit_fetch_retries=int(health_retries),
                        pause_between_subreddit_requests_sec=float(health_sub_pause),
                        subreddit_host=str(health_sub_host),
                        skip_subreddit_meta=bool(health_skip_sub),
                    )
                    health_rows.append(h)
                    if s:
                        sub_rows.append(s)
            st.session_state["health_rows"] = health_rows
            st.session_state["health_sub_rows"] = sub_rows
            _append_run_history("post_viability", health_fields, health_rows)

    health_rows: list[PostHealthRecord] = st.session_state.get("health_rows", [])
    if health_rows:
        total = len(health_rows)
        likely_yes = sum(1 for r in health_rows if r.can_post_new_comment.lower().startswith("likely"))
        blocked = sum(1 for r in health_rows if r.can_post_new_comment.lower() == "no")
        fetch_err = sum(1 for r in health_rows if r.status == "fetch_error")
        restricted_n = sum(1 for r in health_rows if r.status == "restricted")
        m1, m2, m3, m4, m5 = st.columns(5)
        m1.metric("Rows", total)
        m2.metric("Likely commentable", likely_yes)
        m3.metric("Blocked (no comment)", blocked)
        m4.metric("Fetch / API errors", fetch_err)
        m5.metric("Restricted (403)", restricted_n)
        st.dataframe(_rows_to_table(health_rows, health_fields), use_container_width=True)
        _table_copy_and_export(health_rows, health_fields, "post_viability", "health", "Copy / Export health table")

    sub_rows: list[SubredditRecord] = st.session_state.get("health_sub_rows", [])
    if sub_rows:
        st.subheader("Subreddit rules/moderators snapshot")
        sub_fields = _field_selector(
            "Subreddit snapshot fields",
            list(SubredditRecord.__dataclass_fields__.keys()),
            ["normalized_name", "subreddit_url", "rules_markdown", "moderators", "widgets", "error"],
            "health_sub_fields",
            presets={
                "Rules only": ["normalized_name", "subreddit_url", "rules_markdown", "error"],
                "Rules + mods": ["normalized_name", "subreddit_url", "rules_markdown", "moderators", "error"],
                "Full": list(SubredditRecord.__dataclass_fields__.keys()),
            },
        )
        st.dataframe(_rows_to_table(sub_rows, sub_fields), use_container_width=True)
        _table_copy_and_export(sub_rows, sub_fields, "subreddit_snapshot", "health_sub", "Copy / Export subreddit snapshot")

with tabs[3]:
    st.subheader("Subreddit Extractor")
    st.caption("One-line use: extract subreddit profile, rules, moderators, widgets/apps and copy/export selected fields.")
    subs_text = st.text_area("Subreddit names (one per line)", height=160, placeholder="saas\nstartups\nEntrepreneur", key="subs_text", help="Accepts `saas`, `r/saas`, or subreddit URLs.")

    sub_fetch = _reddit_fetch_rate_expander(
        "sub_fetch",
        caption="Each subreddit triggers about, rules, mods, widgets, and sometimes wiki rules. Tune spacing for large batches.",
        subreddit_meta_host=True,
        pause_between_items=True,
        pause_items_label="Pause between subreddits (seconds)",
        pause_items_range=(0.0, 5.0, 0.5, 0.25),
        fetch_retries=True,
        sub_endpoint_pause=True,
    )

    sub_fields = _field_selector(
        "Required field selection",
        list(SubredditRecord.__dataclass_fields__.keys()),
        ["normalized_name", "subreddit_url", "title", "public_description", "subscribers", "active_user_count", "rules_markdown", "moderators", "widgets", "error"],
        "subs_fields",
        presets={
            "Minimal (5 fields)": ["normalized_name", "subreddit_url", "title", "subscribers", "active_user_count"],
            "Rules focus": ["normalized_name", "subreddit_url", "rules_markdown", "moderators", "error"],
            "Growth view": ["normalized_name", "title", "public_description", "subscribers", "active_user_count", "over18"],
            "Full": list(SubredditRecord.__dataclass_fields__.keys()),
        },
    )

    if st.button("Extract subreddits", type="primary", key="subs_run", help="Fetch subreddit metadata/rules/mods/widgets."):
        names = [s.strip() for s in subs_text.splitlines() if s.strip()]
        if not names:
            st.warning("Add at least one subreddit name.")
        else:
            with st.spinner("Extracting subreddit data..."):
                rows = extract_subreddits(
                    names,
                    fetch_retries=int(sub_fetch.get("fetch_retries") or 0),
                    pause_between_requests_sec=float(sub_fetch.get("pause_between_subreddit_requests_sec") or 0),
                    pause_between_subreddits_sec=float(sub_fetch.get("pause_between_items") or 0),
                    subreddit_host=str(sub_fetch.get("subreddit_host") or "www"),
                )
            st.session_state["subs_rows"] = rows
            _append_run_history("subreddits", sub_fields, rows)

    rows: list[SubredditRecord] = st.session_state.get("subs_rows", [])
    if rows:
        total = len(rows)
        with_rules = sum(1 for r in rows if (r.rules_markdown or "").strip())
        with_errors = sum(1 for r in rows if (r.error or "").strip())
        m1, m2, m3 = st.columns(3)
        m1.metric("Rows", total)
        m2.metric("Rules fetched", with_rules)
        m3.metric("Rows with endpoint errors", with_errors)
        st.dataframe(_rows_to_table(rows, sub_fields), use_container_width=True)
        _table_copy_and_export(rows, sub_fields, "subreddits", "subs", "Copy / Export subreddit table")

with tabs[4]:
    st.subheader("User Explorer")
    st.caption(
        "Enter usernames to pull profile status, account age, karma, and recent activity. "
        "For large batches, prefer **Old Reddit HTML** (one page per user) or increase pauses; raw JSON does three API calls per user and hits rate limits quickly. "
        "In Cursor, you can also use the **Reddit MCP** tool `user_analysis` for one-off research (separate from this app)."
    )
    users_source = st.radio(
        "Fetch method",
        options=["json", "old_reddit"],
        format_func=lambda x: (
            "Official .json endpoints (3 requests per user, richest fields)"
            if x == "json"
            else "Old Reddit HTML (1 request per user, overview + sidebar karma)"
        ),
        horizontal=True,
        key="users_source",
        help="HTML mode scrapes old.reddit.com server-rendered pages; JSON uses public API endpoints.",
    )
    users_text = st.text_area(
        "Reddit usernames (one per line)",
        height=160,
        placeholder="spez\nu/example_user\nanother_user",
        key="users_text",
        help="Accepts username, u/username, or profile URL segment.",
    )
    sample_limit = st.slider(
        "Recent activity sample size",
        min_value=1,
        max_value=50,
        value=10,
        key="users_limit",
        help="How many recent posts/comments to sample from each profile.",
    )
    users_rate_col1, users_rate_col2 = st.columns(2)
    with users_rate_col1:
        users_profile_delay = st.slider(
            "Pause between profiles (seconds)",
            min_value=0.0,
            max_value=5.0,
            value=1.25,
            step=0.25,
            key="users_profile_delay",
            help="Spacing between users lowers chance of Reddit throttling. Use ~1.25s+ for medium batches; increase if you still see errors.",
        )
    with users_rate_col2:
        users_req_delay = st.slider(
            "Pause between requests, same profile (seconds)",
            min_value=0.0,
            max_value=1.0,
            value=0.2,
            step=0.05,
            key="users_req_delay",
            help="JSON mode only: gap between about / submitted / comments. Ignored for Old Reddit HTML.",
            disabled=(users_source == "old_reddit"),
        )
    users_fetch_retries = st.slider(
        "Fetch retries (429/backoff)",
        min_value=0,
        max_value=6,
        value=3,
        step=1,
        key="users_fetch_retries",
        help="Retries on each Reddit request (HTML mode: one page; JSON mode: each of the three endpoints).",
    )
    user_fields = _field_selector(
        "Required field selection",
        list(RedditUserRecord.__dataclass_fields__.keys()),
        [
            "input_username",
            "username",
            "profile_url",
            "status",
            "account_created_utc",
            "total_karma",
            "link_karma",
            "comment_karma",
            "recent_posts_count",
            "recent_comments_count",
            "latest_activity_utc",
            "recent_post_titles",
            "recent_comment_excerpts",
            "error",
        ],
        "users_fields",
        presets={
            "Minimal (5 fields)": ["username", "status", "account_created_utc", "total_karma", "profile_url"],
            "Karma + activity": ["username", "status", "total_karma", "link_karma", "comment_karma", "recent_posts_count", "recent_comments_count", "latest_activity_utc"],
            "Outreach profile": ["username", "status", "account_created_utc", "total_karma", "recent_post_titles", "recent_comment_excerpts", "profile_url"],
            "Full": list(RedditUserRecord.__dataclass_fields__.keys()),
        },
    )
    if st.button("Explore users", type="primary", key="users_run", help="Fetch and summarize profile data for each username."):
        names = [x.strip() for x in users_text.splitlines() if x.strip()]
        if not names:
            st.warning("Add at least one username.")
        else:
            with st.spinner("Extracting user profiles and recent activity..."):
                rows = extract_reddit_users(
                    names,
                    sample_limit=int(sample_limit),
                    source=str(users_source),
                    delay_between_profiles_sec=float(users_profile_delay),
                    pause_between_requests_sec=float(users_req_delay),
                    fetch_retries=int(users_fetch_retries),
                )
            st.session_state["users_rows"] = rows
            _append_run_history("reddit_users", user_fields, rows)

    rows: list[RedditUserRecord] = st.session_state.get("users_rows", [])
    if rows:
        total = len(rows)
        live = sum(1 for r in rows if r.status == "live")
        errored = sum(1 for r in rows if (r.error or "").strip())
        m1, m2, m3 = st.columns(3)
        m1.metric("Rows", total)
        m2.metric("Live profiles", live)
        m3.metric("Rows with endpoint errors", errored)
        st.dataframe(_rows_to_table(rows, user_fields), use_container_width=True)
        _table_copy_and_export(rows, user_fields, "reddit_users", "users", "Copy / Export users table")

with tabs[5]:
    st.subheader("Persistent Run History + Comparison")
    st.caption("Compares two saved runs of the same tool and shows added/removed/common rows.")

    history = _load_run_history()
    if not history:
        st.info("No saved runs yet. Execute any tool run first.")
    else:
        run_types = sorted({h.get("run_type", "") for h in history if h.get("run_type")})
        run_type = st.selectbox("Run type", options=run_types, key="hist_type")
        subset = [h for h in history if h.get("run_type") == run_type]
        subset_sorted = sorted(subset, key=lambda x: x.get("timestamp_utc", ""), reverse=True)
        labels = [
            f"{h.get('timestamp_utc','')} | {h.get('run_id','')} | rows={h.get('row_count',0)}"
            for h in subset_sorted
        ]
        if len(subset_sorted) < 2:
            st.warning("Need at least two runs of this type to compare.")
        else:
            a_idx = st.selectbox("Run A (newer)", options=range(len(subset_sorted)), format_func=lambda i: labels[i], key="hist_a")
            b_idx = st.selectbox("Run B (older)", options=range(len(subset_sorted)), index=min(1, len(subset_sorted)-1), format_func=lambda i: labels[i], key="hist_b")
            if a_idx == b_idx:
                st.warning("Choose two different runs.")
            else:
                run_a = subset_sorted[a_idx]
                run_b = subset_sorted[b_idx]
                rows_a = run_a.get("rows", [])
                rows_b = run_b.get("rows", [])
                keys = _history_key_fields(run_type)

                map_a = {_row_sig(r, keys): r for r in rows_a}
                map_b = {_row_sig(r, keys): r for r in rows_b}

                added_keys = sorted(set(map_a.keys()) - set(map_b.keys()))
                removed_keys = sorted(set(map_b.keys()) - set(map_a.keys()))
                common_keys = sorted(set(map_a.keys()) & set(map_b.keys()))

                m1, m2, m3 = st.columns(3)
                m1.metric("Added in A", len(added_keys))
                m2.metric("Removed in A", len(removed_keys))
                m3.metric("Common", len(common_keys))

                view = st.radio("View", ["Added", "Removed", "Common"], horizontal=True, key="hist_view")
                if view == "Added":
                    out_rows = [map_a[k] for k in added_keys]
                elif view == "Removed":
                    out_rows = [map_b[k] for k in removed_keys]
                else:
                    out_rows = [map_a[k] for k in common_keys]

                out_fields = run_a.get("fields", []) or (list(out_rows[0].keys()) if out_rows else [])
                if out_rows:
                    st.dataframe(out_rows, use_container_width=True)

                    # lightweight wrapper to reuse copy/export helpers expecting dataclasses
                    class _Wrap:
                        def __init__(self, d: dict) -> None:
                            self._d = d

                    wrapped = [_Wrap(r) for r in out_rows]
                    # monkey-patch asdict usage via __dict__ compatible object
                    def _asdict_like(x):
                        return x._d

                    # inline copy/export without touching shared helper contracts
                    tsv = "\t".join(out_fields) + "\n" + "\n".join(
                        "\t".join(str(r.get(f, "")).replace("\n", " ") for f in out_fields) for r in out_rows
                    )
                    md = "| " + " | ".join(out_fields) + " |\n" + "| " + " | ".join(["---"] * len(out_fields)) + " |\n" + "\n".join(
                        "| " + " | ".join(str(r.get(f, "")).replace("\n", " ") for f in out_fields) + " |" for r in out_rows
                    )
                    js = json.dumps(out_rows, indent=2)
                    c1, c2, c3 = st.columns(3)
                    with c1:
                        _copy_button("Copy TSV (Google Sheets / Excel)", tsv, "hist_tsv")
                    with c2:
                        _copy_button("Copy Markdown table", md, "hist_md")
                    with c3:
                        _copy_button("Copy JSON", js, "hist_json")

                    fmt = st.selectbox("Export format", ["TSV (Sheets)", "CSV", "Markdown", "JSON"], key="hist_fmt")
                    if fmt == "TSV (Sheets)":
                        data = tsv.encode("utf-8")
                        fname = f"history_compare_{view.lower()}.tsv"
                        mime = "text/tab-separated-values"
                    elif fmt == "CSV":
                        header = ",".join(out_fields)
                        lines = [header]
                        for r in out_rows:
                            vals = []
                            for f in out_fields:
                                s = str(r.get(f, "")).replace('"', '""').replace("\n", " ")
                                vals.append(f'"{s}"')
                            lines.append(",".join(vals))
                        data = "\n".join(lines).encode("utf-8")
                        fname = f"history_compare_{view.lower()}.csv"
                        mime = "text/csv"
                    elif fmt == "Markdown":
                        data = md.encode("utf-8")
                        fname = f"history_compare_{view.lower()}.md"
                        mime = "text/markdown"
                    else:
                        data = js.encode("utf-8")
                        fname = f"history_compare_{view.lower()}.json"
                        mime = "application/json"
                    st.download_button("Export file", data=data, file_name=fname, mime=mime, key="hist_dl")
                else:
                    st.info(f"No rows for '{view}' in this run comparison.")
