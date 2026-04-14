from __future__ import annotations

import io
from dataclasses import asdict
from datetime import datetime
import csv

import streamlit as st

from reddit_status_checker import check_one, extract_reddit_users, extract_subreddits


APP_TITLE = "Reddit Status Checker Minimal"
APP_SUBTITLE = "Fast checks for post/comment live status, subreddit extraction, and Reddit user info."


def _inject_styles() -> None:
    st.markdown(
        """
        <style>
        :root {
          --bg1: #f4f9ff;
          --bg2: #eef6ff;
          --text: #0b2340;
          --accent: #2b7fff;
          --accent-2: #00a6ff;
          --ok: #0ca678;
          --warn: #ff7a00;
          --card: #ffffff;
          --border: #d8e8ff;
        }
        .stApp {
          background: linear-gradient(180deg, var(--bg1) 0%, var(--bg2) 100%);
          color: var(--text);
        }
        .hero {
          background: linear-gradient(135deg, #2b7fff 0%, #00b4ff 100%);
          border-radius: 18px;
          padding: 18px 20px;
          color: #ffffff;
          box-shadow: 0 10px 28px rgba(0, 92, 204, 0.25);
          margin-bottom: 8px;
        }
        .hero h1 {
          margin: 0 0 6px 0;
          font-size: 1.55rem;
          font-weight: 800;
          letter-spacing: 0.2px;
        }
        .hero p {
          margin: 0;
          opacity: 0.97;
          font-size: 0.97rem;
        }
        .metric-card {
          background: var(--card);
          border: 1px solid var(--border);
          border-radius: 14px;
          padding: 10px 12px;
          box-shadow: 0 4px 14px rgba(13, 74, 170, 0.08);
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _to_csv_bytes(rows: list[dict]) -> bytes:
    if not rows:
        return b""
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue().encode("utf-8")


def _status_bucket(status: str) -> str:
    return "live" if status == "live" else "unlive"


def _parse_lines(text: str) -> list[str]:
    return [line.strip() for line in text.splitlines() if line.strip()]


def _status_tab() -> None:
    st.subheader("Post & Comment Live/Unlive Checker")
    urls_text = st.text_area(
        "Reddit URLs (one per line)",
        height=180,
        placeholder="https://www.reddit.com/r/test/comments/abc123/example_post/\nhttps://www.reddit.com/r/test/comments/abc123/example_post/def456/",
    )
    forced_kind = st.selectbox("URL type", ["auto", "post", "comment"], index=0)
    retries = st.slider("Fetch retries", min_value=0, max_value=6, value=3, step=1)
    thread_mode = st.selectbox(
        "Thread host mode",
        options=["www_then_old", "www", "old"],
        index=0,
        help="www_then_old is usually most reliable in batches.",
    )

    if st.button("Check live status", type="primary"):
        urls = _parse_lines(urls_text)
        if not urls:
            st.warning("Enter at least one Reddit URL.")
            return

        with st.spinner("Checking URLs..."):
            results = [
                check_one(
                    url=u,
                    forced_kind=forced_kind,
                    fetch_retries=int(retries),
                    thread_fetch_mode=thread_mode,
                )
                for u in urls
            ]

        table = []
        for r in results:
            row = asdict(r)
            row["availability"] = _status_bucket(r.status)
            table.append(row)

        live_count = sum(1 for x in table if x["availability"] == "live")
        unlive_count = len(table) - live_count

        c1, c2 = st.columns(2)
        c1.markdown(f'<div class="metric-card"><b>Live</b><br><span style="font-size:1.3rem;color:#0ca678">{live_count}</span></div>', unsafe_allow_html=True)
        c2.markdown(f'<div class="metric-card"><b>Unlive</b><br><span style="font-size:1.3rem;color:#ff7a00">{unlive_count}</span></div>', unsafe_allow_html=True)

        cols = [
            "input_url",
            "link_kind",
            "availability",
            "status",
            "subreddit",
            "author",
            "created_utc",
            "permalink",
            "error",
        ]
        st.dataframe([{k: r.get(k, "") for k in cols} for r in table], use_container_width=True)
        st.download_button(
            "Download status CSV",
            data=_to_csv_bytes([{k: r.get(k, "") for k in cols} for r in table]),
            file_name=f"reddit_status_minimal_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
        )


def _subreddit_tab() -> None:
    st.subheader("Subreddit Extractor")
    names_text = st.text_area(
        "Subreddit names (one per line)",
        height=180,
        placeholder="saas\nstartups\nEntrepreneur",
    )
    host = st.selectbox("Subreddit metadata host", ["www", "old"], index=0)
    retries = st.slider("Fetch retries", min_value=0, max_value=6, value=3, step=1, key="subs_retries")
    pause_req = st.slider(
        "Pause between endpoints (seconds)",
        min_value=0.0,
        max_value=1.5,
        value=0.15,
        step=0.05,
    )
    pause_sub = st.slider(
        "Pause between subreddits (seconds)",
        min_value=0.0,
        max_value=4.0,
        value=0.0,
        step=0.1,
    )

    if st.button("Extract subreddits", type="primary"):
        names = _parse_lines(names_text)
        if not names:
            st.warning("Enter at least one subreddit name.")
            return
        with st.spinner("Extracting subreddit data..."):
            rows = extract_subreddits(
                names=names,
                fetch_retries=int(retries),
                pause_between_requests_sec=float(pause_req),
                pause_between_subreddits_sec=float(pause_sub),
                subreddit_host=host,
            )
        table = [asdict(x) for x in rows]
        cols = [
            "normalized_name",
            "subreddit_url",
            "title",
            "subscribers",
            "active_user_count",
            "rules_markdown",
            "moderators",
            "widgets",
            "error",
        ]
        st.dataframe([{k: r.get(k, "") for k in cols} for r in table], use_container_width=True)
        st.download_button(
            "Download subreddit CSV",
            data=_to_csv_bytes([{k: r.get(k, "") for k in cols} for r in table]),
            file_name=f"subreddit_extract_minimal_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
        )


def _users_tab() -> None:
    st.subheader("Reddit User Info Extractor")
    users_text = st.text_area(
        "Usernames (one per line)",
        height=180,
        placeholder="spez\nexample_user",
    )
    source = st.radio(
        "Data source",
        options=["json", "old_reddit"],
        horizontal=True,
        help="json = richer fields, old_reddit = fewer requests/user.",
    )
    sample_limit = st.slider("Recent activity sample per user", 1, 25, 10, 1)
    retries = st.slider("Fetch retries", 0, 6, 3, 1, key="users_retries")
    profile_delay = st.slider("Pause between users (seconds)", 0.0, 4.0, 1.25, 0.05)
    req_delay = st.slider(
        "Pause between user requests (seconds)",
        0.0,
        1.0,
        0.2,
        0.05,
        disabled=(source == "old_reddit"),
    )

    if st.button("Extract user info", type="primary"):
        usernames = _parse_lines(users_text)
        if not usernames:
            st.warning("Enter at least one username.")
            return
        with st.spinner("Extracting user info..."):
            rows = extract_reddit_users(
                usernames=usernames,
                sample_limit=int(sample_limit),
                source=source,
                fetch_retries=int(retries),
                delay_between_profiles_sec=float(profile_delay),
                pause_between_requests_sec=float(req_delay),
            )
        table = [asdict(x) for x in rows]
        cols = [
            "username",
            "status",
            "account_created_utc",
            "total_karma",
            "link_karma",
            "comment_karma",
            "recent_posts_count",
            "recent_comments_count",
            "latest_activity_utc",
            "profile_url",
            "error",
        ]
        st.dataframe([{k: r.get(k, "") for k in cols} for r in table], use_container_width=True)
        st.download_button(
            "Download users CSV",
            data=_to_csv_bytes([{k: r.get(k, "") for k in cols} for r in table]),
            file_name=f"reddit_users_minimal_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
        )


def main() -> None:
    st.set_page_config(page_title=APP_TITLE, page_icon="🟦", layout="wide")
    _inject_styles()
    st.markdown(
        f"""
        <div class="hero">
          <h1>{APP_TITLE}</h1>
          <p>{APP_SUBTITLE}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    tab_status, tab_subs, tab_users = st.tabs(
        ["Live/Unlive Status", "Subreddit Extractor", "User Info Extractor"]
    )
    with tab_status:
        _status_tab()
    with tab_subs:
        _subreddit_tab()
    with tab_users:
        _users_tab()


if __name__ == "__main__":
    main()
