# Reddit Link Status Checker (CLI + Streamlit + EXE)

A no-API-key **Reddit link checker** to verify whether Reddit post URLs and comment URLs are live, removed, deleted, or unavailable.

This project works as:
- a **Python CLI tool**
- a **Streamlit web app**
- a **Windows EXE** (folder build and single-file build)

If you searched for terms like **"Reddit link status checker"**, **"check deleted Reddit post"**, **"Reddit comment URL checker"**, **"Reddit post removed detector"**, or **"bulk Reddit URL status checker"**, this repository is built for that use case.

It checks whether each URL is:
- `live`
- `removed`
- `deleted`
- `not_found`
- `fetch_error`
- `invalid_url`

It also captures metadata (subreddit, author, posted date, score, comment count) and can export CSV + Markdown reports.

## Keywords this tool targets

Reddit link checker, Reddit URL status checker, check if Reddit post is deleted, check if Reddit comment is removed, Reddit moderation status checker, bulk Reddit link audit, Reddit post status API alternative, Reddit thread extraction, Reddit comment extraction, Reddit export CSV.

## Features

- Check Reddit URLs as `live`, `removed`, `deleted`, `not_found`, `fetch_error`, or `invalid_url`
- Process one URL or bulk lists from file
- Export status results to CSV and Markdown
- Optional full thread extraction (post + recursive comments)
- Streamlit UI for non-technical users
- Portable Windows executable builds

### Full thread extraction (post body + comments)

For each thread URL, the app and CLI can also pull from the same JSON response:

- Post: title, full `selftext`, external `url`, domain, flair, NSFW/spoiler/locked/stickied, `score` / `ups` / `downs` / `upvote_ratio`, reported `num_comments`, permalink, thumbnail, edited flag.
- Comments: recursive flatten with **depth**, **parent**, author, full **body** text, score, time, permalink, live/removed/deleted.

Reddit often omits deep branches via `more` objects or caps listing size; the export includes a **note** when counts diverge. For 100% of comments you need the official API.

## Why this works without Reddit API keys

The tool uses Reddit's public `.json` endpoints, for example:
- `https://www.reddit.com/r/<sub>/comments/<post_id>/.json`
- `https://www.reddit.com/r/<sub>/comments/<post_id>/<slug>/<comment_id>/.json`

## Quick install (recommended)

Install directly from GitHub:

```powershell
python -m pip install "git+https://github.com/javedhamzabwn/reddit_analyzer.git"
```

After install, users get two commands:

- `reddit-link-status-checker` (CLI)
- `reddit-link-status-ui` (Streamlit app launcher)

## 1) CLI usage

Single URL:

```powershell
reddit-link-status-checker --url "https://www.reddit.com/r/test/comments/abc123/example_post/"
```

### CLI script usage (developer mode)

```powershell
python reddit_status_checker.py --url "https://www.reddit.com/r/test/comments/abc123/example_post/"
```

Multiple URLs:

```powershell
python reddit_status_checker.py `
  --url "https://www.reddit.com/r/test/comments/abc123/example_post/" `
  --url "https://www.reddit.com/r/test/comments/abc123/example_post/def456/"
```

From file (one URL per line):

```powershell
python reddit_status_checker.py --input-file urls.txt
```

Force type:

```powershell
python reddit_status_checker.py --input-file urls.txt --type comment
```

Custom output paths:

```powershell
python reddit_status_checker.py --input-file urls.txt --csv-out out.csv --md-out out.md
```

Full thread outputs (posts CSV, comments CSV, formatted Markdown, JSON bundle):

```powershell
python reddit_status_checker.py --input-file urls.txt --extract-thread --json-limit 500
```

## 2) UI usage (Streamlit)

Run installed app:

```powershell
reddit-link-status-ui
```

### UI script usage (developer mode)

Install deps manually:

```powershell
python -m pip install -r requirements.txt
```

Run UI:

```powershell
streamlit run app.py
```

## 3) Deploy on Streamlit Community Cloud

1. Push this project to GitHub (already done in your `reddit_analyzer` repo).
2. Go to [https://share.streamlit.io](https://share.streamlit.io) and click **New app**.
3. Select:
   - **Repository**: `javedhamzabwn/reddit_analyzer`
   - **Branch**: `main`
   - **Main file path**: `app.py`
4. Deploy.

### Streamlit Cloud notes

- `requirements.txt` is already present and enough for deployment.
- No secrets are required for basic status checking.
- If you want a custom URL, add it in Streamlit app settings after deploy.

## Build Windows EXE (no Python required on target PC)

You can package this app into a portable Windows app folder containing the `.exe` and all Python/runtime dependencies.

1. On a build machine with Python installed, run:

```powershell
build_exe.bat
```

2. After build completes, share this folder:

```text
dist\RedditResearchWorkspace\
```

3. End user runs:

```text
RedditResearchWorkspace.exe
```

### Single-file EXE option (no folder sharing)

If you want to share just one file, run:

```powershell
build_exe_onefile.bat
```

Output:

```text
dist\RedditResearchWorkspace-OneFile.exe
```

You can send this single `.exe` to users.

### Notes

- Build target is **Windows only** and should match target architecture (x64 -> x64).
- First launch may take a little longer.
- One-file EXE extracts runtime files to a temp directory at startup, so startup is usually slower than the folder (`--onedir`) build.
- The app still opens in a browser tab (Streamlit UI), but no Python install is needed on the end-user machine.

In the UI (tabbed workspace):
1. **Daily Scan**: presets + keyword finder (no API) + lead scoring + duplicate/repost grouping + improved comment extraction
2. **Live Checker**: status-only checker with field selection
3. **Post Viability**: checks if a new comment is likely possible now (status/lock + mod/bot/deleted signals + last comment time + subreddit rules snapshot)
4. **Subreddit Extractor**: subreddit metadata/rules/moderators/widgets (best-effort)
5. Every tab supports selecting fields and **Copy (TSV)** for direct Google Sheets paste

## Notes

- Works for `reddit.com`, `www.reddit.com`, `old.reddit.com`, `np.reddit.com`
- For private/quarantined content, status may return `fetch_error` depending on Reddit response.

## FAQ

### Can I check many Reddit links at once?
Yes. Use `--input-file` with one URL per line.

### Can I use this without Reddit API credentials?
Yes. It uses Reddit public JSON endpoints.

### Can I share this with non-technical users?
Yes. Use either Streamlit Cloud deployment or the EXE build.
