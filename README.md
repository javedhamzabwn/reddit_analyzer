# Reddit Link Status Checker

No-credentials checker for Reddit post/comment URLs.

It checks whether each URL is:
- `live`
- `removed`
- `deleted`
- `not_found`
- `fetch_error`
- `invalid_url`

It also captures metadata (subreddit, author, posted date, score, comment count) and can export CSV + Markdown.

### Full thread extraction (post body + comments)

For each thread URL, the app and CLI can also pull from the same JSON response:

- Post: title, full `selftext`, external `url`, domain, flair, NSFW/spoiler/locked/stickied, `score` / `ups` / `downs` / `upvote_ratio`, reported `num_comments`, permalink, thumbnail, edited flag.
- Comments: recursive flatten with **depth**, **parent**, author, full **body** text, score, time, permalink, live/removed/deleted.

Reddit often omits deep branches via `more` objects or caps listing size; the export includes a **note** when counts diverge. For 100% of comments you need the official API.

## Why this works without Reddit API keys

The tool uses Reddit's public `.json` endpoints, for example:
- `https://www.reddit.com/r/<sub>/comments/<post_id>/.json`
- `https://www.reddit.com/r/<sub>/comments/<post_id>/<slug>/<comment_id>/.json`

## 1) CLI script usage

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

Install deps:

```powershell
python -m pip install -r requirements.txt
```

Run UI:

```powershell
streamlit run app.py
```

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

### Notes

- Build target is **Windows only** and should match target architecture (x64 -> x64).
- First launch may take a little longer.
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
