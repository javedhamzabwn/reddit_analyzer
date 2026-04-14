# Reddit Link Status Checker

Professional Reddit post/comment URL monitoring tool with:
- CLI for automation and batch workflows
- Streamlit UI (full and minimal variants)
- Windows executable builds for non-technical users

Use this project to check whether Reddit links are live, removed, deleted, not found, restricted, or invalid, and to extract subreddit/user metadata without API credentials.

## Core use cases

- Reddit link status checker for post and comment URLs
- Bulk Reddit URL audit from a text file
- Subreddit metadata extraction (rules, moderators, widgets)
- Reddit user profile and activity extraction
- Export-ready CSV reports for ops, moderation, or growth teams

## Status types

- `live`
- `removed`
- `deleted`
- `not_found`
- `restricted`
- `fetch_error`
- `invalid_url`

## Project variants

- **Full app**: `app.py` (all advanced tabs and workflows)
- **Minimal app**: `app_minimal.py` (bright UI with 3 focused tabs)
  - Live/Unlive Status Checker
  - Subreddit Extractor
  - User Info Extractor

## Installation

Install directly from GitHub:

```powershell
python -m pip install "git+https://github.com/javedhamzabwn/reddit_analyzer.git"
```

Installed commands:

- `reddit-link-status-checker` (CLI)
- `reddit-link-status-ui` (full Streamlit launcher)
- `reddit-link-status-ui-minimal` (minimal Streamlit launcher)

## Quick start

### CLI

Single URL check:

```powershell
reddit-link-status-checker --url "https://www.reddit.com/r/test/comments/abc123/example_post/"
```

Batch from file:

```powershell
reddit-link-status-checker --input-file urls.txt
```

Thread extraction outputs:

```powershell
reddit-link-status-checker --input-file urls.txt --extract-thread --json-limit 500
```

### Streamlit UI

Full UI:

```powershell
reddit-link-status-ui
```

Minimal UI:

```powershell
reddit-link-status-ui-minimal
```

## Developer mode

Install dependencies:

```powershell
python -m pip install -r requirements.txt
```

Run full app:

```powershell
streamlit run app.py
```

Run minimal app:

```powershell
streamlit run app_minimal.py
```

## Deployment options

### Streamlit Community Cloud

1. Push repository to GitHub
2. Open [https://share.streamlit.io](https://share.streamlit.io)
3. Create app and set:
   - Repository: `javedhamzabwn/reddit_analyzer`
   - Branch: `main`
   - Main file: `app.py` or `app_minimal.py`

### Google Cloud Run (recommended for production)

Run Streamlit with:

```bash
streamlit run app_minimal.py --server.port=$PORT --server.address=0.0.0.0 --server.headless=true
```

Then deploy with `gcloud run deploy`.

## Windows EXE builds

### Folder build (faster startup)

```powershell
build_exe.bat
```

Share folder:

```text
dist\RedditResearchWorkspace\
```

### Single-file build (easier sharing)

```powershell
build_exe_onefile.bat
```

Share file:

```text
dist\RedditResearchWorkspace-OneFile.exe
```

## Recommended tools

- **Python 3.10+**
- **GitHub CLI (`gh`)** for release and repo automation
- **Streamlit** for UI hosting
- **PyInstaller** for Windows executable packaging
- **Google Cloud Run** for managed production deployment

## Architecture summary

- `reddit_status_checker.py`: core URL normalization, status inference, extraction logic
- `app.py`: full Streamlit interface
- `app_minimal.py`: simplified bright Streamlit interface
- `launcher.py` / `launcher_minimal.py`: entrypoints for installed UI commands
- `build_exe*.bat`: Windows packaging scripts

## Notes and limitations

- Supports `reddit.com`, `www.reddit.com`, `old.reddit.com`, `np.reddit.com`, and `redd.it`
- Public endpoint behavior can vary by region/network/rate limits
- Deep threads may include `more` placeholders; full comment completeness requires official API
- Private or quarantined content may return `restricted` or `fetch_error`

## FAQ

### Can this run without Reddit API keys?
Yes. It uses Reddit public JSON endpoints.

### Can I share with non-technical users?
Yes. Use Streamlit deployment or the Windows EXE release artifact.

### Why might hosted behavior differ from local?
Network/rate limits/redirect behavior can differ by host. Recent updates include fallback handling for `redd.it` short links in cloud environments.
