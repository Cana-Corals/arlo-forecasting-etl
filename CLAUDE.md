# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

ETL pipeline for hotel revenue forecasting at Arlo Williamsburg. Ingests reservation, rate, guest satisfaction, and daily statistics data to support forecasting workflows.

## Running Scripts

```powershell
# Run the data inspection script
python scripts/01_load_inspect_raw_files.py
```

**Dependencies:**
```powershell
pip install -r requirements.txt
```

## Architecture

```
data/raw/       → source files (CSV, XLSX, XLS) — never modified
scripts/        → numbered ETL steps (01_, 02_, ...)
app/            → reusable modules (not yet populated)
outputs/        → processed/transformed data
models/         → model artifacts
notebooks/      → exploratory analysis
```

**Data sources loaded in `scripts/01_load_inspect_raw_files.py`:**
- `res_daily_*.csv` — daily reservation snapshots (2024–2025)
- `res_main.csv` — full reservation records
- `wburg_daily_stats_source_*.csv` — daily stats by origin/source code
- `wburg_daily_stats_by_market_*.csv` — daily stats by market segment
- `wburg_daily_stats_by_rt.csv` — daily stats by room type
- `Arlo+Williamsburg+RateChange_*.xlsx` — rate change history
- `medallia_*.xls` — guest satisfaction scores (half-year chunks, HTML-based XLS format)

**File reading**: `read_file()` in `01_load_inspect_raw_files.py` handles CSV, XLSX, and the Medallia `.xls` files (which are actually HTML tables requiring `pd.read_html` fallback).

**Path convention**: All scripts use `Path(__file__).resolve().parents[1]` to anchor `BASE_DIR` to the repo root, so scripts run correctly regardless of working directory.

## Git Workflow

As work is completed, Claude must regularly commit changes to Git and push them to GitHub using clean, professional commit messages. Before starting a new task, check Git status. After completing each task, commit the changes and push them to GitHub when the remote is connected. This ensures the project always has a saved version and can be safely reverted if needed.

Commit message conventions used in this project:
- `chore:` — setup, config, tooling
- `docs:` — README, CLAUDE.md, comments
- `feat:` — new scripts or modules
- `fix:` — bug or data corrections
- `test:` — validation or profiling scripts

Do NOT add Co-Authored-By or any AI attribution lines to commit messages.
