"""
Scrape historical competitor hotel rates (2024-2025) from the
Internet Archive Wayback Machine.

Strategy:
  1. CDX API  — find up to 1 archived snapshot per month per hotel
  2. Fetch    — retrieve each archived Booking.com HTML page
  3. Parse    — extract published "from" price using multiple strategies
  4. Fill     — forward-fill sparse monthly snapshots to a daily spine

Coverage is intentionally sparse (~1-4 data points / month / hotel).
Rates captured are published "from" prices at the moment of archival,
not actual ADR. Document this limitation in the thesis methodology.

Thesis note:
  If the hotel obtains an STR compset report, replace the output of
  this script with that data by dropping competitor_rates_daily.csv
  and re-running 07_build_master_dataset.py.

Output: data/processed/competitor_rates_daily.csv

Competitor URL slugs:
  Verify each at booking.com and update COMPETITORS below if a hotel
  returns "0 snapshots found" — the slug is likely wrong.
"""

import re
import sys
import json
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from app.config import PROCESSED_DIR

DATE_START = "2024-01-01"
DATE_END   = "2025-12-31"

# Booking.com hotel page URLs — verify slugs and update if needed
COMPETITORS = {
    "hoxton":      "https://www.booking.com/hotel/us/the-hoxton-williamsburg.html",
    "williamvale": "https://www.booking.com/hotel/us/the-williamvale.html",
    "moxy":        "https://www.booking.com/hotel/us/moxy-new-york-williamsburg.html",
    "indigo":      "https://www.booking.com/hotel/us/indigo-new-york-williamsburg-brooklyn.html",
    "white_hotel": "https://www.booking.com/hotel/us/white-hotel-brooklyn.html",
}

CDX_URL  = "http://web.archive.org/cdx/search/cdx"
WBM_BASE = "https://web.archive.org/web/{ts}/{url}"
SLEEP_S  = 2.5   # polite delay — Wayback Machine is a shared public resource

PRICE_RE = re.compile(r'\$\s*([\d,]+)', re.IGNORECASE)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; AcademicResearch/1.0; "
        "hotel-forecasting-thesis)"
    )
}


# ---------------------------------------------------------------------------
# Step 1: Discover snapshots via CDX API
# ---------------------------------------------------------------------------

def find_snapshots(url: str) -> list[dict]:
    """
    Return one archived snapshot per month for the given URL.
    collapse=timestamp:6 folds all snapshots in a month to the first one.
    """
    params = {
        "url":          url,
        "output":       "json",
        "from":         "20240101",
        "to":           "20251231",
        "fl":           "timestamp,original,statuscode",
        "filter":       "statuscode:200",
        "collapse":     "timestamp:6",   # one per calendar month
        "limit":        "30",            # 24 months max
    }
    try:
        resp = requests.get(CDX_URL, params=params, timeout=20, headers=HEADERS)
        resp.raise_for_status()
        rows = resp.json()
        if len(rows) <= 1:              # first row is the header
            return []
        header = rows[0]
        return [dict(zip(header, row)) for row in rows[1:]]
    except Exception as exc:
        print(f"      CDX error: {exc}")
        return []


# ---------------------------------------------------------------------------
# Step 2: Fetch a single archived snapshot
# ---------------------------------------------------------------------------

def fetch_snapshot(timestamp: str, url: str) -> str | None:
    """Retrieve the archived HTML at the given Wayback timestamp."""
    wayback_url = WBM_BASE.format(ts=timestamp, url=url)
    try:
        resp = requests.get(wayback_url, timeout=25, headers=HEADERS)
        resp.raise_for_status()
        return resp.text
    except Exception as exc:
        print(f"      Fetch error ({timestamp}): {exc}")
        return None


# ---------------------------------------------------------------------------
# Step 3: Parse a price from HTML
# ---------------------------------------------------------------------------

def _try_meta_description(soup: BeautifulSoup) -> float | None:
    """Extract price from <meta name='description'> — often 'from $XXX'."""
    tag = soup.find("meta", attrs={"name": "description"})
    if tag and tag.get("content"):
        m = PRICE_RE.search(tag["content"])
        if m:
            val = float(m.group(1).replace(",", ""))
            if 50 <= val <= 2000:        # sanity: plausible hotel nightly rate
                return val
    return None


def _try_og_description(soup: BeautifulSoup) -> float | None:
    """Extract price from Open Graph description tag."""
    tag = soup.find("meta", property="og:description")
    if tag and tag.get("content"):
        m = PRICE_RE.search(tag["content"])
        if m:
            val = float(m.group(1).replace(",", ""))
            if 50 <= val <= 2000:
                return val
    return None


def _try_json_ld(soup: BeautifulSoup) -> float | None:
    """Extract price from JSON-LD structured data (schema.org/Hotel)."""
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
            # handle both single objects and arrays
            items = data if isinstance(data, list) else [data]
            for item in items:
                offers = item.get("offers", {})
                if isinstance(offers, dict):
                    price = offers.get("price") or offers.get("lowPrice")
                    if price:
                        val = float(str(price).replace(",", ""))
                        if 50 <= val <= 2000:
                            return val
        except Exception:
            continue
    return None


def _try_next_data(html: str) -> float | None:
    """
    Extract price from __NEXT_DATA__ JSON blob embedded by Next.js apps.
    Booking.com embeds initial page state here; not always present.
    """
    m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.S)
    if not m:
        return None
    try:
        data = json.loads(m.group(1))
        text = json.dumps(data)
        prices = PRICE_RE.findall(text)
        candidates = [
            float(p.replace(",", "")) for p in prices
            if 50 <= float(p.replace(",", "")) <= 2000
        ]
        if candidates:
            return float(np.median(candidates))
    except Exception:
        pass
    return None


def _try_text_search(soup: BeautifulSoup) -> float | None:
    """Last resort: find the lowest plausible dollar amount in visible text."""
    text = soup.get_text(" ", strip=True)
    candidates = []
    for m in PRICE_RE.finditer(text):
        val = float(m.group(1).replace(",", ""))
        if 50 <= val <= 800:            # tighter cap for text search noise
            candidates.append(val)
    if candidates:
        return min(candidates)          # published "from" price = minimum
    return None


def parse_rate(html: str) -> float | None:
    """Try all parsing strategies in order, return first successful price."""
    soup = BeautifulSoup(html, "html.parser")
    for strategy in [
        _try_meta_description,
        _try_og_description,
        _try_json_ld,
    ]:
        price = strategy(soup)
        if price:
            return price
    price = _try_next_data(html)
    if price:
        return price
    return _try_text_search(soup)


# ---------------------------------------------------------------------------
# Step 4: Collect all snapshots for one hotel
# ---------------------------------------------------------------------------

def collect_hotel_rates(label: str, url: str) -> list[dict]:
    """Return a list of {date, rate} dicts for one competitor."""
    print(f"    {label:<15}", end=" ", flush=True)
    snapshots = find_snapshots(url)

    if not snapshots:
        print("→ 0 snapshots (check URL slug)")
        return []

    print(f"→ {len(snapshots)} snapshots found, fetching...", flush=True)
    rows = []
    success = 0

    for snap in snapshots:
        ts  = snap["timestamp"]          # e.g. "20240315120000"
        date = pd.to_datetime(ts[:8], format="%Y%m%d")

        time.sleep(SLEEP_S)
        html = fetch_snapshot(ts, url)
        if html is None:
            continue

        rate = parse_rate(html)
        if rate:
            rows.append({"business_date": date, f"{label}_rate": rate})
            success += 1
            print(f"      {date.date()}  →  ${rate:.0f}")
        else:
            print(f"      {date.date()}  →  (no price parsed)")

    print(f"    {label}: {success}/{len(snapshots)} snapshots yielded prices\n")
    return rows


# ---------------------------------------------------------------------------
# Step 5: Build daily spine with forward-filled rates
# ---------------------------------------------------------------------------

def build_daily_rates(all_rows: dict[str, list[dict]]) -> pd.DataFrame:
    spine = pd.DataFrame({
        "business_date": pd.date_range(DATE_START, DATE_END, freq="D")
    })

    for label, rows in all_rows.items():
        col = f"{label}_rate"
        if not rows:
            spine[col] = np.nan
            continue
        df = pd.DataFrame(rows).drop_duplicates("business_date").sort_values("business_date")
        spine = spine.merge(df[["business_date", col]], on="business_date", how="left")
        # Forward-fill: each snapshot rate applies until the next snapshot
        spine[col] = spine[col].ffill()

    rate_cols = [c for c in spine.columns if c.endswith("_rate")]

    # Aggregate signals across all competitors
    spine["competitor_avg_rate"] = spine[rate_cols].mean(axis=1).round(2)
    spine["competitor_min_rate"] = spine[rate_cols].min(axis=1).round(2)
    spine["competitor_max_rate"] = spine[rate_cols].max(axis=1).round(2)

    # Rate spread: how far apart are competitors on any given day
    spine["competitor_rate_spread"] = (
        spine["competitor_max_rate"] - spine["competitor_min_rate"]
    ).round(2)

    return spine


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("Fetching competitor rates from Wayback Machine (2024-2025)...")
    print("  Polite delay between requests:", SLEEP_S, "seconds\n")

    all_rows: dict[str, list[dict]] = {}

    for label, url in COMPETITORS.items():
        rows = collect_hotel_rates(label, url)
        all_rows[label] = rows
        time.sleep(SLEEP_S)

    daily = build_daily_rates(all_rows)

    col_order = [
        "business_date",
        "hoxton_rate", "williamvale_rate", "moxy_rate",
        "indigo_rate", "white_hotel_rate",
        "competitor_avg_rate", "competitor_min_rate",
        "competitor_max_rate", "competitor_rate_spread",
    ]
    daily = daily[[c for c in col_order if c in daily.columns]]

    out_path = PROCESSED_DIR / "competitor_rates_daily.csv"
    daily.to_csv(out_path, index=False)

    n_covered = daily["competitor_avg_rate"].notna().sum()
    rate_cols = [c for c in daily.columns if c.endswith("_rate") and "competitor" not in c]

    print("\ncompetitor_rates_daily.csv")
    print(f"  Rows with competitor data : {n_covered:,} / {len(daily):,} days")
    print(f"  Coverage                  : {n_covered/len(daily)*100:.1f}% (forward-filled from snapshots)")
    print()
    for col in rate_cols:
        label = col.replace("_rate", "")
        n = daily[col].notna().sum()
        avg = daily[col].mean()
        print(f"  {label:<15} : {n:>3} days with data  |  avg ${avg:.0f}" if n > 0
              else f"  {label:<15} : 0 days — URL slug may need correction")
    print()
    if daily["competitor_avg_rate"].notna().any():
        print(f"  Competitor avg rate range : "
              f"${daily['competitor_avg_rate'].min():.0f} – "
              f"${daily['competitor_avg_rate'].max():.0f}")
    print(f"\n  Saved: {out_path}")
    print()
    print("  NOTE: If a hotel shows 0 data points, navigate to booking.com,")
    print("  find the hotel page, copy the URL slug, and update COMPETITORS")
    print("  at the top of this script, then re-run.")


if __name__ == "__main__":
    main()
