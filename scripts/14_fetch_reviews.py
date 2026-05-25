"""
Fetch Google Reviews for Arlo Williamsburg via Google Places API (New).

Usage:
    python scripts/14_fetch_reviews.py

Requirements:
    - GOOGLE_MAPS_API_KEY in .env or environment
    - pip install requests

The Places API is free within Google's $200/month credit (covers ~11,700 requests).
To get an API key:
  1. Go to https://console.cloud.google.com/
  2. Enable "Places API (New)"
  3. Create an API key under Credentials
  4. Add it to .env: GOOGLE_MAPS_API_KEY=your_key_here

Output: data/raw/google_reviews_raw.csv
"""

import os
import sys
import json
import time
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR  = BASE_DIR / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

# ── Config ────────────────────────────────────────────────────────────────────
HOTEL_NAME    = "Arlo Williamsburg"
HOTEL_ADDRESS = "96 Wythe Ave, Brooklyn, NY 11249"
PLACE_ID      = "ChIJt5bMqlBbwokRzgxGJz-qXLc"   # pre-looked-up, avoids a billable search call

# Try .env first, then environment
try:
    from dotenv import load_dotenv
    load_dotenv(BASE_DIR / ".env")
except ImportError:
    pass

API_KEY = os.getenv("GOOGLE_MAPS_API_KEY", "")


def fetch_place_details(place_id: str, api_key: str) -> dict:
    """Fetch place details including reviews from Places API (New)."""
    url = f"https://places.googleapis.com/v1/places/{place_id}"
    headers = {
        "X-Goog-Api-Key": api_key,
        "X-Goog-FieldMask": "displayName,rating,userRatingCount,reviews",
    }
    resp = requests.get(url, headers=headers, timeout=15)
    resp.raise_for_status()
    return resp.json()


def parse_reviews(data: dict) -> list[dict]:
    """Extract review records from Places API response."""
    out = []
    for r in data.get("reviews", []):
        # relativePublishTimeDescription e.g. "2 weeks ago"
        out.append({
            "source":        "Google",
            "author":        r.get("authorAttribution", {}).get("displayName", ""),
            "rating":        r.get("rating"),
            "text":          r.get("text", {}).get("text", ""),
            "language":      r.get("text", {}).get("languageCode", ""),
            "relative_time": r.get("relativePublishTimeDescription", ""),
            "publish_time":  r.get("publishTime", ""),
            "fetched_at":    datetime.utcnow().isoformat(),
        })
    return out


def main():
    if not API_KEY:
        print("ERROR: GOOGLE_MAPS_API_KEY not set.")
        print("       Add it to .env: GOOGLE_MAPS_API_KEY=your_key_here")
        print("       Get a free key at: https://console.cloud.google.com/")
        sys.exit(1)

    print(f"Fetching reviews for {HOTEL_NAME} (place_id={PLACE_ID}) …")
    data = fetch_place_details(PLACE_ID, API_KEY)

    overall_rating = data.get("rating")
    total_ratings  = data.get("userRatingCount")
    print(f"  Overall rating : {overall_rating} ({total_ratings:,} total ratings on Google)")

    reviews = parse_reviews(data)
    print(f"  Reviews returned by API: {len(reviews)}")
    print("  Note: Google Places API returns the 5 most relevant reviews.")
    print("        For all reviews, export from Google Business Profile dashboard.")

    if not reviews:
        print("No reviews returned.")
        sys.exit(0)

    out_path = RAW_DIR / "google_reviews_raw.csv"

    # Append to existing file if present (for incremental updates)
    if out_path.exists():
        existing = pd.read_csv(out_path)
        df = pd.concat([existing, pd.DataFrame(reviews)], ignore_index=True)
        df = df.drop_duplicates(subset=["author", "publish_time"])
    else:
        df = pd.DataFrame(reviews)

    df.to_csv(out_path, index=False)
    print(f"\nSaved {len(df)} reviews → {out_path.relative_to(BASE_DIR)}")
    for r in reviews:
        stars = "★" * int(r["rating"] or 0)
        print(f"  [{stars}] {r['author'][:30]:<30} {r['relative_time']}")
        print(f"         {r['text'][:100]}…" if len(r['text']) > 100 else f"         {r['text']}")


if __name__ == "__main__":
    main()
