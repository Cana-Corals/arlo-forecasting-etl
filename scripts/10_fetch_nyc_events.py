"""
Fetch major NYC-area events (2024-2025) from the Ticketmaster Discovery API.

Tracks events at venues that drive demand for hotels near Arlo Williamsburg:
  Barclays Center (Brooklyn) — Nets, major concerts        [~10 min from hotel]
  Madison Square Garden (Manhattan) — Knicks, Rangers, concerts
  Yankee Stadium (Bronx) — Yankees
  Citi Field (Queens) — Mets

Setup:
  1. Register free at https://developer.ticketmaster.com/
  2. PowerShell : $env:TICKETMASTER_API_KEY = 'your_consumer_key'
     Bash       : export TICKETMASTER_API_KEY=your_consumer_key

Output: data/processed/nyc_events_daily.csv
"""

import sys
import os
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import numpy as np
import pandas as pd
import requests
from app.config import PROCESSED_DIR

DATE_START = "2024-01-01"
DATE_END   = "2025-12-31"

PAGE_SIZE = 200    # Ticketmaster max per request
SLEEP_S   = 0.25  # ~4 req/s — free tier allows 5 req/s

BASE_URL       = "https://app.ticketmaster.com/discovery/v2/events.json"
VENUE_SRCH_URL = "https://app.ticketmaster.com/discovery/v2/venues.json"

# Venues to track; IDs are resolved dynamically at runtime via venue search
TARGET_VENUES = [
    ("Barclays Center",       "barclays_center"),
    ("Madison Square Garden", "msg"),
    ("Yankee Stadium",        "yankee_stadium"),
    ("Citi Field",            "citi_field"),
    # US Open tennis (late Aug – early Sep, large demand spike)
    ("Arthur Ashe Stadium",   "arthur_ashe"),
    # Brooklyn / Queens music venues — close to Arlo Williamsburg
    ("Brooklyn Mirage",       "brooklyn_mirage"),
    ("Brooklyn Storehouse",   "brooklyn_storehouse"),
    ("Knockdown Center",      "knockdown_center"),
]

# Venues large enough to classify a Music event as a "major concert"
LARGE_CONCERT_VENUES = {
    "barclays_center", "msg",
    "brooklyn_mirage", "brooklyn_storehouse", "knockdown_center",
}

# USTA venues used to flag US Open days specifically
USTA_VENUES = {"arthur_ashe"}


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

def _get_api_key() -> str:
    key = os.getenv("TICKETMASTER_API_KEY", "").strip()
    if not key:
        raise EnvironmentError(
            "TICKETMASTER_API_KEY is not set.\n"
            "  1. Register free at https://developer.ticketmaster.com/\n"
            "  2. PowerShell : $env:TICKETMASTER_API_KEY = 'your_key'\n"
            "     Bash       : export TICKETMASTER_API_KEY=your_key"
        )
    return key


def resolve_venue_id(display_name: str, api_key: str) -> str | None:
    """Return the Ticketmaster venue ID for a venue by keyword search."""
    resp = requests.get(
        VENUE_SRCH_URL,
        params={"apikey": api_key, "keyword": display_name, "countryCode": "US"},
        timeout=15,
    )
    resp.raise_for_status()
    venues = resp.json().get("_embedded", {}).get("venues", [])
    for v in venues:
        if display_name.lower() in v.get("name", "").lower():
            return v["id"]
    return None


def _paginate(params: dict) -> list[dict]:
    """Return all event records across pages (capped at 1,000 per query)."""
    events, page = [], 0
    while page < 5:
        params["page"] = page
        resp = requests.get(BASE_URL, params=params, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        batch = data.get("_embedded", {}).get("events", [])
        events.extend(batch)
        total_pages = data.get("page", {}).get("totalPages", 1)
        page += 1
        if page >= total_pages:
            break
        time.sleep(SLEEP_S)
    return events


def _parse_events(raw: list[dict], venue_label: str) -> pd.DataFrame:
    rows = []
    for e in raw:
        date_str = e.get("dates", {}).get("start", {}).get("localDate")
        if not date_str:
            continue
        cls = (e.get("classifications") or [{}])[0]
        rows.append({
            "business_date": date_str,
            "event_name":    e.get("name", ""),
            "venue":         venue_label,
            "segment":       cls.get("segment", {}).get("name", ""),
            "genre":         cls.get("genre",   {}).get("name", ""),
        })
    if rows:
        return pd.DataFrame(rows)
    return pd.DataFrame(columns=["business_date", "event_name", "venue", "segment", "genre"])


def fetch_venue_events(venue_id: str, venue_label: str, api_key: str) -> pd.DataFrame:
    print(f"    {venue_label:<25}", end=" ", flush=True)
    params = {
        "apikey":        api_key,
        "venueId":       venue_id,
        "startDateTime": f"{DATE_START}T00:00:00Z",
        "endDateTime":   f"{DATE_END}T23:59:59Z",
        "locale":        "*",
    }
    raw = _paginate(params)
    time.sleep(SLEEP_S)
    print(f"→ {len(raw):,} events")
    return _parse_events(raw, venue_label)


# ---------------------------------------------------------------------------
# Aggregate events → daily signals
# ---------------------------------------------------------------------------

def build_daily_signals(events: pd.DataFrame) -> pd.DataFrame:
    spine = pd.DataFrame({"business_date": pd.date_range(DATE_START, DATE_END, freq="D")})

    bool_cols = [
        "is_barclays_event", "is_msg_event",
        "is_yankees_game", "is_mets_game",
        "is_nba_game", "is_mlb_game", "is_nhl_game",
        "is_us_open_event",
        "is_brooklyn_music_event",
        "is_major_concert", "is_major_sports_event", "is_major_event_day",
    ]
    for col in bool_cols:
        spine[col] = False
    spine["event_count"]       = 0
    spine["days_to_next_event"] = 14

    if events.empty:
        return spine

    events = events.copy()
    events["business_date"] = pd.to_datetime(events["business_date"])

    def date_set(mask: pd.Series) -> set:
        return set(events.loc[mask, "business_date"])

    barclays_dates       = date_set(events["venue"] == "barclays_center")
    msg_dates            = date_set(events["venue"] == "msg")
    yankees_dates        = date_set((events["venue"] == "yankee_stadium") & (events["genre"] == "Baseball"))
    mets_dates           = date_set((events["venue"] == "citi_field")     & (events["genre"] == "Baseball"))
    nba_dates            = date_set(events["genre"] == "Basketball")
    mlb_dates            = date_set(events["genre"] == "Baseball")
    nhl_dates            = date_set(events["genre"] == "Hockey")
    us_open_dates        = date_set(events["venue"].isin(USTA_VENUES))
    brooklyn_music_dates = date_set(
        (events["segment"] == "Music") &
        events["venue"].isin({"brooklyn_mirage", "brooklyn_storehouse", "knockdown_center"})
    )
    concert_dates        = date_set(
        (events["segment"] == "Music") & events["venue"].isin(LARGE_CONCERT_VENUES)
    )

    counts = events["business_date"].value_counts().rename("event_count").reset_index()
    counts.columns = ["business_date", "event_count"]
    spine = spine.drop(columns=["event_count"]).merge(counts, on="business_date", how="left")
    spine["event_count"] = spine["event_count"].fillna(0).astype(int)

    def set_flag(col: str, date_set_: set) -> None:
        spine[col] = spine["business_date"].isin(date_set_)

    set_flag("is_barclays_event",    barclays_dates)
    set_flag("is_msg_event",         msg_dates)
    set_flag("is_yankees_game",      yankees_dates)
    set_flag("is_mets_game",         mets_dates)
    set_flag("is_nba_game",          nba_dates)
    set_flag("is_mlb_game",          mlb_dates)
    set_flag("is_nhl_game",          nhl_dates)
    set_flag("is_us_open_event",     us_open_dates)
    set_flag("is_brooklyn_music_event", brooklyn_music_dates)
    set_flag("is_major_concert",     concert_dates)

    spine["is_major_sports_event"] = (
        spine["is_nba_game"] | spine["is_mlb_game"] | spine["is_nhl_game"]
    )
    spine["is_major_event_day"] = (
        spine["is_barclays_event"]    | spine["is_msg_event"]   |
        spine["is_yankees_game"]      | spine["is_mets_game"]   |
        spine["is_us_open_event"]     | spine["is_brooklyn_music_event"]
    )

    # Days until the next major event — captures pre-event booking lead-up demand.
    # Value of 0 means the event is today; capped at 14.
    event_dates_arr = np.array(
        sorted(spine.loc[spine["is_major_event_day"], "business_date"]),
        dtype="datetime64[D]",
    )
    dates_arr = spine["business_date"].values.astype("datetime64[D]")
    idx = np.searchsorted(event_dates_arr, dates_arr)  # first event >= each date

    days_list = []
    for i in range(len(dates_arr)):
        j = idx[i]
        if j < len(event_dates_arr):
            delta = int((event_dates_arr[j] - dates_arr[i]) / np.timedelta64(1, "D"))
            days_list.append(delta)
        else:
            days_list.append(14)
    spine["days_to_next_event"] = np.clip(days_list, 0, 14)

    return spine


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    api_key = _get_api_key()
    print("Fetching NYC major events (2024-2025) from Ticketmaster Discovery API...")
    print("  Resolving venue IDs...")

    venue_id_map: dict[str, tuple[str, str]] = {}
    for display_name, label in TARGET_VENUES:
        vid = resolve_venue_id(display_name, api_key)
        time.sleep(SLEEP_S)
        if vid:
            venue_id_map[label] = (vid, display_name)
            print(f"    {display_name:<30} → {vid}")
        else:
            print(f"    {display_name:<30} → NOT FOUND (skipped)")

    print("\n  Fetching events by venue...")
    all_frames = []
    for label, (vid, display_name) in venue_id_map.items():
        df = fetch_venue_events(vid, label, api_key)
        all_frames.append(df)

    events = pd.concat(all_frames, ignore_index=True) if all_frames else pd.DataFrame()
    print(f"\n  Total raw events collected: {len(events):,}")

    daily = build_daily_signals(events)

    col_order = [
        "business_date",
        "event_count", "is_major_event_day",
        "is_barclays_event", "is_msg_event",
        "is_yankees_game", "is_mets_game",
        "is_nba_game", "is_mlb_game", "is_nhl_game",
        "is_us_open_event",
        "is_brooklyn_music_event",
        "is_major_concert", "is_major_sports_event",
        "days_to_next_event",
    ]
    daily = daily[[c for c in col_order if c in daily.columns]]

    out_path = PROCESSED_DIR / "nyc_events_daily.csv"
    daily.to_csv(out_path, index=False)

    print(f"\nnyc_events_daily.csv")
    print(f"  Rows               : {len(daily):,} (one per calendar day)")
    print(f"  Major event days   : {int(daily['is_major_event_day'].sum())}")
    print(f"  NBA game days      : {int(daily['is_nba_game'].sum())}")
    print(f"  MLB game days      : {int(daily['is_mlb_game'].sum())}")
    print(f"  NHL game days      : {int(daily['is_nhl_game'].sum())}")
    print(f"  US Open days       : {int(daily['is_us_open_event'].sum())}")
    print(f"  Brooklyn music days: {int(daily['is_brooklyn_music_event'].sum())}")
    print(f"  Concert days       : {int(daily['is_major_concert'].sum())}")
    print(f"  Barclays events    : {int(daily['is_barclays_event'].sum())}")
    print(f"  MSG events         : {int(daily['is_msg_event'].sum())}")
    print(f"\n  Saved: {out_path}")


if __name__ == "__main__":
    main()
