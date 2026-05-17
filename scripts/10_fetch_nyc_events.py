"""
Fetch major NYC-area events and build a daily event signals dataset.

TWO-SOURCE STRATEGY:
  Historical (2024 – today) : Free official league APIs — no key required
    MLB : statsapi.mlb.com     Yankees + Mets home games
    NHL : api-web.nhle.com     Rangers home games
    NBA : nba_api package      Knicks + Nets home games
    US Open: hardcoded         Arthur Ashe Stadium, late Aug – early Sep

  Future (today – 12 months): Ticketmaster Discovery API — uses .env key
    Barclays Center, MSG, Yankee Stadium, Citi Field, Brooklyn venues
    Enables the forecasting app to see upcoming events automatically.

Concert data (Setlist.fm) to be added in a separate step.

Output: data/processed/nyc_events_daily.csv
"""

import sys
import os
import time
from pathlib import Path
from datetime import date, timedelta

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# Load .env from repo root
_env_file = Path(__file__).resolve().parents[1] / ".env"
if _env_file.exists():
    for _line in _env_file.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _, _v = _line.partition("=")
            os.environ.setdefault(_k.strip(), _v.strip().strip('"').strip("'"))

import numpy as np
import pandas as pd
import requests
from app.config import PROCESSED_DIR

HIST_START   = "2024-01-01"
HIST_END     = date.today().isoformat()
FUTURE_END   = (date.today() + timedelta(days=365)).isoformat()
FULL_START   = HIST_START
FULL_END     = FUTURE_END


# ---------------------------------------------------------------------------
# HISTORICAL — League APIs (no key required)
# ---------------------------------------------------------------------------

def fetch_mlb_home_games(team_id: int) -> set:
    home_dates = set()
    for season in [2024, 2025, 2026]:
        try:
            resp = requests.get(
                "https://statsapi.mlb.com/api/v1/schedule",
                params={"sportId": 1, "teamId": team_id, "season": season, "gameType": "R"},
                timeout=20,
            )
            resp.raise_for_status()
            for day in resp.json().get("dates", []):
                for game in day.get("games", []):
                    if game["teams"]["home"]["team"]["id"] == team_id:
                        home_dates.add(date.fromisoformat(day["date"]))
        except Exception as e:
            print(f"      MLB team {team_id} {season}: {e}")
        time.sleep(0.3)
    return home_dates


def fetch_nhl_home_games(team_abbr: str) -> set:
    home_dates = set()
    for season in ["20232024", "20242025", "20252026"]:
        try:
            resp = requests.get(
                f"https://api-web.nhle.com/v1/club-schedule-season/{team_abbr}/{season}",
                timeout=20,
            )
            resp.raise_for_status()
            for game in resp.json().get("games", []):
                if game.get("homeTeam", {}).get("abbrev") == team_abbr:
                    game_date = game["gameDate"][:10]
                    home_dates.add(date.fromisoformat(game_date))
        except Exception as e:
            print(f"      NHL {team_abbr} {season}: {e}")
        time.sleep(0.3)
    return home_dates


def fetch_nba_home_games(team_id: int, team_label: str) -> set:
    from nba_api.stats.endpoints import teamgamelog
    home_dates = set()
    for season in ["2023-24", "2024-25"]:
        try:
            log = teamgamelog.TeamGameLog(
                team_id=team_id,
                season=season,
                season_type_all_star="Regular Season",
                timeout=30,
            )
            df = log.get_data_frames()[0]
            home = df[~df["MATCHUP"].str.contains("@")]
            for game_date_str in home["GAME_DATE"]:
                home_dates.add(pd.to_datetime(game_date_str).date())
            time.sleep(1)
        except Exception as e:
            print(f"      NBA {team_label} {season}: {e}")
    return home_dates


# US Open — always last Mon of Aug to first Sun of Sep
US_OPEN_DATES = set(
    list(pd.date_range("2024-08-26", "2024-09-08").date) +
    list(pd.date_range("2025-08-25", "2025-09-07").date)
)


# ---------------------------------------------------------------------------
# FUTURE — Ticketmaster (upcoming events only)
# ---------------------------------------------------------------------------

TICKETMASTER_VENUES = [
    ("Barclays Center",       "barclays_center"),
    ("Madison Square Garden", "msg"),
    ("Yankee Stadium",        "yankee_stadium"),
    ("Citi Field",            "citi_field"),
    ("Arthur Ashe Stadium",   "arthur_ashe"),
    ("Brooklyn Mirage",       "brooklyn_mirage"),
    ("Knockdown Center",      "knockdown_center"),
]

LARGE_CONCERT_VENUES  = {"barclays_center", "msg", "brooklyn_mirage", "knockdown_center"}
BROOKLYN_MUSIC_VENUES = {"brooklyn_mirage", "knockdown_center"}


def _tm_paginate(params: dict, api_key: str) -> list:
    base = "https://app.ticketmaster.com/discovery/v2/events.json"
    events, page = [], 0
    while page < 5:
        params["page"] = page
        try:
            resp = requests.get(base, params={**params, "apikey": api_key}, timeout=20)
            resp.raise_for_status()
            data = resp.json()
            batch = data.get("_embedded", {}).get("events", [])
            events.extend(batch)
            if page + 1 >= data.get("page", {}).get("totalPages", 1):
                break
            page += 1
            time.sleep(0.25)
        except Exception:
            break
    return events


def fetch_ticketmaster_future(api_key: str) -> tuple[set, set, set]:
    """Return (major_concert_dates, brooklyn_music_dates, future_event_dates)."""
    venue_url = "https://app.ticketmaster.com/discovery/v2/venues.json"
    concert_dates, brooklyn_music_dates, all_future_dates = set(), set(), set()

    for display_name, label in TICKETMASTER_VENUES:
        try:
            r = requests.get(
                venue_url,
                params={"apikey": api_key, "keyword": display_name, "countryCode": "US"},
                timeout=15,
            )
            r.raise_for_status()
            venues = r.json().get("_embedded", {}).get("venues", [])
            venue_id = next(
                (v["id"] for v in venues if display_name.lower() in v.get("name", "").lower()),
                None,
            )
        except Exception:
            venue_id = None

        if not venue_id:
            print(f"      {display_name}: venue not found, skipped")
            continue

        raw = _tm_paginate(
            {
                "venueId":       venue_id,
                "startDateTime": f"{HIST_END}T00:00:00Z",
                "endDateTime":   f"{FUTURE_END}T23:59:59Z",
                "locale":        "*",
            },
            api_key,
        )
        time.sleep(0.25)

        for e in raw:
            d_str = e.get("dates", {}).get("start", {}).get("localDate")
            if not d_str:
                continue
            d = date.fromisoformat(d_str)
            all_future_dates.add(d)
            cls = (e.get("classifications") or [{}])[0]
            segment = cls.get("segment", {}).get("name", "")
            if segment == "Music":
                if label in LARGE_CONCERT_VENUES:
                    concert_dates.add(d)
                if label in BROOKLYN_MUSIC_VENUES:
                    brooklyn_music_dates.add(d)

        print(f"      {display_name:<30} → {len(raw)} upcoming events")

    return concert_dates, brooklyn_music_dates, all_future_dates


# ---------------------------------------------------------------------------
# Build daily signals
# ---------------------------------------------------------------------------

def build_daily_signals(
    yankees, mets, rangers, knicks, nets,
    future_concerts, future_brooklyn, future_all,
) -> pd.DataFrame:

    spine = pd.DataFrame({"business_date": pd.date_range(FULL_START, FULL_END, freq="D")})
    bd = spine["business_date"].dt.date

    # Historical sports
    spine["is_yankees_game"]  = bd.isin(yankees)
    spine["is_mets_game"]     = bd.isin(mets)
    spine["is_rangers_game"]  = bd.isin(rangers)
    spine["is_knicks_game"]   = bd.isin(knicks)
    spine["is_nets_game"]     = bd.isin(nets)
    spine["is_us_open_event"] = bd.isin(US_OPEN_DATES)

    # Future concerts (Ticketmaster)
    spine["is_major_concert"]        = bd.isin(future_concerts)
    spine["is_brooklyn_music_event"] = bd.isin(future_brooklyn)

    # Derived venue flags
    spine["is_msg_event"]      = spine["is_rangers_game"] | spine["is_knicks_game"]
    spine["is_barclays_event"] = spine["is_nets_game"]
    spine["is_mlb_game"]       = spine["is_yankees_game"] | spine["is_mets_game"]
    spine["is_nba_game"]       = spine["is_knicks_game"]  | spine["is_nets_game"]
    spine["is_nhl_game"]       = spine["is_rangers_game"]

    spine["is_major_sports_event"] = spine["is_mlb_game"] | spine["is_nba_game"] | spine["is_nhl_game"]
    spine["is_major_event_day"]    = (
        spine["is_major_sports_event"] |
        spine["is_us_open_event"]      |
        spine["is_major_concert"]      |
        spine["is_brooklyn_music_event"]
    )

    spine["event_count"] = (
        spine[["is_yankees_game", "is_mets_game", "is_rangers_game",
               "is_knicks_game", "is_nets_game", "is_us_open_event",
               "is_major_concert", "is_brooklyn_music_event"]]
        .astype(int).sum(axis=1)
    )

    # Days to next major event — capped at 14
    event_dates_arr = np.array(
        sorted(spine.loc[spine["is_major_event_day"], "business_date"]),
        dtype="datetime64[D]",
    )
    dates_arr = spine["business_date"].values.astype("datetime64[D]")
    idx = np.searchsorted(event_dates_arr, dates_arr)
    days_list = []
    for i in range(len(dates_arr)):
        j = idx[i]
        if j < len(event_dates_arr):
            delta = int((event_dates_arr[j] - dates_arr[i]) / np.timedelta64(1, "D"))
            days_list.append(min(delta, 14))
        else:
            days_list.append(14)
    spine["days_to_next_event"] = days_list

    col_order = [
        "business_date",
        "event_count", "is_major_event_day",
        "is_barclays_event", "is_msg_event",
        "is_yankees_game", "is_mets_game",
        "is_knicks_game", "is_nets_game", "is_rangers_game",
        "is_nba_game", "is_mlb_game", "is_nhl_game",
        "is_us_open_event",
        "is_brooklyn_music_event", "is_major_concert",
        "is_major_sports_event", "days_to_next_event",
    ]
    return spine[[c for c in col_order if c in spine.columns]]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print(f"Fetching NYC events — historical via league APIs, future via Ticketmaster")
    print(f"  Historical : {HIST_START} → {HIST_END}")
    print(f"  Future     : {HIST_END} → {FUTURE_END}\n")

    # --- Historical sports ---
    print("  MLB (statsapi.mlb.com):")
    print("    Yankees...", end=" ", flush=True)
    yankees = fetch_mlb_home_games(147)
    print(f"{len(yankees)} home games")
    print("    Mets...", end=" ", flush=True)
    mets = fetch_mlb_home_games(121)
    print(f"{len(mets)} home games")

    print("\n  NHL (api-web.nhle.com):")
    print("    Rangers...", end=" ", flush=True)
    rangers = fetch_nhl_home_games("NYR")
    print(f"{len(rangers)} home games")

    print("\n  NBA (nba_api / stats.nba.com):")
    print("    Knicks...", end=" ", flush=True)
    knicks = fetch_nba_home_games(1610612752, "knicks")
    print(f"{len(knicks)} home games")
    print("    Nets...", end=" ", flush=True)
    nets = fetch_nba_home_games(1610612751, "nets")
    print(f"{len(nets)} home games")

    print(f"\n  US Open (hardcoded): {len(US_OPEN_DATES)} days")

    # --- Future events via Ticketmaster ---
    api_key = os.getenv("TICKETMASTER_API_KEY", "").strip()
    future_concerts, future_brooklyn, future_all = set(), set(), set()
    if api_key:
        print("\n  Ticketmaster (upcoming events):")
        future_concerts, future_brooklyn, future_all = fetch_ticketmaster_future(api_key)
        print(f"    Major concerts found   : {len(future_concerts)}")
        print(f"    Brooklyn music events  : {len(future_brooklyn)}")
    else:
        print("\n  Ticketmaster: TICKETMASTER_API_KEY not set — skipping future events")

    # --- Build and save ---
    daily = build_daily_signals(
        yankees, mets, rangers, knicks, nets,
        future_concerts, future_brooklyn, future_all,
    )

    out_path = PROCESSED_DIR / "nyc_events_daily.csv"
    daily.to_csv(out_path, index=False)

    print(f"\nnyc_events_daily.csv")
    print(f"  Total rows         : {len(daily):,}  ({FULL_START} → {FULL_END})")
    print(f"  Major event days   : {int(daily['is_major_event_day'].sum())}")
    print(f"  Yankees home games : {int(daily['is_yankees_game'].sum())}")
    print(f"  Mets home games    : {int(daily['is_mets_game'].sum())}")
    print(f"  Rangers home games : {int(daily['is_rangers_game'].sum())}")
    print(f"  Knicks home games  : {int(daily['is_knicks_game'].sum())}")
    print(f"  Nets home games    : {int(daily['is_nets_game'].sum())}")
    print(f"  US Open days       : {int(daily['is_us_open_event'].sum())}")
    print(f"  Future concerts    : {int(daily['is_major_concert'].sum())}")
    print(f"  Brooklyn music     : {int(daily['is_brooklyn_music_event'].sum())}")
    print(f"\n  Saved: {out_path}")


if __name__ == "__main__":
    main()
