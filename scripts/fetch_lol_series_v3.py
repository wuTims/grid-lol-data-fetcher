#!/usr/bin/env python3
"""
GRID API Data Fetcher for LoL Draft Assistant (v3 - Resilient)

Features:
- Saves after EVERY successful series fetch (no data loss)
- Batch processing with configurable batch sizes
- Detailed logging with timestamps
- Separate raw JSON files per batch (prevents large file corruption)
- Easy resume from any interruption point
- Progress summary and ETA tracking

Usage:
    python fetch_lol_series_v3.py                    # Fetch all series from CSV
    python fetch_lol_series_v3.py --limit 100        # Fetch first 100 series from CSV
    python fetch_lol_series_v3.py --series 123,456   # Fetch specific series IDs (comma-separated)
    python fetch_lol_series_v3.py --api-key KEY      # Use custom API key
    python fetch_lol_series_v3.py --input file.csv   # Use custom input CSV
    python fetch_lol_series_v3.py --batch-size 25    # Smaller batches
    python fetch_lol_series_v3.py --reset            # Clear progress and start fresh
    python fetch_lol_series_v3.py --status           # Show current progress
    python fetch_lol_series_v3.py --export           # Export all data to CSVs
"""

import argparse
import csv
import json
import os
import sys
import time
import requests
from datetime import datetime, timedelta
from pathlib import Path
from collections import defaultdict
from typing import Optional, Dict, List, Tuple

# =============================================================================
# Configuration (defaults - can be overridden via CLI or environment)
# =============================================================================

API_URL = "https://api-op.grid.gg/live-data-feed/series-state/graphql"
RATE_LIMIT = 20  # requests per minute
DELAY = 60 / RATE_LIMIT + 0.1  # 3.1 seconds between requests (small buffer)

# File paths (defaults - INPUT_CSV can be overridden via CLI)
DEFAULT_INPUT_CSV = "/mnt/user-data/uploads/LoLSeriesGames_2024_2025.csv"
OUTPUT_DIR = Path("/mnt/user-data/outputs")
DATA_DIR = OUTPUT_DIR / "grid_data"
BATCHES_DIR = DATA_DIR / "batches"
PROGRESS_FILE = DATA_DIR / "progress.json"
LOG_FILE = DATA_DIR / "fetch.log"

# Runtime config (set by CLI args or environment)
API_KEY = os.environ.get("GRID_API_KEY", "")
INPUT_CSV = DEFAULT_INPUT_CSV

# =============================================================================
# GraphQL Queries
# =============================================================================

QUERY_VERSION = """
query VersionCheck($seriesId: ID!) {
  seriesState(id: $seriesId) { id version }
}
"""

QUERIES = {
    "base": """
query SeriesState($seriesId: ID!) {
  seriesState(id: $seriesId) {
    id version
    title { nameShortened }
    format started finished startedAt
    teams { id name won score }
    games {
      id sequenceNumber started finished paused
      clock { currentSeconds ticking }
      map { name }
      draftActions {
        id sequenceNumber type
        drafter { id type }
        draftable { id type name }
      }
      teams {
        id name side won score kills deaths structuresDestroyed
        objectives { id type }
        players {
          id name participationStatus
          character { id name }
          kills deaths killAssistsGiven
        }
      }
    }
  }
}
""",
    "v3.10": """
query SeriesState($seriesId: ID!) {
  seriesState(id: $seriesId) {
    id version
    title { nameShortened }
    format started finished startedAt
    teams { id name won score }
    games {
      id sequenceNumber started finished paused
      clock { currentSeconds ticking }
      map { name }
      draftActions {
        id sequenceNumber type
        drafter { id type }
        draftable { id type name }
      }
      teams {
        id name side won score kills deaths structuresDestroyed firstKill
        objectives { id type }
        players {
          id name participationStatus
          character { id name }
          kills deaths killAssistsGiven firstKill
        }
      }
    }
  }
}
""",
    "v3.23": """
query SeriesState($seriesId: ID!) {
  seriesState(id: $seriesId) {
    id version
    title { nameShortened }
    format started finished startedAt
    teams { id name won score }
    games {
      id sequenceNumber started finished paused
      clock { currentSeconds ticking }
      titleVersion { name }
      map { name }
      draftActions {
        id sequenceNumber type
        drafter { id type }
        draftable { id type name }
      }
      teams {
        id name side won score kills deaths structuresDestroyed firstKill
        objectives { id type }
        players {
          id name participationStatus
          character { id name }
          kills deaths killAssistsGiven firstKill
          ... on GamePlayerStateLol {
            damageDealt
            experiencePoints
          }
        }
      }
    }
  }
}
""",
    "v3.30": """
query SeriesState($seriesId: ID!) {
  seriesState(id: $seriesId) {
    id version
    title { nameShortened }
    format started finished startedAt
    teams { id name won score }
    games {
      id sequenceNumber started finished paused
      clock { currentSeconds ticking }
      titleVersion { name }
      map { name }
      draftActions {
        id sequenceNumber type
        drafter { id type }
        draftable { id type name }
      }
      teams {
        id name side won score kills deaths structuresDestroyed firstKill
        objectives { id type }
        players {
          id name participationStatus
          character { id name }
          kills deaths killAssistsGiven firstKill
          ... on GamePlayerStateLol {
            damageDealt
            experiencePoints
            visionScore
            kdaRatio
          }
        }
      }
    }
  }
}
""",
    "v3.35": """
query SeriesState($seriesId: ID!) {
  seriesState(id: $seriesId) {
    id version
    title { nameShortened }
    format started finished startedAt
    teams { id name won score }
    games {
      id sequenceNumber started finished paused
      clock { currentSeconds ticking }
      titleVersion { name }
      map { name }
      draftActions {
        id sequenceNumber type
        drafter { id type }
        draftable { id type name }
      }
      teams {
        id name side won score kills deaths structuresDestroyed firstKill
        objectives { id type }
        players {
          id name participationStatus
          character { id name }
          kills deaths killAssistsGiven firstKill
          ... on GamePlayerStateLol {
            damageDealt
            experiencePoints
            visionScore
            kdaRatio
            killParticipation
          }
        }
      }
    }
  }
}
""",
    "v3.43": """
query SeriesState($seriesId: ID!) {
  seriesState(id: $seriesId) {
    id version
    title { nameShortened }
    format started finished startedAt
    teams { id name won score }
    games {
      id sequenceNumber started finished paused
      clock { currentSeconds ticking }
      titleVersion { name }
      map { name }
      draftActions {
        id sequenceNumber type
        drafter { id type }
        draftable { id type name }
      }
      teams {
        id name side won score kills deaths structuresDestroyed firstKill
        objectives { id type }
        players {
          id name participationStatus
          character { id name }
          kills deaths killAssistsGiven firstKill
          ... on GamePlayerStateLol {
            damageDealt
            experiencePoints
            visionScore
            kdaRatio
            killParticipation
          }
        }
      }
    }
  }
}
"""
}


# =============================================================================
# Utility Functions
# =============================================================================

def setup_directories():
    """Create necessary directories."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    BATCHES_DIR.mkdir(parents=True, exist_ok=True)


def log(message: str, also_print: bool = True):
    """Log message to file and optionally print."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_line = f"[{timestamp}] {message}"
    
    with open(LOG_FILE, 'a') as f:
        f.write(log_line + "\n")
    
    if also_print:
        print(log_line)


def load_progress() -> Dict:
    """Load progress from file."""
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE, 'r') as f:
            return json.load(f)
    return {
        "completed": {},
        "failed": {},
        "started_at": None,
        "last_updated": None,
        "version_stats": {}
    }


def save_progress(progress: Dict):
    """Save progress to file."""
    progress["last_updated"] = datetime.now().isoformat()
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(progress, f, indent=2)


def load_series_ids(csv_path: str) -> List[str]:
    """Extract unique series IDs from CSV."""
    series_ids = set()
    with open(csv_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            series_ids.add(row['SeriesID'])
    return sorted(list(series_ids))


def select_query_for_version(version_str: str) -> str:
    """Select appropriate query based on schema version string."""
    parts = version_str.split('.')
    major = int(parts[0])
    minor = int(parts[1]) if len(parts) > 1 else 0
    version_tuple = (major, minor)
    
    if version_tuple >= (3, 43):
        return QUERIES["v3.43"]
    elif version_tuple >= (3, 35):
        return QUERIES["v3.35"]
    elif version_tuple >= (3, 30):
        return QUERIES["v3.30"]
    elif version_tuple >= (3, 23):
        return QUERIES["v3.23"]
    elif version_tuple >= (3, 10):
        return QUERIES["v3.10"]
    return QUERIES["base"]


def format_duration(seconds: float) -> str:
    """Format seconds into human readable duration."""
    if seconds < 60:
        return f"{seconds:.0f}s"
    elif seconds < 3600:
        return f"{seconds/60:.1f}m"
    else:
        return f"{seconds/3600:.1f}h"


# =============================================================================
# API Functions
# =============================================================================

def create_session() -> requests.Session:
    """Create configured requests session."""
    session = requests.Session()
    session.headers.update({
        "Content-Type": "application/json",
        "x-api-key": API_KEY
    })
    return session


def fetch_version(series_id: str, session: requests.Session) -> Optional[str]:
    """Fetch schema version for a series."""
    try:
        response = session.post(
            API_URL,
            json={"query": QUERY_VERSION, "variables": {"seriesId": series_id}},
            timeout=30
        )
        response.raise_for_status()
        result = response.json()
        
        if "errors" in result:
            return None
        
        series_state = result.get("data", {}).get("seriesState")
        if not series_state:
            return None
            
        return series_state.get("version", "3.0")
    except Exception as e:
        log(f"  Version fetch error for {series_id}: {e}")
        return None


def fetch_series_data(series_id: str, version: str, session: requests.Session) -> Tuple[Optional[Dict], Optional[str]]:
    """Fetch series data with appropriate query. Returns (data, error)."""
    try:
        query = select_query_for_version(version)
        response = session.post(
            API_URL,
            json={"query": query, "variables": {"seriesId": series_id}},
            timeout=30
        )
        response.raise_for_status()
        result = response.json()
        
        if "errors" in result:
            error_msg = result["errors"][0].get("message", "Unknown error")
            return None, error_msg
        
        return result, None
    except requests.exceptions.Timeout:
        return None, "Request timeout"
    except requests.exceptions.RequestException as e:
        return None, f"Network error: {str(e)}"
    except Exception as e:
        return None, f"Unexpected error: {str(e)}"


# =============================================================================
# Batch Processing
# =============================================================================

def save_series_data(series_id: str, data: Dict):
    """Save individual series data to a JSON file."""
    series_file = BATCHES_DIR / f"series_{series_id}.json"
    with open(series_file, 'w') as f:
        json.dump(data, f)
    return str(series_file)


def load_all_series_data() -> Dict[str, Dict]:
    """Load all series files into a single dict."""
    all_data = {}
    series_files = BATCHES_DIR.glob("series_*.json")
    
    for series_file in series_files:
        series_id = series_file.stem.replace("series_", "")
        with open(series_file, 'r') as f:
            all_data[series_id] = json.load(f)
    
    return all_data


def process_series(
    series_id: str,
    session: requests.Session,
    progress: Dict
) -> Tuple[bool, Optional[str]]:
    """Process a single series. Returns (success, error_message)."""
    
    # Fetch version
    version = fetch_version(series_id, session)
    time.sleep(DELAY)
    
    if version is None:
        return False, "Could not fetch version"
    
    # Track version stats
    version_key = f"v{version}"
    progress["version_stats"][version_key] = progress["version_stats"].get(version_key, 0) + 1
    
    # Fetch data
    data, error = fetch_series_data(series_id, version, session)
    time.sleep(DELAY)
    
    if error:
        return False, error
    
    # Save individual series file immediately
    save_series_data(series_id, data)
    
    # Extract team names for logging
    series_state = data.get("data", {}).get("seriesState", {})
    teams = series_state.get("teams", [])
    team_names = " vs ".join([t.get("name", "?")[:15] for t in teams])
    
    return True, f"v{version}: {team_names}"


# =============================================================================
# Data Export
# =============================================================================

def infer_role_from_champion(champion_name: str) -> str:
    """Infer role from champion pick."""
    TOP = {'Aatrox', 'Ambessa', 'Aurora', 'Camille', 'Cho\'Gath', 'Darius', 'Dr. Mundo', 'Fiora', 
           'Gangplank', 'Garen', 'Gnar', 'Gragas', 'Gwen', 'Illaoi', 'Irelia', 'Jax', 'Jayce',
           'K\'Sante', 'Kayle', 'Kennen', 'Kled', 'Malphite', 'Mordekaiser', 'Nasus', 'Olaf',
           'Ornn', 'Quinn', 'Renekton', 'Riven', 'Rumble', 'Sett', 'Shen', 'Singed', 'Sion',
           'Tahm Kench', 'Teemo', 'Trundle', 'Tryndamere', 'Urgot', 'Volibear', 'Warwick',
           'Wukong', 'Yasuo', 'Yone', 'Yorick'}
    JNG = {'Amumu', 'Bel\'Veth', 'Brand', 'Briar', 'Diana', 'Ekko', 'Elise', 'Evelynn',
           'Fiddlesticks', 'Graves', 'Hecarim', 'Ivern', 'Jarvan IV', 'Karthus', 'Kayn',
           'Kha\'Zix', 'Kindred', 'Lee Sin', 'Lillia', 'Maokai', 'Master Yi', 'Nidalee',
           'Nocturne', 'Nunu & Willump', 'Poppy', 'Rek\'Sai', 'Rengar', 'Sejuani', 'Shaco',
           'Shyvana', 'Skarner', 'Talon', 'Udyr', 'Vi', 'Viego', 'Xin Zhao', 'Zac'}
    MID = {'Ahri', 'Akali', 'Akshan', 'Anivia', 'Annie', 'Aurelion Sol', 'Azir', 'Cassiopeia',
           'Corki', 'Fizz', 'Galio', 'Hwei', 'Kassadin', 'Katarina', 'LeBlanc', 'Lissandra',
           'Lux', 'Malzahar', 'Naafiri', 'Neeko', 'Orianna', 'Qiyana', 'Ryze', 'Syndra',
           'Sylas', 'Taliyah', 'Twisted Fate', 'Veigar', 'Vex', 'Viktor', 'Vladimir',
           'Xerath', 'Zed', 'Ziggs', 'Zoe'}
    ADC = {'Aphelios', 'Ashe', 'Caitlyn', 'Draven', 'Ezreal', 'Jhin', 'Jinx', 'Kai\'Sa',
           'Kalista', 'Kog\'Maw', 'Lucian', 'Miss Fortune', 'Nilah', 'Samira', 'Senna',
           'Sivir', 'Smolder', 'Tristana', 'Twitch', 'Varus', 'Vayne', 'Xayah', 'Zeri'}
    SUP = {'Alistar', 'Bard', 'Blitzcrank', 'Braum', 'Janna', 'Karma', 'Leona', 'Lulu',
           'Milio', 'Morgana', 'Nami', 'Nautilus', 'Pyke', 'Rakan', 'Rell', 'Renata Glasc',
           'Seraphine', 'Sona', 'Soraka', 'Taric', 'Thresh', 'Yuumi', 'Zilean', 'Zyra'}
    
    if champion_name in TOP: return 'TOP'
    elif champion_name in JNG: return 'JNG'
    elif champion_name in MID: return 'MID'
    elif champion_name in ADC: return 'ADC'
    elif champion_name in SUP: return 'SUP'
    return 'UNKNOWN'


def extract_and_export(raw_data: Dict[str, Dict]):
    """Extract entities from raw data and export to CSVs."""
    
    teams = {}
    players = {}
    champions = {}
    series_list = []
    games_list = []
    draft_actions_list = []
    player_game_stats_list = []
    player_teams = defaultdict(lambda: {"team_id": None, "team_name": None})
    
    for series_id, response in raw_data.items():
        if "errors" in response or not response.get("data", {}).get("seriesState"):
            continue
        
        series = response["data"]["seriesState"]
        version = series.get("version", "3.0")
        
        for team in series.get("teams", []):
            teams[team["id"]] = {"id": team["id"], "name": team["name"]}
        
        blue_team_id = red_team_id = None
        for game in series.get("games", []):
            for team in game.get("teams", []):
                if team.get("side") == "blue": blue_team_id = team["id"]
                elif team.get("side") == "red": red_team_id = team["id"]
        
        series_list.append({
            "id": series_id,
            "tournament_id": None,
            "blue_team_id": blue_team_id,
            "red_team_id": red_team_id,
            "format": series.get("format", ""),
            "match_date": series.get("startedAt", ""),
            "started": series.get("started", False),
            "finished": series.get("finished", False),
            "schema_version": version
        })
        
        for game in series.get("games", []):
            game_id = game.get("id", "")
            game_winner_id = None
            for team in game.get("teams", []):
                if team.get("won"): game_winner_id = team["id"]
            
            games_list.append({
                "id": game_id,
                "series_id": series_id,
                "game_number": game.get("sequenceNumber", 0),
                "winner_team_id": game_winner_id,
                "duration_seconds": game.get("clock", {}).get("currentSeconds", 0),
                "patch_version": game.get("titleVersion", {}).get("name") if game.get("titleVersion") else None
            })
            
            for action in game.get("draftActions", []):
                champ_id = action.get("draftable", {}).get("id", "")
                champ_name = action.get("draftable", {}).get("name", "")
                if champ_id:
                    champions[champ_id] = {"id": champ_id, "name": champ_name}
                
                draft_actions_list.append({
                    "game_id": game_id,
                    "series_id": series_id,
                    "sequence_number": action.get("sequenceNumber", ""),
                    "action_type": action.get("type", ""),
                    "team_id": action.get("drafter", {}).get("id", ""),
                    "champion_id": champ_id,
                    "champion_name": champ_name
                })
            
            for team in game.get("teams", []):
                team_id = team.get("id", "")
                team_kills = team.get("kills", 0)
                
                for player in team.get("players", []):
                    player_id = player.get("id", "")
                    champ_id = player.get("character", {}).get("id", "")
                    champ_name = player.get("character", {}).get("name", "")
                    
                    players[player_id] = {"id": player_id, "name": player.get("name", "")}
                    player_teams[player_id] = {"team_id": team_id, "team_name": team.get("name", "")}
                    
                    if champ_id:
                        champions[champ_id] = {"id": champ_id, "name": champ_name}
                    
                    kills = player.get("kills", 0)
                    deaths = player.get("deaths", 0)
                    assists = player.get("killAssistsGiven", 0)
                    
                    roles = player.get("roles", [])
                    role = roles[0].get("id", "UNKNOWN") if roles else infer_role_from_champion(champ_name)
                    
                    kda = player.get("kdaRatio") or ((kills + assists) / max(deaths, 1))
                    kp = player.get("killParticipation")
                    if kp is None and team_kills > 0:
                        kp = ((kills + assists) / team_kills) * 100
                    
                    player_game_stats_list.append({
                        "game_id": game_id,
                        "series_id": series_id,
                        "player_id": player_id,
                        "player_name": player.get("name", ""),
                        "team_id": team_id,
                        "team_side": team.get("side", ""),
                        "team_won": team.get("won", False),
                        "champion_id": champ_id,
                        "champion_name": champ_name,
                        "role": role,
                        "kills": kills,
                        "deaths": deaths,
                        "assists": assists,
                        "kda_ratio": round(kda, 2) if kda else None,
                        "kill_participation": round(kp, 2) if kp else None,
                        "damage_dealt": player.get("damageDealt"),
                        "experience_points": player.get("experiencePoints"),
                        "vision_score": round(player.get("visionScore"), 2) if player.get("visionScore") else None,
                        "first_kill": player.get("firstKill"),
                        "team_first_kill": team.get("firstKill")
                    })
    
    for pid, p in players.items():
        p["team_id"] = player_teams[pid]["team_id"]
        p["team_name"] = player_teams[pid]["team_name"]
    
    def write_csv(data, filename, fieldnames):
        if not data:
            log(f"  No data for {filename}")
            return
        filepath = OUTPUT_DIR / filename
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)
        log(f"  {filename}: {len(data)} rows")
    
    log("Exporting CSVs...")
    write_csv(list(teams.values()), "teams.csv", ["id", "name"])
    write_csv(list(players.values()), "players.csv", ["id", "name", "team_id", "team_name"])
    write_csv(list(champions.values()), "champions.csv", ["id", "name"])
    write_csv(series_list, "series.csv", 
              ["id", "tournament_id", "blue_team_id", "red_team_id", "format", "match_date", "started", "finished", "schema_version"])
    write_csv(games_list, "games.csv",
              ["id", "series_id", "game_number", "winner_team_id", "duration_seconds", "patch_version"])
    write_csv(draft_actions_list, "draft_actions.csv",
              ["game_id", "series_id", "sequence_number", "action_type", "team_id", "champion_id", "champion_name"])
    write_csv(player_game_stats_list, "player_game_stats.csv",
              ["game_id", "series_id", "player_id", "player_name", "team_id", "team_side", "team_won",
               "champion_id", "champion_name", "role", "kills", "deaths", "assists", "kda_ratio",
               "kill_participation", "damage_dealt", "experience_points", "vision_score", "first_kill", "team_first_kill"])
    
    return {
        "teams": len(teams),
        "players": len(players),
        "champions": len(champions),
        "series": len(series_list),
        "games": len(games_list),
        "draft_actions": len(draft_actions_list),
        "player_game_stats": len(player_game_stats_list)
    }


# =============================================================================
# Main Commands
# =============================================================================

def cmd_status():
    """Show current progress status."""
    progress = load_progress()
    
    print("\n=== GRID Data Fetch Status ===\n")
    
    completed = len(progress.get("completed", {}))
    failed = len(progress.get("failed", {}))
    
    print(f"Completed: {completed}")
    print(f"Failed: {failed}")
    
    if progress.get("started_at"):
        print(f"Started: {progress['started_at']}")
    if progress.get("last_updated"):
        print(f"Last update: {progress['last_updated']}")
    
    if progress.get("version_stats"):
        print(f"\nVersion distribution:")
        for v, count in sorted(progress["version_stats"].items()):
            print(f"  {v}: {count}")
    
    series_files = list(BATCHES_DIR.glob("series_*.json")) if BATCHES_DIR.exists() else []
    print(f"\nSeries files saved: {len(series_files)}")
    
    if Path(INPUT_CSV).exists():
        all_ids = load_series_ids(INPUT_CSV)
        remaining = len(all_ids) - completed - failed
        print(f"\nTotal series in input: {len(all_ids)}")
        print(f"Remaining: {remaining}")
        
        if remaining > 0:
            est_seconds = remaining * 2 * DELAY
            print(f"Estimated time remaining: {format_duration(est_seconds)}")


def cmd_reset():
    """Reset all progress and data."""
    print("Resetting all progress...")
    
    if PROGRESS_FILE.exists():
        PROGRESS_FILE.unlink()
        print(f"  Deleted {PROGRESS_FILE}")
    
    if LOG_FILE.exists():
        LOG_FILE.unlink()
        print(f"  Deleted {LOG_FILE}")
    
    series_files = list(BATCHES_DIR.glob("series_*.json")) if BATCHES_DIR.exists() else []
    for sf in series_files:
        sf.unlink()
    print(f"  Deleted {len(series_files)} series files")
    
    print("Reset complete.")


def cmd_export():
    """Export all fetched data to CSVs."""
    print("Loading all series data...")
    raw_data = load_all_series_data()
    print(f"Loaded {len(raw_data)} series")
    
    if not raw_data:
        print("No data to export!")
        return
    
    stats = extract_and_export(raw_data)
    
    print("\n=== Export Summary ===")
    for key, value in stats.items():
        print(f"  {key}: {value}")


def cmd_fetch(limit: Optional[int] = None, series_ids: Optional[List[str]] = None, batch_size: int = 50):
    """Fetch series data.
    
    Args:
        limit: Fetch first N series from CSV
        series_ids: Fetch specific series IDs (ignores CSV if provided)
        batch_size: Progress update frequency
    """
    setup_directories()
    
    log("=== GRID API Series Fetcher v3 (Resilient) ===")
    
    # Determine which series to fetch
    if series_ids:
        # Use explicitly provided series IDs
        all_series_ids = series_ids
        log(f"Fetching {len(all_series_ids)} specific series IDs")
    else:
        # Load from CSV
        all_series_ids = load_series_ids(INPUT_CSV)
        if limit:
            all_series_ids = all_series_ids[:limit]
            log(f"Fetching first {limit} series from CSV")
    
    total = len(all_series_ids)
    log(f"Total series to process: {total}")
    
    progress = load_progress()
    if not progress.get("started_at"):
        progress["started_at"] = datetime.now().isoformat()
    
    completed = set(progress.get("completed", {}).keys())
    failed = set(progress.get("failed", {}).keys())
    
    remaining_ids = [sid for sid in all_series_ids if sid not in completed and sid not in failed]
    
    log(f"Already completed: {len(completed)}")
    log(f"Already failed: {len(failed)}")
    log(f"Remaining: {len(remaining_ids)}")
    
    if not remaining_ids:
        log("All series already processed!")
        return
    
    est_seconds = len(remaining_ids) * 2 * DELAY
    log(f"Estimated time: {format_duration(est_seconds)}")
    log("-" * 50)
    
    session = create_session()
    
    total_success = 0
    total_fail = 0
    start_time = time.time()
    
    for i, series_id in enumerate(remaining_ids):
        success, message = process_series(series_id, session, progress)
        
        if success:
            progress["completed"][series_id] = str(BATCHES_DIR / f"series_{series_id}.json")
            total_success += 1
            log(f"[{i+1}/{len(remaining_ids)}] {series_id}: OK - {message}")
        else:
            progress["failed"][series_id] = message
            total_fail += 1
            log(f"[{i+1}/{len(remaining_ids)}] {series_id}: FAILED - {message}")
        
        # Save progress after EVERY series
        save_progress(progress)
        
        # Progress update every batch_size series
        if (i + 1) % batch_size == 0:
            elapsed = time.time() - start_time
            processed = total_success + total_fail
            if processed > 0:
                rate = processed / elapsed * 60
                remaining = len(remaining_ids) - processed
                eta_seconds = remaining / rate * 60 if rate > 0 else 0
                log(f"--- Checkpoint: {processed}/{len(remaining_ids)} processed, "
                    f"Rate: {rate:.1f}/min, ETA: {format_duration(eta_seconds)} ---")
    
    log("\n" + "=" * 50)
    log("=== Fetch Complete ===")
    log(f"Total success: {total_success}")
    log(f"Total failed: {total_fail}")
    log(f"Total time: {format_duration(time.time() - start_time)}")
    
    if progress.get("version_stats"):
        log("Version distribution:")
        for v, count in sorted(progress["version_stats"].items()):
            log(f"  {v}: {count}")


# =============================================================================
# Entry Point
# =============================================================================

def main():
    global API_KEY, INPUT_CSV
    
    parser = argparse.ArgumentParser(description="GRID LoL Series Data Fetcher v3")
    parser.add_argument("--limit", type=int, help="Fetch first N series from CSV")
    parser.add_argument("--series", type=str, help="Comma-separated list of specific series IDs to fetch")
    parser.add_argument("--api-key", type=str, help="API key (required, or set GRID_API_KEY env var)")
    parser.add_argument("--input", type=str, help="Input CSV file path (overrides default)")
    parser.add_argument("--batch-size", type=int, default=50, help="Progress update frequency (default: 50)")
    parser.add_argument("--status", action="store_true", help="Show current progress status")
    parser.add_argument("--reset", action="store_true", help="Reset all progress and start fresh")
    parser.add_argument("--export", action="store_true", help="Export fetched data to CSVs")
    
    args = parser.parse_args()
    
    # Apply configuration overrides
    if args.api_key:
        API_KEY = args.api_key
    if args.input:
        INPUT_CSV = args.input
    
    # Parse series IDs if provided
    series_ids = None
    if args.series:
        series_ids = [s.strip() for s in args.series.split(",") if s.strip()]
    
    # Commands that don't require API key
    if args.status:
        cmd_status()
        return
    elif args.reset:
        cmd_reset()
        return
    elif args.export:
        cmd_export()
        return
    
    # Fetching requires API key
    if not API_KEY:
        print("ERROR: API key required. Provide via --api-key or set GRID_API_KEY environment variable.")
        sys.exit(1)
    
    try:
        cmd_fetch(limit=args.limit, series_ids=series_ids, batch_size=args.batch_size)
    except KeyboardInterrupt:
        log("\n\nInterrupted by user. Progress has been saved.")
        log("Run again to resume from last checkpoint.")
        sys.exit(0)


if __name__ == "__main__":
    main()