#!/usr/bin/env python3
"""
GRID API Data Fetcher for LoL Draft Assistant (v3 - Resilient)

Features:
- Run-based organization: each fetch creates an isolated run folder
- Saves after EVERY successful series fetch (no data loss)
- Batch processing with configurable batch sizes
- Detailed logging with timestamps
- Separate raw JSON files per series (prevents large file corruption)
- Easy resume via --run latest or --run <name>
- Progress summary and ETA tracking

Usage:
    # Create new run (default behavior)
    python fetch_lol_series_v3.py --api-key KEY --limit 100

    # Continue most recent run
    python fetch_lol_series_v3.py --api-key KEY --run latest

    # Use/continue named run
    python fetch_lol_series_v3.py --api-key KEY --run "lec_spring_2025"

    # Fetch specific series IDs
    python fetch_lol_series_v3.py --api-key KEY --series 123,456

    # List all runs
    python fetch_lol_series_v3.py --list-runs

    # Check status of a run
    python fetch_lol_series_v3.py --status --run latest

    # Export a run to CSVs
    python fetch_lol_series_v3.py --export --run latest

    # Reset a specific run
    python fetch_lol_series_v3.py --reset --run "lec_spring_2025"
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

# Default paths (can be overridden via CLI)
DEFAULT_OUTPUT_DIR = Path("./outputs")
DEFAULT_INPUT_CSV = None  # No default - must provide via --input or --series

# Runtime config (set by CLI args or environment, then used to derive paths)
API_KEY = os.environ.get("GRID_API_KEY", "")
OUTPUT_DIR: Path = DEFAULT_OUTPUT_DIR
RUN_DIR: Optional[Path] = None  # Set during run initialization
RAW_DIR: Optional[Path] = None
CSV_DIR: Optional[Path] = None
PROGRESS_FILE: Optional[Path] = None
LOG_FILE: Optional[Path] = None
RUN_CONFIG_FILE: Optional[Path] = None

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
# Run Management Functions
# =============================================================================

def generate_run_id() -> str:
    """Generate a timestamp-based run ID."""
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def init_run_paths(run_id: str):
    """Initialize global path variables for a specific run."""
    global RUN_DIR, RAW_DIR, CSV_DIR, PROGRESS_FILE, LOG_FILE, RUN_CONFIG_FILE

    RUN_DIR = OUTPUT_DIR / run_id
    RAW_DIR = RUN_DIR / "raw"
    CSV_DIR = RUN_DIR / "csv"
    PROGRESS_FILE = RUN_DIR / "progress.json"
    LOG_FILE = RUN_DIR / "fetch.log"
    RUN_CONFIG_FILE = RUN_DIR / "run_config.json"


def setup_directories():
    """Create necessary directories for the current run."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    if RUN_DIR:
        RUN_DIR.mkdir(parents=True, exist_ok=True)
        RAW_DIR.mkdir(parents=True, exist_ok=True)
        CSV_DIR.mkdir(parents=True, exist_ok=True)


def get_all_runs() -> List[Dict]:
    """Get all runs with their metadata, sorted by creation time (newest first)."""
    runs = []
    if not OUTPUT_DIR.exists():
        return runs

    for run_dir in OUTPUT_DIR.iterdir():
        if not run_dir.is_dir():
            continue
        config_file = run_dir / "run_config.json"
        progress_file = run_dir / "progress.json"

        run_info = {
            "run_id": run_dir.name,
            "path": str(run_dir),
            "created_at": None,
            "completed": 0,
            "failed": 0,
        }

        if config_file.exists():
            with open(config_file, 'r') as f:
                config = json.load(f)
                run_info["created_at"] = config.get("created_at")
                run_info["input_source"] = config.get("input_source")

        if progress_file.exists():
            with open(progress_file, 'r') as f:
                progress = json.load(f)
                run_info["completed"] = len(progress.get("completed", {}))
                run_info["failed"] = len(progress.get("failed", {}))
                run_info["last_updated"] = progress.get("last_updated")

        runs.append(run_info)

    # Sort by created_at descending (newest first), handling None
    runs.sort(key=lambda x: x.get("created_at") or "", reverse=True)
    return runs


def get_latest_run() -> Optional[str]:
    """Get the most recent run ID."""
    runs = get_all_runs()
    return runs[0]["run_id"] if runs else None


def run_exists(run_id: str) -> bool:
    """Check if a run exists."""
    return (OUTPUT_DIR / run_id).is_dir()


def save_run_config(input_source: str, series_ids: Optional[List[str]] = None,
                    limit: Optional[int] = None):
    """Save run configuration to run_config.json."""
    config = {
        "run_id": RUN_DIR.name,
        "created_at": datetime.now().isoformat(),
        "input_source": input_source,
        "series_ids": series_ids,
        "limit": limit,
        "api_endpoint": API_URL,
    }
    with open(RUN_CONFIG_FILE, 'w') as f:
        json.dump(config, f, indent=2)


def load_run_config() -> Optional[Dict]:
    """Load run configuration from run_config.json."""
    if RUN_CONFIG_FILE and RUN_CONFIG_FILE.exists():
        with open(RUN_CONFIG_FILE, 'r') as f:
            return json.load(f)
    return None


# =============================================================================
# Utility Functions
# =============================================================================

def log(message: str, also_print: bool = True):
    """Log message to file and optionally print."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_line = f"[{timestamp}] {message}"

    if LOG_FILE:
        with open(LOG_FILE, 'a') as f:
            f.write(log_line + "\n")

    if also_print:
        print(log_line)


def load_progress() -> Dict:
    """Load progress from file."""
    if PROGRESS_FILE and PROGRESS_FILE.exists():
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
    series_file = RAW_DIR / f"series_{series_id}.json"
    with open(series_file, 'w') as f:
        json.dump(data, f)
    return str(series_file)


def load_all_series_data() -> Dict[str, Dict]:
    """Load all series files into a single dict from the current run."""
    all_data = {}
    if not RAW_DIR or not RAW_DIR.exists():
        return all_data

    series_files = RAW_DIR.glob("series_*.json")

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
        filepath = CSV_DIR / filename
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

def cmd_list_runs():
    """List all available runs."""
    runs = get_all_runs()

    if not runs:
        print("\nNo runs found.")
        print(f"Output directory: {OUTPUT_DIR.absolute()}")
        return

    print(f"\n=== Available Runs ({len(runs)}) ===\n")
    print(f"{'Run ID':<20} {'Created':<20} {'Completed':<10} {'Failed':<10} {'Last Updated':<20}")
    print("-" * 80)

    for run in runs:
        created = run.get("created_at", "")[:19] if run.get("created_at") else "N/A"
        updated = run.get("last_updated", "")[:19] if run.get("last_updated") else "N/A"
        print(f"{run['run_id']:<20} {created:<20} {run['completed']:<10} {run['failed']:<10} {updated:<20}")

    print(f"\nOutput directory: {OUTPUT_DIR.absolute()}")


def cmd_status():
    """Show current progress status for the active run."""
    if not RUN_DIR:
        print("ERROR: No run specified. Use --run <name> or --run latest")
        return

    if not RUN_DIR.exists():
        print(f"ERROR: Run '{RUN_DIR.name}' does not exist.")
        return

    config = load_run_config()
    progress = load_progress()

    print(f"\n=== Run Status: {RUN_DIR.name} ===\n")
    print(f"Path: {RUN_DIR}")

    if config:
        print(f"Created: {config.get('created_at', 'N/A')}")
        print(f"Input: {config.get('input_source', 'N/A')}")
        if config.get('limit'):
            print(f"Limit: {config['limit']}")

    completed = len(progress.get("completed", {}))
    failed = len(progress.get("failed", {}))

    print(f"\nCompleted: {completed}")
    print(f"Failed: {failed}")

    if progress.get("last_updated"):
        print(f"Last update: {progress['last_updated']}")

    if progress.get("version_stats"):
        print(f"\nVersion distribution:")
        for v, count in sorted(progress["version_stats"].items()):
            print(f"  {v}: {count}")

    series_files = list(RAW_DIR.glob("series_*.json")) if RAW_DIR and RAW_DIR.exists() else []
    print(f"\nSeries files saved: {len(series_files)}")

    # Check for exported CSVs
    if CSV_DIR and CSV_DIR.exists():
        csv_files = list(CSV_DIR.glob("*.csv"))
        if csv_files:
            print(f"CSV exports: {len(csv_files)} files")


def cmd_reset():
    """Reset the specified run (delete all data)."""
    if not RUN_DIR:
        print("ERROR: No run specified. Use --run <name> to specify which run to reset.")
        return

    if not RUN_DIR.exists():
        print(f"ERROR: Run '{RUN_DIR.name}' does not exist.")
        return

    print(f"Resetting run: {RUN_DIR.name}")

    import shutil
    shutil.rmtree(RUN_DIR)
    print(f"  Deleted {RUN_DIR}")

    print("Reset complete.")


def cmd_export():
    """Export all fetched data to CSVs for the active run."""
    if not RUN_DIR:
        print("ERROR: No run specified. Use --run <name> or --run latest")
        return

    if not RUN_DIR.exists():
        print(f"ERROR: Run '{RUN_DIR.name}' does not exist.")
        return

    # Ensure CSV directory exists
    CSV_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Exporting run: {RUN_DIR.name}")
    print("Loading all series data...")
    raw_data = load_all_series_data()
    print(f"Loaded {len(raw_data)} series")

    if not raw_data:
        print("No data to export!")
        return

    stats = extract_and_export(raw_data)

    print(f"\n=== Export Summary ===")
    print(f"Output directory: {CSV_DIR}")
    for key, value in stats.items():
        print(f"  {key}: {value}")


def cmd_fetch(input_csv: Optional[str] = None, limit: Optional[int] = None,
               series_ids: Optional[List[str]] = None, batch_size: int = 50,
               is_new_run: bool = True):
    """Fetch series data.

    Args:
        input_csv: Path to input CSV file
        limit: Fetch first N series from CSV
        series_ids: Fetch specific series IDs (ignores CSV if provided)
        batch_size: Progress update frequency
        is_new_run: Whether this is a new run (save config) or continuing existing
    """
    setup_directories()

    # Save run config for new runs
    if is_new_run:
        if series_ids:
            save_run_config("explicit_series_ids", series_ids=series_ids, limit=limit)
        elif input_csv:
            save_run_config(input_csv, limit=limit)
        else:
            print("ERROR: Must provide --input CSV file or --series IDs")
            sys.exit(1)

    log(f"=== GRID API Series Fetcher v3 ===")
    log(f"Run: {RUN_DIR.name}")

    # Determine which series to fetch
    if series_ids:
        all_series_ids = series_ids
        log(f"Fetching {len(all_series_ids)} specific series IDs")
    elif input_csv:
        all_series_ids = load_series_ids(input_csv)
        if limit:
            all_series_ids = all_series_ids[:limit]
            log(f"Fetching first {limit} series from CSV: {input_csv}")
        else:
            log(f"Fetching all {len(all_series_ids)} series from CSV: {input_csv}")
    else:
        # Continuing existing run - load config
        config = load_run_config()
        if not config:
            print("ERROR: No run config found. Cannot determine what to fetch.")
            sys.exit(1)

        if config.get("series_ids"):
            all_series_ids = config["series_ids"]
            log(f"Continuing run with {len(all_series_ids)} specific series IDs")
        elif config.get("input_source") and config["input_source"] != "explicit_series_ids":
            input_csv = config["input_source"]
            all_series_ids = load_series_ids(input_csv)
            if config.get("limit"):
                all_series_ids = all_series_ids[:config["limit"]]
            log(f"Continuing run from CSV: {input_csv}")
        else:
            print("ERROR: Invalid run config. Cannot determine what to fetch.")
            sys.exit(1)

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
            progress["completed"][series_id] = str(RAW_DIR / f"series_{series_id}.json")
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

    log(f"\nRun directory: {RUN_DIR}")
    log(f"To export: python {sys.argv[0]} --export --run {RUN_DIR.name}")


# =============================================================================
# Interactive Confirmation
# =============================================================================

def get_series_count(input_csv: Optional[str], series_ids: Optional[List[str]],
                     limit: Optional[int]) -> Tuple[int, str]:
    """Get the number of series that will be fetched and describe the source."""
    if series_ids:
        return len(series_ids), "explicit series IDs"
    elif input_csv:
        try:
            all_ids = load_series_ids(input_csv)
            total = len(all_ids)
            if limit:
                return min(limit, total), f"{input_csv} (limited from {total})"
            return total, input_csv
        except Exception as e:
            return 0, f"{input_csv} (error: {e})"
    return 0, "unknown"


def estimate_time(series_count: int) -> str:
    """Estimate fetch time based on series count."""
    # 2 API calls per series (version check + data), 3.1s delay each
    seconds = series_count * 2 * DELAY
    if seconds < 60:
        return f"{seconds:.0f} seconds"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f} minutes"
    else:
        hours = seconds / 3600
        return f"{hours:.1f} hours"


def show_run_preview(run_id: str, input_csv: Optional[str],
                     series_ids: Optional[List[str]], limit: Optional[int],
                     is_new_run: bool, batch_size: int = 50):
    """Display a preview of what the run will do."""
    series_count, source_desc = get_series_count(input_csv, series_ids, limit)
    est_time = estimate_time(series_count)

    run_status = "new" if is_new_run else "resume"

    print()
    print("═" * 65)
    print("  GRID Data Fetcher - Run Preview")
    print("═" * 65)
    print()
    print(f"  Run ID:         {run_id} ({run_status})")
    print(f"  Output:         {OUTPUT_DIR / run_id}/")
    print()
    print(f"  Data Source:    {source_desc}")
    print(f"  Series Count:   {series_count:,}")
    print(f"  Estimated Time: {est_time}")
    print()
    print("  Output Structure:")
    print(f"    {OUTPUT_DIR / run_id}/")
    print("    ├── run_config.json")
    print("    ├── progress.json")
    print("    ├── fetch.log")
    print("    ├── raw/")
    print(f"    │   └── series_*.json ({series_count:,} files)")
    print("    └── csv/           (after --export)")
    print()
    print("═" * 65)
    print("  Options You May Want to Configure:")
    print("═" * 65)
    print()
    print(f"  --output DIR     Base output directory (current: {OUTPUT_DIR})")
    print(f"  --run NAME       Custom run name (current: {run_id})")
    print(f"  --limit N        Limit series count (current: {limit or 'none'})")
    print(f"  --batch-size N   Progress frequency (current: {batch_size})")
    print()
    print("═" * 65)


def show_reset_preview(run_id: str):
    """Display a preview of what reset will delete."""
    print()
    print("═" * 65)
    print("  GRID Data Fetcher - Reset Preview")
    print("═" * 65)
    print()
    print(f"  Run to delete:  {run_id}")
    print(f"  Path:           {RUN_DIR}")
    print()

    # Count files that will be deleted
    if RUN_DIR and RUN_DIR.exists():
        raw_files = list((RUN_DIR / "raw").glob("series_*.json")) if (RUN_DIR / "raw").exists() else []
        csv_files = list((RUN_DIR / "csv").glob("*.csv")) if (RUN_DIR / "csv").exists() else []
        print(f"  Files to delete:")
        print(f"    - {len(raw_files)} raw series JSON files")
        print(f"    - {len(csv_files)} exported CSV files")
        print(f"    - Configuration and log files")
    print()
    print("  ⚠️  This action cannot be undone!")
    print()
    print("═" * 65)


def prompt_confirmation(action: str = "Proceed") -> bool:
    """Prompt user for Y/n confirmation. Returns True if confirmed."""
    try:
        response = input(f"\n{action}? [Y/n]: ").strip().lower()
        return response in ("", "y", "yes")
    except (EOFError, KeyboardInterrupt):
        print()
        return False


# =============================================================================
# Entry Point
# =============================================================================

def main():
    global API_KEY, OUTPUT_DIR

    parser = argparse.ArgumentParser(
        description="GRID LoL Series Data Fetcher v3 - Run-based data fetching",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Create new run fetching specific series
  python fetch_lol_series_v3.py --api-key KEY --series 123,456,789

  # Create new run from CSV
  python fetch_lol_series_v3.py --api-key KEY --input data.csv --limit 100

  # Continue most recent run
  python fetch_lol_series_v3.py --api-key KEY --run latest

  # Continue a named run
  python fetch_lol_series_v3.py --api-key KEY --run my_run_name

  # List all runs
  python fetch_lol_series_v3.py --list-runs

  # Check status of a run
  python fetch_lol_series_v3.py --status --run latest

  # Export run data to CSVs
  python fetch_lol_series_v3.py --export --run latest
        """
    )

    # Run management
    parser.add_argument("--output", type=str, default="./outputs",
                        help="Base output directory (default: ./outputs)")
    parser.add_argument("--run", type=str,
                        help="Run name: 'latest' for most recent, or custom name to create/continue")
    parser.add_argument("--list-runs", action="store_true",
                        help="List all available runs")

    # Data source
    parser.add_argument("--input", type=str,
                        help="Input CSV file with SeriesID column")
    parser.add_argument("--series", type=str,
                        help="Comma-separated list of specific series IDs to fetch")
    parser.add_argument("--limit", type=int,
                        help="Fetch first N series from CSV")

    # Fetching options
    parser.add_argument("--api-key", type=str,
                        help="API key (or set GRID_API_KEY env var)")
    parser.add_argument("--batch-size", type=int, default=50,
                        help="Progress update frequency (default: 50)")

    # Commands
    parser.add_argument("--status", action="store_true",
                        help="Show progress status for a run")
    parser.add_argument("--reset", action="store_true",
                        help="Delete a run and all its data")
    parser.add_argument("--export", action="store_true",
                        help="Export run data to CSVs")

    # Confirmation options
    parser.add_argument("--yes", "-y", action="store_true",
                        help="Skip confirmation prompts (for automation)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would happen without executing")

    args = parser.parse_args()

    # Set output directory
    OUTPUT_DIR = Path(args.output)

    # Apply API key
    if args.api_key:
        API_KEY = args.api_key

    # Parse series IDs if provided
    series_ids = None
    if args.series:
        series_ids = [s.strip() for s in args.series.split(",") if s.strip()]

    # Handle --list-runs (doesn't need a run)
    if args.list_runs:
        cmd_list_runs()
        return

    # Determine run ID for commands that need it
    run_id = None
    is_new_run = False

    if args.run:
        if args.run.lower() == "latest":
            run_id = get_latest_run()
            if not run_id:
                print("ERROR: No existing runs found. Create a new run first.")
                sys.exit(1)
            is_new_run = False
        else:
            run_id = args.run
            is_new_run = not run_exists(run_id)
    elif args.status or args.reset or args.export:
        # These commands require --run
        print("ERROR: Must specify --run <name> or --run latest")
        sys.exit(1)
    else:
        # Fetching without --run creates a new timestamped run
        run_id = generate_run_id()
        is_new_run = True

    # Initialize paths for the run
    if run_id:
        init_run_paths(run_id)

    # Handle commands
    if args.status:
        cmd_status()
        return

    if args.reset:
        if not args.yes:
            show_reset_preview(run_id)
            if args.dry_run:
                print("Dry run - no changes made.")
                return
            if not prompt_confirmation("Delete this run"):
                print("\nAborted. Run not deleted.")
                return
        cmd_reset()
        return

    if args.export:
        cmd_export()
        return

    # Fetching requires API key
    if not API_KEY:
        print("ERROR: API key required. Provide via --api-key or set GRID_API_KEY environment variable.")
        sys.exit(1)

    # Validate input for new runs
    if is_new_run and not series_ids and not args.input:
        print("ERROR: New runs require --input CSV file or --series IDs")
        sys.exit(1)

    # Show preview and prompt for confirmation (unless --yes)
    if not args.yes:
        show_run_preview(
            run_id=run_id,
            input_csv=args.input,
            series_ids=series_ids,
            limit=args.limit,
            is_new_run=is_new_run,
            batch_size=args.batch_size
        )
        if args.dry_run:
            print("Dry run - no changes made.")
            return
        if not prompt_confirmation("Start fetching"):
            print("\nAborted. Adjust options and try again.")
            return

    try:
        cmd_fetch(
            input_csv=args.input,
            limit=args.limit,
            series_ids=series_ids,
            batch_size=args.batch_size,
            is_new_run=is_new_run
        )
    except KeyboardInterrupt:
        log("\n\nInterrupted by user. Progress has been saved.")
        log(f"To resume: python {sys.argv[0]} --api-key KEY --run {run_id}")
        sys.exit(0)


if __name__ == "__main__":
    main()