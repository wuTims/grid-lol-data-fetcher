# Schema Mapping Reference

This document maps GRID API fields to output CSV columns and target database schemas.

## API Response → CSV Mapping

### Series Level

| API Path | CSV Column | Type | Notes |
|----------|------------|------|-------|
| `seriesState.id` | series.id | TEXT | Primary key |
| `seriesState.format` | series.format | TEXT | best-of-1/3/5 |
| `seriesState.startedAt` | series.match_date | TEXT | ISO 8601 |
| `seriesState.started` | series.started | BOOL | |
| `seriesState.finished` | series.finished | BOOL | |
| `seriesState.games[].teams[].side` | series.blue_team_id | TEXT | Where side="blue" |
| `seriesState.games[].teams[].side` | series.red_team_id | TEXT | Where side="red" |
| — | series.tournament_id | TEXT | Not in API |

### Game Level

| API Path | CSV Column | Type | Notes |
|----------|------------|------|-------|
| `games[].id` | games.id | TEXT | UUID |
| `games[].sequenceNumber` | games.game_number | INT | 1-indexed |
| `games[].clock.currentSeconds` | games.duration_seconds | INT | Final game time |
| `games[].teams[].won` | games.winner_team_id | TEXT | Where won=true |
| — | games.patch_version | TEXT | Not in API |

### Draft Actions

| API Path | CSV Column | Type | Notes |
|----------|------------|------|-------|
| `draftActions[].sequenceNumber` | draft_actions.sequence_number | INT | 1-20 |
| `draftActions[].type` | draft_actions.action_type | TEXT | "ban" or "pick" |
| `draftActions[].drafter.id` | draft_actions.team_id | TEXT | |
| `draftActions[].draftable.id` | draft_actions.champion_id | TEXT | UUID |
| `draftActions[].draftable.name` | draft_actions.champion_name | TEXT | |

### Player Stats

| API Path | CSV Column | Type | Notes |
|----------|------------|------|-------|
| `players[].id` | player_game_stats.player_id | TEXT | |
| `players[].name` | player_game_stats.player_name | TEXT | |
| `players[].character.id` | player_game_stats.champion_id | TEXT | |
| `players[].character.name` | player_game_stats.champion_name | TEXT | |
| `players[].kills` | player_game_stats.kills | INT | |
| `players[].deaths` | player_game_stats.deaths | INT | |
| `players[].killAssistsGiven` | player_game_stats.assists | INT | |
| `teams[].side` | player_game_stats.team_side | TEXT | |
| `teams[].won` | player_game_stats.team_won | BOOL | |
| — | player_game_stats.role | TEXT | Inferred |
| — | player_game_stats.damage_dealt | INT | Not in API |
| — | player_game_stats.gold_earned | INT | Not in API |
| — | player_game_stats.vision_score | FLOAT | Not in API |
| — | player_game_stats.cs | INT | Not in API |
| — | player_game_stats.first_blood | BOOL | Not in API |

## Calculated Fields

### KDA Ratio
```python
kda_ratio = (kills + assists) / max(deaths, 1)
```

### Kill Participation
```python
kill_participation = (kills + assists) / team_total_kills
```

## Target Database Schema (SQLite)

### Core Tables

```sql
CREATE TABLE teams (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    short_name TEXT,
    region TEXT
);

CREATE TABLE players (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    team_id TEXT REFERENCES teams(id),
    role TEXT
);

CREATE TABLE champions (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    riot_key TEXT,
    rework_date TEXT
);

CREATE TABLE series (
    id TEXT PRIMARY KEY,
    tournament_id TEXT,
    blue_team_id TEXT REFERENCES teams(id),
    red_team_id TEXT REFERENCES teams(id),
    match_date TEXT NOT NULL,
    description TEXT
);

CREATE TABLE games (
    id TEXT PRIMARY KEY,
    series_id TEXT REFERENCES series(id),
    game_number INTEGER,
    winner_team_id TEXT REFERENCES teams(id),
    duration_seconds INTEGER,
    patch_version TEXT
);

CREATE TABLE draft_actions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id TEXT REFERENCES games(id),
    sequence_number INTEGER,
    action_type TEXT NOT NULL,
    team_id TEXT REFERENCES teams(id),
    champion_id TEXT REFERENCES champions(id)
);

CREATE TABLE player_game_stats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id TEXT REFERENCES games(id),
    player_id TEXT REFERENCES players(id),
    team_id TEXT REFERENCES teams(id),
    champion_id TEXT REFERENCES champions(id),
    role TEXT,
    kills INTEGER,
    deaths INTEGER,
    assists INTEGER,
    damage_dealt INTEGER,
    gold_earned INTEGER,
    vision_score REAL,
    cs INTEGER,
    kda_ratio REAL,
    kill_participation REAL,
    first_blood BOOLEAN
);
```

### Indexes

```sql
CREATE INDEX idx_draft_actions_game ON draft_actions(game_id);
CREATE INDEX idx_player_stats_game ON player_game_stats(game_id);
CREATE INDEX idx_player_stats_player ON player_game_stats(player_id);
CREATE INDEX idx_series_date ON series(match_date);
```

## Data Type Conversions

| Source Type | CSV Type | SQLite Type | Python Type |
|-------------|----------|-------------|-------------|
| ID string | TEXT | TEXT | str |
| Boolean | TEXT (True/False) | INTEGER (0/1) | bool |
| Integer | TEXT | INTEGER | int |
| Float | TEXT | REAL | float |
| Timestamp | TEXT (ISO 8601) | TEXT | str |
| Null | Empty string | NULL | None |

## CSV Import to SQLite

```python
import sqlite3
import csv

def import_csv_to_sqlite(csv_path: str, table_name: str, db_path: str):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        columns = reader.fieldnames
        
        placeholders = ','.join(['?' for _ in columns])
        column_names = ','.join(columns)
        
        for row in reader:
            values = [row[col] if row[col] != '' else None for col in columns]
            cursor.execute(
                f"INSERT OR REPLACE INTO {table_name} ({column_names}) VALUES ({placeholders})",
                values
            )
    
    conn.commit()
    conn.close()
```
