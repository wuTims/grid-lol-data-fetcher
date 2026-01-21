# GRID LoL Data Fetcher Skill

Fetch and process League of Legends professional match data from the GRID Open Access API for analytics, draft analysis, and machine learning applications.

## Installation

### Claude Code

```bash
# Clone and install to personal skills (available in all projects)
git clone https://github.com/youruser/grid-lol-data-skill.git
cp -r grid-lol-data-skill ~/.claude/skills/

# Or install to a specific project
cp -r grid-lol-data-skill /path/to/project/.claude/skills/
```

### Claude.ai

1. Download or clone this repository
2. Zip the folder:
   ```bash
   zip -r grid-lol-data-skill.zip grid-lol-data-skill/
   ```
3. Go to **Settings → Features → Upload Custom Skill**

### Cursor

Copy key instructions from `SKILL.md` into `.cursorrules` or `.cursor/rules/grid-lol-data.mdc`

### GitHub Copilot

Copy key instructions from `SKILL.md` into `.github/copilot-instructions.md`

## Requirements

- Python 3.8+
- `requests` library (`pip install requests`)
- GRID API key (obtain from [GRID Open Access program](https://grid.gg/))

## Quick Start

```bash
# Set API key
export GRID_API_KEY=your_api_key

# Fetch specific series
python scripts/fetch_lol_series_v3.py --series 2847564,2847565

# Fetch from CSV with limit
python scripts/fetch_lol_series_v3.py --input series.csv --limit 100

# Resume interrupted run
python scripts/fetch_lol_series_v3.py --run latest

# Export to CSVs
python scripts/fetch_lol_series_v3.py --export --run latest
```

## Documentation

See [SKILL.md](SKILL.md) for full documentation including:
- API configuration and rate limits
- All CLI options
- Output CSV schemas
- Usage examples

## License

MIT
