#!/usr/bin/env python3
"""
Fetch champion data from Riot Data Dragon CDN.
No API key required - this is public data.

Outputs:
- champions_datadragon.json: Full champion data
- champion_icon_mapping.csv: name -> riot_key -> icon_url mapping
"""

import json
import requests
import csv
from pathlib import Path

# Data Dragon CDN
VERSIONS_URL = "https://ddragon.leagueoflegends.com/api/versions.json"
CHAMPION_DATA_URL = "https://ddragon.leagueoflegends.com/cdn/{version}/data/en_US/champion.json"
CHAMPION_ICON_URL = "https://ddragon.leagueoflegends.com/cdn/{version}/img/champion/{riot_key}.png"

OUTPUT_DIR = "/mnt/user-data/outputs"


def get_latest_version() -> str:
    """Get the latest Data Dragon version."""
    response = requests.get(VERSIONS_URL)
    response.raise_for_status()
    versions = response.json()
    return versions[0]  # First is latest


def fetch_champion_data(version: str) -> dict:
    """Fetch champion data for a specific version."""
    url = CHAMPION_DATA_URL.format(version=version)
    response = requests.get(url)
    response.raise_for_status()
    return response.json()


def build_champion_mapping(champion_data: dict, version: str) -> list:
    """Build champion name to riot_key mapping with icon URLs."""
    mappings = []
    
    for riot_key, champ_info in champion_data["data"].items():
        # Handle special name cases
        display_name = champ_info["name"]
        
        mappings.append({
            "display_name": display_name,
            "riot_key": riot_key,
            "riot_id": champ_info["id"],  # Same as riot_key usually
            "title": champ_info["title"],
            "icon_url": CHAMPION_ICON_URL.format(version=version, riot_key=riot_key),
            "tags": "|".join(champ_info.get("tags", [])),
            "partype": champ_info.get("partype", ""),  # Mana, Energy, etc.
        })
    
    return sorted(mappings, key=lambda x: x["display_name"])


def create_grid_name_mapping(mappings: list) -> dict:
    """
    Create mapping from GRID champion names to riot_key.
    Handles edge cases like apostrophes and special characters.
    """
    grid_to_riot = {}
    
    for m in mappings:
        display_name = m["display_name"]
        riot_key = m["riot_key"]
        
        # Direct mapping
        grid_to_riot[display_name] = riot_key
        
        # Common GRID API variations
        # GRID sometimes uses slightly different names
        variations = [
            display_name.replace("'", ""),     # Kai'Sa -> KaiSa
            display_name.replace("'", "'"),    # Different apostrophe
            display_name.replace(" ", ""),     # Nunu & Willump -> NunuWillump
            display_name.replace("&", "and"),  # & -> and
        ]
        
        for var in variations:
            if var != display_name:
                grid_to_riot[var] = riot_key
    
    # Manual overrides for known mismatches
    manual_mappings = {
        "Nunu & Willump": "Nunu",
        "Renata Glasc": "Renata",
        "Wukong": "MonkeyKing",
    }
    grid_to_riot.update(manual_mappings)
    
    return grid_to_riot


def main():
    print("=== Data Dragon Champion Fetcher ===")
    
    # Get latest version
    print("Fetching latest version...")
    version = get_latest_version()
    print(f"Latest version: {version}")
    
    # Fetch champion data
    print("Fetching champion data...")
    champion_data = fetch_champion_data(version)
    total_champs = len(champion_data["data"])
    print(f"Found {total_champs} champions")
    
    # Build mapping
    print("Building champion mappings...")
    mappings = build_champion_mapping(champion_data, version)
    
    # Save full JSON
    json_path = f"{OUTPUT_DIR}/champions_datadragon.json"
    with open(json_path, 'w') as f:
        json.dump({
            "version": version,
            "champions": mappings
        }, f, indent=2)
    print(f"Saved: {json_path}")
    
    # Save CSV mapping
    csv_path = f"{OUTPUT_DIR}/champion_icon_mapping.csv"
    with open(csv_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=[
            "display_name", "riot_key", "riot_id", "title", "icon_url", "tags", "partype"
        ])
        writer.writeheader()
        writer.writerows(mappings)
    print(f"Saved: {csv_path}")
    
    # Save GRID name -> riot_key mapping
    grid_mapping = create_grid_name_mapping(mappings)
    grid_mapping_path = f"{OUTPUT_DIR}/grid_to_riot_key.json"
    with open(grid_mapping_path, 'w') as f:
        json.dump(grid_mapping, f, indent=2, sort_keys=True)
    print(f"Saved: {grid_mapping_path}")
    
    print(f"\n=== Summary ===")
    print(f"Data Dragon Version: {version}")
    print(f"Total Champions: {total_champs}")
    print(f"\nFiles created:")
    print(f"  - {json_path}")
    print(f"  - {csv_path}")
    print(f"  - {grid_mapping_path}")
    
    # Print example
    print(f"\n=== Example Mapping ===")
    examples = ["Kai'Sa", "K'Sante", "Rek'Sai", "Nunu & Willump", "Renata Glasc"]
    for name in examples:
        if name in grid_mapping:
            riot_key = grid_mapping[name]
            icon_url = CHAMPION_ICON_URL.format(version=version, riot_key=riot_key)
            print(f"  {name:20} -> {riot_key:15} -> {icon_url}")


if __name__ == "__main__":
    main()
