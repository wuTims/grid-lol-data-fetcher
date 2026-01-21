#!/bin/bash
# GRID API cURL Examples
# Replace YOUR_API_KEY with your actual GRID Open Access API key

API_KEY="YOUR_API_KEY"
API_URL="https://api-op.grid.gg/live-data-feed/series-state/graphql"

# =============================================================================
# Example 1: Check Schema Version (Lightweight)
# =============================================================================
echo "=== Example 1: Version Check ==="
curl -s -X POST "$API_URL" \
  -H "Content-Type: application/json" \
  -H "x-api-key: $API_KEY" \
  -d '{
    "query": "query VersionCheck($seriesId: ID!) { seriesState(id: $seriesId) { id version } }",
    "variables": {"seriesId": "2618419"}
  }' | jq .

# =============================================================================
# Example 2: Get Series Metadata Only
# =============================================================================
echo -e "\n=== Example 2: Series Metadata ==="
curl -s -X POST "$API_URL" \
  -H "Content-Type: application/json" \
  -H "x-api-key: $API_KEY" \
  -d '{
    "query": "query SeriesMeta($seriesId: ID!) { seriesState(id: $seriesId) { id format started finished startedAt teams { id name won score } } }",
    "variables": {"seriesId": "2618419"}
  }' | jq .

# =============================================================================
# Example 3: Get Full Series with Draft and Player Stats
# =============================================================================
echo -e "\n=== Example 3: Full Series Data ==="
curl -s -X POST "$API_URL" \
  -H "Content-Type: application/json" \
  -H "x-api-key: $API_KEY" \
  -d '{
    "query": "query SeriesState($seriesId: ID!) { seriesState(id: $seriesId) { id version title { nameShortened } format started finished startedAt teams { id name won score } games { id sequenceNumber started finished paused clock { currentSeconds ticking } map { name } draftActions { id sequenceNumber type drafter { id type } draftable { id type name } } teams { id name side won score kills deaths structuresDestroyed objectives { id type } players { id name participationStatus character { id name } kills deaths killAssistsGiven } } } } }",
    "variables": {"seriesId": "2618419"}
  }' | jq .

# =============================================================================
# Example 4: Get Draft Actions Only
# =============================================================================
echo -e "\n=== Example 4: Draft Actions Only ==="
curl -s -X POST "$API_URL" \
  -H "Content-Type: application/json" \
  -H "x-api-key: $API_KEY" \
  -d '{
    "query": "query DraftOnly($seriesId: ID!) { seriesState(id: $seriesId) { id games { id sequenceNumber draftActions { sequenceNumber type drafter { id } draftable { name } } } } }",
    "variables": {"seriesId": "2618419"}
  }' | jq '.data.seriesState.games[0].draftActions'

# =============================================================================
# Example 5: Get Player Stats for a Game
# =============================================================================
echo -e "\n=== Example 5: Player Stats ==="
curl -s -X POST "$API_URL" \
  -H "Content-Type: application/json" \
  -H "x-api-key: $API_KEY" \
  -d '{
    "query": "query PlayerStats($seriesId: ID!) { seriesState(id: $seriesId) { games { id teams { name side won players { name character { name } kills deaths killAssistsGiven } } } } }",
    "variables": {"seriesId": "2618419"}
  }' | jq '.data.seriesState.games[0].teams'

# =============================================================================
# Example 6: Save Full Response to File
# =============================================================================
echo -e "\n=== Example 6: Save to File ==="
curl -s -X POST "$API_URL" \
  -H "Content-Type: application/json" \
  -H "x-api-key: $API_KEY" \
  -d '{
    "query": "query SeriesState($seriesId: ID!) { seriesState(id: $seriesId) { id version title { nameShortened } format started finished startedAt teams { id name won score } games { id sequenceNumber started finished paused clock { currentSeconds ticking } map { name } draftActions { id sequenceNumber type drafter { id type } draftable { id type name } } teams { id name side won score kills deaths structuresDestroyed objectives { id type } players { id name participationStatus character { id name } kills deaths killAssistsGiven } } } } }",
    "variables": {"seriesId": "2618419"}
  }' > series_2618419.json
echo "Saved to series_2618419.json"

# =============================================================================
# Example 7: Batch Fetch with Rate Limiting
# =============================================================================
echo -e "\n=== Example 7: Batch Fetch (first 3 series) ==="
SERIES_IDS=("2618418" "2618419" "2618420")

for sid in "${SERIES_IDS[@]}"; do
  echo "Fetching series $sid..."
  curl -s -X POST "$API_URL" \
    -H "Content-Type: application/json" \
    -H "x-api-key: $API_KEY" \
    -d "{
      \"query\": \"query { seriesState(id: \\\"$sid\\\") { id teams { name } } }\",
      \"variables\": {}
    }" | jq -c '.data.seriesState | {id, teams: [.teams[].name]}'
  
  # Rate limit: 3 second delay
  sleep 3
done

echo -e "\nDone!"
