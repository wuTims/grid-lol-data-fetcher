# LoL Professional Draft Sequence

This document details the standard League of Legends professional draft format and how it maps to GRID API data.

## Draft Overview

Professional LoL uses a **20-action draft** consisting of:
- 10 bans (5 per team)
- 10 picks (5 per team)

The draft is divided into 4 phases with alternating team actions.

## Draft Phases

### Ban Phase 1 (Actions 1-6)

| Sequence | Team | Action |
|----------|------|--------|
| 1 | Blue | Ban |
| 2 | Red | Ban |
| 3 | Blue | Ban |
| 4 | Red | Ban |
| 5 | Blue | Ban |
| 6 | Red | Ban |

**Strategic notes:**
- Target S-tier champions that don't fit your composition
- Remove opponent's comfort/signature picks
- Ban hard counters to your planned picks

### Pick Phase 1 (Actions 7-12)

| Sequence | Team | Action | Strategic Name |
|----------|------|--------|----------------|
| 7 | Blue | Pick | B1 (First Pick) |
| 8 | Red | Pick | R1 |
| 9 | Red | Pick | R2 |
| 10 | Blue | Pick | B2 |
| 11 | Blue | Pick | B3 |
| 12 | Red | Pick | R3 |

**Strategic notes:**
- Blue B1: Highest priority contested champion or strong flex
- Red R1-R2: Counter blue's first pick OR secure two strong champions
- Blue B2-B3: Round out composition needs
- Red R3: Setup for ban phase 2 information

### Ban Phase 2 (Actions 13-16)

| Sequence | Team | Action |
|----------|------|--------|
| 13 | Red | Ban |
| 14 | Blue | Ban |
| 15 | Red | Ban |
| 16 | Blue | Ban |

**Strategic notes:**
- Target specific players based on remaining champion pool
- Remove counters to your existing picks
- Deny obvious synergy completions

### Pick Phase 2 (Actions 17-20)

| Sequence | Team | Action | Strategic Name |
|----------|------|--------|----------------|
| 17 | Red | Pick | R4 |
| 18 | Blue | Pick | B4 |
| 19 | Blue | Pick | B5 (Last Pick) |
| 20 | Red | Pick | R5 (Counter Pick) |

**Strategic notes:**
- Red R4: Deny blue's obvious choice
- Blue B4-B5: Counter red or complete synergy
- Red R5: Hard counter for favorable lane matchup

## Side Advantages

### Blue Side
- First pick advantage (can secure highest priority champion)
- More picks before ban phase 2 (better information)
- Sets the tempo of the draft

### Red Side
- Counter-pick advantage (last pick)
- Sees 6 enemy champions before final pick
- Can react to blue's composition

## GRID API Mapping

```json
{
  "draftActions": [
    {
      "sequenceNumber": "1",
      "type": "ban",
      "drafter": { "id": "BLUE_TEAM_ID", "type": "team" },
      "draftable": { "id": "CHAMP_UUID", "name": "Ksante" }
    },
    {
      "sequenceNumber": "2", 
      "type": "ban",
      "drafter": { "id": "RED_TEAM_ID", "type": "team" },
      "draftable": { "id": "CHAMP_UUID", "name": "Viego" }
    }
    // ... continues to sequence 20
  ]
}
```

### Determining Team Side

The API provides `side` field on teams within games:

```json
{
  "teams": [
    { "id": "123", "name": "T1", "side": "blue", "won": true },
    { "id": "456", "name": "Gen.G", "side": "red", "won": false }
  ]
}
```

## Draft Phase Detection

Given a sequence number, determine the phase:

```python
def get_draft_phase(sequence: int) -> str:
    if 1 <= sequence <= 6:
        return "BAN_PHASE_1"
    elif 7 <= sequence <= 12:
        return "PICK_PHASE_1"
    elif 13 <= sequence <= 16:
        return "BAN_PHASE_2"
    elif 17 <= sequence <= 20:
        return "PICK_PHASE_2"
    else:
        raise ValueError(f"Invalid sequence: {sequence}")

def get_acting_team(sequence: int) -> str:
    """Returns 'blue' or 'red' for who acts at this sequence."""
    blue_actions = {1, 3, 5, 7, 10, 11, 14, 16, 18, 19}
    return "blue" if sequence in blue_actions else "red"
```

## Visual Reference

```
        BLUE SIDE                    RED SIDE
        ─────────                    ────────
Ban 1:  [  BAN  ] ◄──────────────── 
Ban 2:  ────────────────────────► [  BAN  ]
Ban 3:  [  BAN  ] ◄──────────────── 
Ban 4:  ────────────────────────► [  BAN  ]
Ban 5:  [  BAN  ] ◄──────────────── 
Ban 6:  ────────────────────────► [  BAN  ]
        ═══════════════════════════════════
Pick 1: [  B1   ] ◄──────────────── 
Pick 2: ────────────────────────► [  R1   ]
Pick 3: ────────────────────────► [  R2   ]
Pick 4: [  B2   ] ◄──────────────── 
Pick 5: [  B3   ] ◄──────────────── 
Pick 6: ────────────────────────► [  R3   ]
        ═══════════════════════════════════
Ban 7:  ────────────────────────► [  BAN  ]
Ban 8:  [  BAN  ] ◄──────────────── 
Ban 9:  ────────────────────────► [  BAN  ]
Ban 10: [  BAN  ] ◄──────────────── 
        ═══════════════════════════════════
Pick 7: ────────────────────────► [  R4   ]
Pick 8: [  B4   ] ◄──────────────── 
Pick 9: [  B5   ] ◄──────────────── 
Pick 10:────────────────────────► [  R5   ]
```

## Common Draft Patterns

### Flex Drafting
Picking champions that can play multiple roles to hide information:
- Tristana (ADC/Mid)
- Neeko (Mid/Top/ADC)
- Aurora (Mid/Top)
- Sett (Top/Support/Jungle)

### Target Banning
Focusing bans on one player's champion pool:
```
If enemy ADC is known for: Kai'Sa, Jinx, Varus
Ban Phase 1: Kai'Sa
Ban Phase 2: Jinx (if Varus picked) or Varus (if not picked)
```

### Comfort Priority
Prioritizing player comfort over "optimal" picks:
- High-profile players often get signature champions
- "Comfort > Counter" in most cases

## Data Analysis Applications

### Ban Rate Calculation
```sql
SELECT 
    champion_name,
    COUNT(*) as times_banned,
    COUNT(*) * 100.0 / (SELECT COUNT(DISTINCT game_id) FROM draft_actions) as ban_rate
FROM draft_actions
WHERE action_type = 'ban'
GROUP BY champion_id
ORDER BY times_banned DESC;
```

### First Pick Priority
```sql
SELECT 
    champion_name,
    COUNT(*) as first_picks
FROM draft_actions
WHERE sequence_number = 7  -- B1
GROUP BY champion_id
ORDER BY first_picks DESC;
```

### Counter Pick Analysis
```sql
-- Find what Red picks last (R5) against specific B5 picks
SELECT 
    b5.champion_name as blue_last_pick,
    r5.champion_name as red_counter,
    COUNT(*) as occurrences
FROM draft_actions b5
JOIN draft_actions r5 ON b5.game_id = r5.game_id
WHERE b5.sequence_number = 19  -- B5
  AND r5.sequence_number = 20  -- R5
GROUP BY b5.champion_id, r5.champion_id
ORDER BY occurrences DESC;
```
