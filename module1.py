import requests
import time
import os
import json
from datetime import datetime, timezone, date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from supabase import create_client

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))

# -----------------------------
# CONFIG
# -----------------------------
CLIENT_ID = os.environ["BLIZZARD_CLIENT_ID"]
CLIENT_SECRET = os.environ["BLIZZARD_CLIENT_SECRET"]
REGION = "us"
LOCALE = "en_US"
NAMESPACE_DYNAMIC = f"dynamic-{REGION}"

BRACKETS = ["2v2", "3v3", "battlegrounds"]

SHUFFLE_BRACKETS = [
    "shuffle-deathknight-blood",
    "shuffle-deathknight-frost",
    "shuffle-deathknight-unholy",
    "shuffle-demonhunter-devourer",
    "shuffle-demonhunter-havoc",
    "shuffle-demonhunter-vengeance",
    "shuffle-druid-balance",
    "shuffle-druid-feral",
    "shuffle-druid-guardian",
    "shuffle-druid-restoration",
    "shuffle-evoker-augmentation",
    "shuffle-evoker-devastation",
    "shuffle-evoker-preservation",
    "shuffle-hunter-beastmastery",
    "shuffle-hunter-marksmanship",
    "shuffle-hunter-survival",
    "shuffle-mage-arcane",
    "shuffle-mage-fire",
    "shuffle-mage-frost",
    "shuffle-monk-brewmaster",
    "shuffle-monk-mistweaver",
    "shuffle-monk-windwalker",
    "shuffle-paladin-holy",
    "shuffle-paladin-protection",
    "shuffle-paladin-retribution",
    "shuffle-priest-discipline",
    "shuffle-priest-holy",
    "shuffle-priest-shadow",
    "shuffle-rogue-assassination",
    "shuffle-rogue-outlaw",
    "shuffle-rogue-subtlety",
    "shuffle-shaman-elemental",
    "shuffle-shaman-enhancement",
    "shuffle-shaman-restoration",
    "shuffle-warlock-affliction",
    "shuffle-warlock-demonology",
    "shuffle-warlock-destruction",
    "shuffle-warrior-arms",
    "shuffle-warrior-fury",
    "shuffle-warrior-protection",
]

ALL_BRACKETS = BRACKETS + SHUFFLE_BRACKETS
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "wow_pvp_arena_data")

CLASS_SLUG_MAP = {
    "deathknight": "Death Knight",
    "demonhunter": "Demon Hunter",
    "druid": "Druid",
    "evoker": "Evoker",
    "hunter": "Hunter",
    "mage": "Mage",
    "monk": "Monk",
    "paladin": "Paladin",
    "priest": "Priest",
    "rogue": "Rogue",
    "shaman": "Shaman",
    "warlock": "Warlock",
    "warrior": "Warrior",
}

def parse_class_spec(bracket):
    if not bracket.startswith("shuffle-"):
        return None, None
    remainder = bracket[len("shuffle-"):]
    class_slug, spec_slug = remainder.rsplit("-", 1)
    return CLASS_SLUG_MAP.get(class_slug, class_slug.title()), spec_slug.title()

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

# -----------------------------
# AUTH
# -----------------------------
def get_access_token():
    url = f"https://{REGION}.battle.net/oauth/token"
    resp = requests.post(url, data={"grant_type": "client_credentials"}, auth=(CLIENT_ID, CLIENT_SECRET))
    resp.raise_for_status()
    return resp.json()["access_token"]

# -----------------------------
# API helper (game data)
# -----------------------------
def get(url, token, params=None):
    if params is None:
        params = {}
    params.setdefault("locale", LOCALE)
    headers = {
        "Authorization": f"Bearer {token}",
        "Battlenet-Namespace": NAMESPACE_DYNAMIC,
    }
    resp = requests.get(url, params=params, headers=headers)
    resp.raise_for_status()
    return resp.json()

# -----------------------------
# Blizzard leaderboard calls
# -----------------------------
def get_current_season(token):
    url = f"https://{REGION}.api.blizzard.com/data/wow/pvp-season/index"
    data = get(url, token)
    current = data.get("current_season")
    if not current:
        raise Exception("No current PvP season in response.")
    return current["id"]

def get_leaderboard(token, season_id, bracket):
    url = f"https://{REGION}.api.blizzard.com/data/wow/pvp-season/{season_id}/pvp-leaderboard/{bracket}"
    return get(url, token)

# -----------------------------
# Character profile enrichment
# -----------------------------
def fetch_character_profile(token, realm_slug, character_name):
    """Return (class_name, spec_name) for a single character, or (None, None) on failure."""
    url = f"https://{REGION}.api.blizzard.com/profile/wow/character/{realm_slug}/{character_name.lower()}"
    try:
        resp = requests.get(url,
            headers={"Authorization": f"Bearer {token}"},
            params={"namespace": f"profile-{REGION}", "locale": LOCALE},
            timeout=10,
        )
        if resp.status_code == 200:
            d = resp.json()
            char_class = d.get("character_class", {}).get("name")
            spec = d.get("active_spec", {}).get("name")
            return char_class, spec
    except Exception:
        pass
    return None, None

def enrich_with_profiles(token, entries, max_workers=20):
    """Parallel character profile lookups. Returns {(name, realm): (class, spec)}."""
    profile_map = {}

    def lookup(entry):
        char = entry.get("character", {})
        name = char.get("name", "")
        realm = char.get("realm", {}).get("slug", "")
        if not name or not realm:
            return name, realm, None, None
        char_class, spec = fetch_character_profile(token, realm, name)
        return name, realm, char_class, spec

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(lookup, entry) for entry in entries]
        for future in as_completed(futures):
            name, realm, char_class, spec = future.result()
            profile_map[(name, realm)] = (char_class, spec)

    return profile_map

# -----------------------------
# Supabase insert
# -----------------------------
def push_to_supabase(supabase, season_id, bracket, data, profile_map=None):
    entries = data.get("entries", [])
    if not entries:
        return 0

    fetched_at = datetime.now(timezone.utc).isoformat()
    snapshot_date = date.today().isoformat()
    bracket_class, bracket_spec = parse_class_spec(bracket)

    rows = []
    for entry in entries:
        char = entry.get("character", {})
        stats = entry.get("season_match_statistics", {})
        name = char.get("name")
        realm = char.get("realm", {}).get("slug")

        if profile_map is not None:
            char_class, spec = profile_map.get((name, realm), (None, None))
        else:
            char_class, spec = bracket_class, bracket_spec

        rows.append({
            "season_id": season_id,
            "bracket": bracket,
            "rank": entry.get("rank"),
            "rating": entry.get("rating"),
            "character_name": name,
            "realm_slug": realm,
            "character_class": char_class,
            "spec": spec,
            "faction": entry.get("faction", {}).get("type"),
            "wins": stats.get("won"),
            "losses": stats.get("lost"),
            "played": stats.get("played"),
            "fetched_at": fetched_at,
            "snapshot_date": snapshot_date,
        })

    # Deduplicate by (season_id, bracket, rank, snapshot_date)
    seen = {}
    for row in rows:
        seen[(row["season_id"], row["bracket"], row["rank"], row["snapshot_date"])] = row
    rows = list(seen.values())

    batch_size = 500
    for i in range(0, len(rows), batch_size):
        supabase.table("pvp_leaderboard").upsert(
            rows[i:i + batch_size],
            on_conflict="season_id,bracket,rank,snapshot_date",
        ).execute()

    # Build daily summary for this bracket
    clean = [r for r in rows if r.get("character_class") and r.get("spec")]
    if clean:
        groups = {}
        for r in clean:
            key = (r["character_class"], r["spec"])
            groups.setdefault(key, []).append(r)

        summary_rows = []
        for (cls, sp), grp in groups.items():
            ratings   = [r["rating"] for r in grp if r.get("rating") is not None]
            win_rates = [
                r["wins"] / r["played"] * 100
                for r in grp
                if r.get("wins") is not None and r.get("played")
            ]
            summary_rows.append({
                "snapshot_date":   snapshot_date,
                "season_id":       season_id,
                "bracket":         bracket,
                "character_class": cls,
                "spec":            sp,
                "players":         len(grp),
                "avg_rating":      round(sum(ratings) / len(ratings), 2) if ratings else None,
                "max_rating":      max(ratings) if ratings else None,
                "avg_win_rate":    round(sum(win_rates) / len(win_rates), 2) if win_rates else None,
            })

        supabase.table("pvp_daily_summary").upsert(
            summary_rows,
            on_conflict="snapshot_date,bracket,character_class,spec",
        ).execute()

    return len(rows)

# -----------------------------
# Main
# -----------------------------
def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print(f"[{datetime.now()}] Starting WoW PvP leaderboard fetch...")
    token = get_access_token()
    print("Token acquired.")

    season_id = get_current_season(token)
    print(f"Current season ID: {season_id}")

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    for bracket in ALL_BRACKETS:
        print(f"  Fetching {bracket}...", end=" ", flush=True)
        try:
            data = get_leaderboard(token, season_id, bracket)
        except requests.HTTPError as e:
            print(f"skipped ({e.response.status_code})")
            continue

        filename = f"season_{season_id}_{bracket}_{date.today().isoformat()}.json"
        with open(os.path.join(OUTPUT_DIR, filename), "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        # For 2v2/3v3 enrich each character with a profile lookup
        if not bracket.startswith("shuffle-"):
            entries = data.get("entries", [])
            print(f"enriching {len(entries)} profiles...", end=" ", flush=True)
            profile_map = enrich_with_profiles(token, entries)
            count = push_to_supabase(supabase, season_id, bracket, data, profile_map=profile_map)
        else:
            count = push_to_supabase(supabase, season_id, bracket, data)

        print(f"{count} entries inserted.")
        time.sleep(0.2)

    cutoff = (date.today() - timedelta(days=7)).isoformat()
    supabase.table("pvp_leaderboard").delete().lt("snapshot_date", cutoff).execute()
    print(f"Pruned raw snapshots older than {cutoff}.")

    print(f"[{datetime.now()}] Done.")

if __name__ == "__main__":
    main()
