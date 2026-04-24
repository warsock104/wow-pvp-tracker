import requests
import time
import os
import json
from datetime import datetime, timezone
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
# API helper
# -----------------------------
def get(url, token, params=None):
    if params is None:
        params = {}
    params.setdefault("locale", LOCALE)
    params.setdefault("access_token", token)
    headers = {
        "Authorization": f"Bearer {token}",
        "Battlenet-Namespace": NAMESPACE_DYNAMIC,
    }
    resp = requests.get(url, params=params, headers=headers)
    resp.raise_for_status()
    return resp.json()

# -----------------------------
# Blizzard API calls
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
# Supabase upsert
# -----------------------------
def push_to_supabase(supabase, season_id, bracket, data):
    entries = data.get("entries", [])
    if not entries:
        return 0

    fetched_at = datetime.now(timezone.utc).isoformat()
    char_class, spec = parse_class_spec(bracket)
    rows = []
    for entry in entries:
        char = entry.get("character", {})
        stats = entry.get("season_match_statistics", {})
        rows.append({
            "season_id": season_id,
            "bracket": bracket,
            "rank": entry.get("rank"),
            "rating": entry.get("rating"),
            "character_name": char.get("name"),
            "realm_slug": char.get("realm", {}).get("slug"),
            "character_class": char_class,
            "spec": spec,
            "faction": entry.get("faction", {}).get("type"),
            "wins": stats.get("won"),
            "losses": stats.get("lost"),
            "played": stats.get("played"),
            "fetched_at": fetched_at,
        })

    # Deduplicate by (season_id, bracket, rank) — API can return duplicate ranks
    seen = {}
    for row in rows:
        seen[(row["season_id"], row["bracket"], row["rank"])] = row
    rows = list(seen.values())

    # Insert in batches of 500
    batch_size = 500
    for i in range(0, len(rows), batch_size):
        supabase.table("pvp_leaderboard").insert(rows[i:i + batch_size]).execute()

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

    print("Clearing existing data...")
    supabase.table("pvp_leaderboard").delete().gte("id", 0).execute()
    print("Table cleared.")

    for bracket in ALL_BRACKETS:
        print(f"  Fetching {bracket}...", end=" ", flush=True)
        try:
            data = get_leaderboard(token, season_id, bracket)
        except requests.HTTPError as e:
            print(f"skipped ({e.response.status_code})")
            continue

        # Save local JSON backup
        filename = f"season_{season_id}_{bracket}.json"
        with open(os.path.join(OUTPUT_DIR, filename), "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        # Push to Supabase
        count = push_to_supabase(supabase, season_id, bracket, data)
        print(f"{count} entries inserted.")
        time.sleep(0.2)

    print(f"[{datetime.now()}] Done.")

if __name__ == "__main__":
    main()
