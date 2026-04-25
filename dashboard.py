import os
import io
import base64
import requests
import streamlit as st
import pandas as pd
import plotly.express as px
from PIL import Image
from concurrent.futures import ThreadPoolExecutor, as_completed
from supabase import create_client
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))

# Streamlit Cloud exposes secrets via st.secrets, not os.environ — bridge them
_REQUIRED_SECRETS = [
    "BLIZZARD_CLIENT_ID", "BLIZZARD_CLIENT_SECRET",
    "SUPABASE_URL", "SUPABASE_KEY",
]
_missing = []
for _k in _REQUIRED_SECRETS:
    if _k not in os.environ:
        try:
            os.environ[_k] = st.secrets[_k]
        except KeyError:
            _missing.append(_k)
if _missing:
    st.error(f"Missing secrets: {', '.join(_missing)}. Add them in the app's Settings → Secrets.")
    st.stop()

# ─────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────
st.set_page_config(
    page_title="WoW PvP Analytics",
    page_icon="⚔️",
    layout="wide",
)

# ─────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────
CLASS_COLORS = {
    "Death Knight": "#C41E3A",
    "Demon Hunter": "#A330C9",
    "Druid":        "#FF7C0A",
    "Evoker":       "#33937F",
    "Hunter":       "#AAD372",
    "Mage":         "#3FC7EB",
    "Monk":         "#00FF98",
    "Paladin":      "#F48CBA",
    "Priest":       "#C8C8C8",
    "Rogue":        "#FFF468",
    "Shaman":       "#0070DD",
    "Warlock":      "#8788EE",
    "Warrior":      "#C79C6E",
}

CLASS_SLUG_MAP = {
    "Death Knight": "deathknight",
    "Demon Hunter": "demonhunter",
    "Druid":        "druid",
    "Evoker":       "evoker",
    "Hunter":       "hunter",
    "Mage":         "mage",
    "Monk":         "monk",
    "Paladin":      "paladin",
    "Priest":       "priest",
    "Rogue":        "rogue",
    "Shaman":       "shaman",
    "Warlock":      "warlock",
    "Warrior":      "warrior",
}

SHUFFLE_CLASSES = list(CLASS_SLUG_MAP.keys())

# Spec names as stored in DB for Solo Shuffle (derived from bracket slug via .title())
ALL_SPECS = {
    "Death Knight":  ["Blood", "Frost", "Unholy"],
    "Demon Hunter":  ["Havoc", "Vengeance"],
    "Druid":         ["Balance", "Feral", "Guardian", "Restoration"],
    "Evoker":        ["Augmentation", "Devastation", "Preservation"],
    "Hunter":        ["Beastmastery", "Marksmanship", "Survival"],
    "Mage":          ["Arcane", "Fire", "Frost"],
    "Monk":          ["Brewmaster", "Mistweaver", "Windwalker"],
    "Paladin":       ["Holy", "Protection", "Retribution"],
    "Priest":        ["Discipline", "Holy", "Shadow"],
    "Rogue":         ["Assassination", "Outlaw", "Subtlety"],
    "Shaman":        ["Elemental", "Enhancement", "Restoration"],
    "Warlock":       ["Affliction", "Demonology", "Destruction"],
    "Warrior":       ["Arms", "Fury", "Protection"],
}

ROLES = ["Tank", "Healer", "Melee DPS", "Ranged DPS"]

# Keyed by (class, spec) to handle shared spec names (e.g. Frost = DK Melee / Mage Ranged)
# Includes both slug-based names (Solo Shuffle DB) and API names (Arena DB)
SPEC_ROLES = {
    ("Death Knight",  "Blood"):         "Tank",
    ("Death Knight",  "Frost"):         "Melee DPS",
    ("Death Knight",  "Unholy"):        "Melee DPS",
    ("Demon Hunter",  "Havoc"):         "Melee DPS",
    ("Demon Hunter",  "Vengeance"):     "Tank",
    ("Druid",         "Balance"):       "Ranged DPS",
    ("Druid",         "Feral"):         "Melee DPS",
    ("Druid",         "Guardian"):      "Tank",
    ("Druid",         "Restoration"):   "Healer",
    ("Evoker",        "Augmentation"):  "Ranged DPS",
    ("Evoker",        "Devastation"):   "Ranged DPS",
    ("Evoker",        "Preservation"):  "Healer",
    ("Hunter",        "Beastmastery"):  "Ranged DPS",  # Solo Shuffle slug name
    ("Hunter",        "Beast Mastery"): "Ranged DPS",  # Arena API name
    ("Hunter",        "Marksmanship"):  "Ranged DPS",
    ("Hunter",        "Survival"):      "Melee DPS",
    ("Mage",          "Arcane"):        "Ranged DPS",
    ("Mage",          "Fire"):          "Ranged DPS",
    ("Mage",          "Frost"):         "Ranged DPS",
    ("Monk",          "Brewmaster"):    "Tank",
    ("Monk",          "Mistweaver"):    "Healer",
    ("Monk",          "Windwalker"):    "Melee DPS",
    ("Paladin",       "Holy"):          "Healer",
    ("Paladin",       "Protection"):    "Tank",
    ("Paladin",       "Retribution"):   "Melee DPS",
    ("Priest",        "Discipline"):    "Healer",
    ("Priest",        "Holy"):          "Healer",
    ("Priest",        "Shadow"):        "Ranged DPS",
    ("Rogue",         "Assassination"): "Melee DPS",
    ("Rogue",         "Outlaw"):        "Melee DPS",
    ("Rogue",         "Subtlety"):      "Melee DPS",
    ("Shaman",        "Elemental"):     "Ranged DPS",
    ("Shaman",        "Enhancement"):   "Melee DPS",
    ("Shaman",        "Restoration"):   "Healer",
    ("Warlock",       "Affliction"):    "Ranged DPS",
    ("Warlock",       "Demonology"):    "Ranged DPS",
    ("Warlock",       "Destruction"):   "Ranged DPS",
    ("Warrior",       "Arms"):          "Melee DPS",
    ("Warrior",       "Fury"):          "Melee DPS",
    ("Warrior",       "Protection"):    "Tank",
}

RATING_BINS   = [0, 1600, 1800, 2000, 2100, 2400, float("inf")]
RATING_LABELS = ["< 1600", "1600–1800", "1800–2000", "2000–2100", "2100–2400", "2400+"]
TIER_COLORS   = {
    "< 1600":    "#555566",
    "1600–1800": "#4a90d9",
    "1800–2000": "#27ae60",
    "2000–2100": "#f39c12",
    "2100–2400": "#e74c3c",
    "2400+":     "#9b59b6",
}

# ─────────────────────────────────────────────
# BLIZZARD ICONS
# ─────────────────────────────────────────────
@st.cache_data(ttl=82800)
def load_blizzard_icons():
    """Fetch class + spec icon URLs from Blizzard media API. Cached 23 h."""
    token_resp = requests.post(
        "https://us.battle.net/oauth/token",
        data={"grant_type": "client_credentials"},
        auth=(os.environ["BLIZZARD_CLIENT_ID"].strip(), os.environ["BLIZZARD_CLIENT_SECRET"].strip()),
    ).json()
    if "access_token" not in token_resp:
        raise RuntimeError(f"Blizzard auth failed: {token_resp}")
    token = token_resp["access_token"]

    headers = {"Authorization": f"Bearer {token}"}
    params  = {"namespace": "static-us", "locale": "en_US"}
    base    = "https://us.api.blizzard.com/data/wow"

    def fetch_and_crop(media_url, factor=0.85):
        try:
            assets = requests.get(media_url, headers=headers, params=params, timeout=10).json().get("assets", [])
            img_url = next((a["value"] for a in assets if a["key"] == "icon"), None)
            if not img_url:
                return None
            raw = requests.get(img_url, timeout=10).content
            img = Image.open(io.BytesIO(raw)).convert("RGBA")
            w, h = img.size
            mx, my = int(w * (1 - factor) / 2), int(h * (1 - factor) / 2)
            cropped = img.crop((mx, my, w - mx, h - my))
            buf = io.BytesIO()
            cropped.save(buf, format="PNG")
            return "data:image/png;base64," + base64.b64encode(buf.getvalue()).decode()
        except Exception:
            return None

    # Collect all (key, media_url) pairs first, then fetch+crop in parallel
    work = {}
    all_classes = requests.get(f"{base}/playable-class/index", headers=headers, params=params).json()
    for cls in all_classes.get("classes", []):
        cls_id, cls_name = cls["id"], cls["name"]
        work[("class", cls_name)] = f"{base}/media/playable-class/{cls_id}"
        cls_data = requests.get(f"{base}/playable-class/{cls_id}", headers=headers, params=params).json()
        for spec in cls_data.get("specializations", []):
            spec_id, spec_name = spec["id"], spec["name"]
            work[("spec", cls_name, spec_name)] = f"{base}/media/playable-specialization/{spec_id}"

    class_icons = {}
    spec_icons  = {}

    with ThreadPoolExecutor(max_workers=20) as ex:
        futures = {ex.submit(fetch_and_crop, url): key for key, url in work.items()}
        for fut in as_completed(futures):
            key = futures[fut]
            result = fut.result()
            if key[0] == "class":
                class_icons[key[1]] = result
            else:
                spec_icons[(key[1], key[2])] = result

    return class_icons, spec_icons


def add_bar_icons(fig, categories, icon_map, bottom_margin=120, size_factor=0.75):
    """Overlay icons below each bar and hide text tick labels."""
    n = len(categories)
    if not n:
        return fig
    icon_w = size_factor / n
    approx_plot_height = max(150, 420 - bottom_margin)
    sizey = min(icon_w * 6, (bottom_margin - 15) / approx_plot_height)
    for i, cat in enumerate(categories):
        url = icon_map.get(cat)
        if url:
            fig.add_layout_image(
                source=url,
                x=(i + 0.5) / n,
                y=-0.02,
                xref="paper", yref="paper",
                sizex=icon_w, sizey=sizey,
                xanchor="center", yanchor="top",
                layer="above",
            )
    fig.update_layout(
        margin=dict(b=bottom_margin),
        xaxis=dict(showticklabels=False, title=""),
    )
    return fig

# ─────────────────────────────────────────────
# SUPABASE DATA
# ─────────────────────────────────────────────
@st.cache_resource
def get_supabase():
    return create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

@st.cache_data(ttl=3600)
def load_arena_trends(bracket: str) -> pd.DataFrame:
    resp = (
        get_supabase()
        .table("pvp_daily_trends")
        .select("*")
        .eq("bracket", bracket)
        .order("snapshot_date")
        .execute()
    )
    return pd.DataFrame(resp.data)

@st.cache_data(ttl=3600)
def load_shuffle_trends(class_name: str) -> pd.DataFrame:
    slug = CLASS_SLUG_MAP[class_name]
    resp = (
        get_supabase()
        .table("pvp_daily_trends")
        .select("*")
        .like("bracket", f"shuffle-{slug}-%")
        .order("snapshot_date")
        .execute()
    )
    return pd.DataFrame(resp.data)

@st.cache_data(ttl=3600)
def load_last_updated() -> str:
    resp = (
        get_supabase()
        .table("pvp_leaderboard")
        .select("fetched_at")
        .order("fetched_at", desc=True)
        .limit(1)
        .execute()
    )
    if resp.data:
        from datetime import datetime, timezone
        ts = datetime.fromisoformat(resp.data[0]["fetched_at"].replace("Z", "+00:00"))
        return ts.astimezone(timezone.utc).strftime("%b %d, %Y at %H:%M UTC")
    return "Unknown"

@st.cache_data(ttl=3600, show_spinner="Loading leaderboard data...")
def load_bracket(bracket: str) -> pd.DataFrame:
    resp = (
        get_supabase()
        .table("pvp_leaderboard")
        .select("rank,character_class,spec,rating,wins,losses,played,faction")
        .eq("bracket", bracket)
        .limit(5000)
        .execute()
    )
    df = pd.DataFrame(resp.data)
    if not df.empty:
        df["win_rate"] = df["wins"] / df["played"].replace(0, pd.NA) * 100
    return df

@st.cache_data(ttl=3600, show_spinner="Loading shuffle data...")
def load_shuffle_class(class_name: str) -> pd.DataFrame:
    slug = CLASS_SLUG_MAP[class_name]
    resp = (
        get_supabase()
        .table("pvp_leaderboard")
        .select("rank,character_class,spec,rating,wins,losses,played,faction,bracket")
        .like("bracket", f"shuffle-{slug}-%")
        .limit(20000)
        .execute()
    )
    df = pd.DataFrame(resp.data)
    if not df.empty:
        df["win_rate"] = df["wins"] / df["played"].replace(0, pd.NA) * 100
    return df

# ─────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────
st.sidebar.title("⚔️ WoW PvP Analytics")
mode = st.sidebar.radio("Bracket Type", ["2v2", "3v3", "Solo Shuffle"])

selected_class = st.sidebar.selectbox(
    "Class", SHUFFLE_CLASSES, disabled=(mode != "Solo Shuffle")
)

if mode == "Solo Shuffle":
    df = load_shuffle_class(selected_class)
    page_title = f"Solo Shuffle — {selected_class}"
else:
    df = load_bracket(mode)
    page_title = f"{mode} Arena — Class & Spec Analytics"

min_games = st.sidebar.slider("Min games played (win rate filter)", 0, 100, 20, step=5)

if mode == "Solo Shuffle":
    class_roles = [r for r in ROLES if r in {
        SPEC_ROLES.get((selected_class, sp), "Unknown")
        for sp in ALL_SPECS.get(selected_class, [])
    }]
else:
    class_roles = ROLES
selected_roles = st.sidebar.multiselect("Roles", class_roles, default=class_roles)

st.sidebar.divider()
st.sidebar.caption(f"Last updated: {load_last_updated()}")
st.sidebar.caption("Refreshes daily at 6 AM EST via GitHub Actions.")

# ─────────────────────────────────────────────
# LOAD ICONS
# ─────────────────────────────────────────────
try:
    class_icons, spec_icons = load_blizzard_icons()
except Exception as _icon_err:
    st.warning(f"Icons unavailable: {_icon_err}")
    class_icons, spec_icons = {}, {}

# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
st.title(page_title)

if df.empty:
    st.warning("No data available.")
    st.stop()

df_clean = df.dropna(subset=["character_class", "spec"]).copy()
df_clean["role"] = df_clean.apply(
    lambda r: SPEC_ROLES.get((r["character_class"], r["spec"]), "Unknown"), axis=1
)
if selected_roles:
    df_clean = df_clean[df_clean["role"].isin(selected_roles)]
df_wr = df_clean[df_clean["played"] >= min_games]

if df_clean.empty:
    st.warning("No data for the selected roles.")
    st.stop()

# ── Top metrics ───────────────────────────────
c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Players", f"{len(df_clean):,}")
c2.metric("Avg Rating",    f"{int(df_clean['rating'].mean()):,}")
c3.metric("Top Rating",    f"{int(df_clean['rating'].max()):,}")
avg_wr = df_wr["win_rate"].mean()
c4.metric("Avg Win Rate",  f"{avg_wr:.1f}%" if not pd.isna(avg_wr) else "—")

st.divider()

# ─────────────────────────────────────────────
# ARENA MODE
# ─────────────────────────────────────────────
if mode in ("2v2", "3v3"):

    # ── Row 1: Class Representation % ─────────────
    counts = (
        df_clean.groupby("character_class").size()
        .reset_index(name="players")
    )
    counts["pct"] = (counts["players"] / counts["players"].sum() * 100).round(1)
    counts = counts.sort_values("pct", ascending=False)
    ordered_classes_pct = counts["character_class"].tolist()
    fig = px.bar(counts, x="character_class", y="pct",
                 color="character_class", color_discrete_map=CLASS_COLORS,
                 category_orders={"character_class": ordered_classes_pct},
                 title="Class Representation %",
                 labels={"character_class": "", "pct": "% of Players"},
                 text=counts["pct"].apply(lambda x: f"{x:.1f}%"),
                 template="plotly_dark")
    fig.update_layout(showlegend=False, yaxis=dict(ticksuffix="%", range=[0, counts["pct"].max() * 1.18]))
    fig.update_traces(textposition="outside", textfont=dict(size=13))
    add_bar_icons(fig, ordered_classes_pct, class_icons)
    st.plotly_chart(fig, use_container_width=True)

    # ── Row 2: Avg Rating by Class ────────────────
    avg_rat = (
        df_clean.groupby("character_class")["rating"]
        .mean().round(0).reset_index()
        .rename(columns={"rating": "avg_rating"})
        .sort_values("avg_rating", ascending=False)
    )
    ordered_classes_rat = avg_rat["character_class"].tolist()
    _rat_min = avg_rat["avg_rating"].min()
    _rat_max = avg_rat["avg_rating"].max()
    _rat_floor = max(0, int(_rat_min // 100) * 100 - 50)
    fig = px.bar(avg_rat, x="character_class", y="avg_rating",
                 color="character_class", color_discrete_map=CLASS_COLORS,
                 category_orders={"character_class": ordered_classes_rat},
                 title="Avg Rating by Class",
                 labels={"character_class": "", "avg_rating": "Avg Rating"},
                 text=avg_rat["avg_rating"].astype(int).apply(lambda x: f"{x:,}"),
                 template="plotly_dark")
    fig.update_traces(textposition="outside", textfont=dict(size=13))
    fig.update_layout(
        showlegend=False,
        yaxis=dict(range=[_rat_floor, _rat_max + (_rat_max - _rat_floor) * 0.18]),
    )
    add_bar_icons(fig, ordered_classes_rat, class_icons)
    st.plotly_chart(fig, use_container_width=True)

    # ── Row 3: Avg Win Rate by Class ──────────────
    wr_class = (
        df_wr.groupby("character_class")["win_rate"]
        .mean().round(1).reset_index()
        .rename(columns={"win_rate": "avg_win_rate"})
        .sort_values("avg_win_rate", ascending=False)
    )
    if not wr_class.empty:
        ordered_classes_wr = wr_class["character_class"].tolist()
        _cwr_min = wr_class["avg_win_rate"].min()
        _cwr_max = wr_class["avg_win_rate"].max()
        _cwr_floor = max(0, min(round(_cwr_min) - 2, 47))
        fig = px.bar(wr_class, x="character_class", y="avg_win_rate",
                     color="character_class", color_discrete_map=CLASS_COLORS,
                     category_orders={"character_class": ordered_classes_wr},
                     title=f"Avg Win Rate by Class (min {min_games} games)",
                     labels={"character_class": "", "avg_win_rate": "Win Rate %"},
                     text=wr_class["avg_win_rate"].apply(lambda x: f"{x:.1f}%"),
                     template="plotly_dark")
        fig.update_traces(textposition="outside", textfont=dict(size=13))
        fig.update_layout(
            showlegend=False,
            yaxis=dict(range=[_cwr_floor, _cwr_max + (_cwr_max - _cwr_floor) * 0.2]),
        )
        fig.add_hline(y=50, line_dash="dash", line_color="rgba(255,255,255,0.25)",
                      annotation_text="50%", annotation_position="right")
        add_bar_icons(fig, ordered_classes_wr, class_icons)
        st.plotly_chart(fig, use_container_width=True)

    # ── Rating Tier Distribution by Class ─────────
    df_clean["tier"] = pd.cut(df_clean["rating"], bins=RATING_BINS, labels=RATING_LABELS, right=False)
    tier_data = df_clean.groupby(["character_class", "tier"], observed=True).size().reset_index(name="players")
    tier_totals = tier_data.groupby("character_class")["players"].sum().reset_index(name="total")
    tier_data = tier_data.merge(tier_totals, on="character_class")
    tier_data["pct"] = (tier_data["players"] / tier_data["total"] * 100).round(1)
    top_tier_order = (
        tier_data[tier_data["tier"] == "2400+"]
        .sort_values("pct", ascending=False)["character_class"].tolist()
    )
    fig = px.bar(tier_data, x="character_class", y="pct", color="tier",
                 category_orders={"character_class": top_tier_order, "tier": RATING_LABELS},
                 color_discrete_map=TIER_COLORS,
                 title="Rating Tier Distribution by Class  (sorted by % at 2400+)",
                 labels={"character_class": "", "pct": "% of Class Players", "tier": "Tier"},
                 template="plotly_dark", barmode="stack")
    fig.update_layout(yaxis=dict(ticksuffix="%"), legend=dict(title="Tier", bgcolor="rgba(0,0,0,0)"))
    st.plotly_chart(fig, use_container_width=True)

    # ── Spec Breakdown ────────────────────────────
    st.subheader("Spec Breakdown")

    raw_spec_data = (
        df_clean.groupby(["character_class", "spec"])
        .agg(players=("rating", "count"), avg_rating=("rating", "mean"))
        .reset_index()
    )
    # Build full spec list — prefer spec_icons keys (API names), fall back to ALL_SPECS
    _spec_source = list(spec_icons.keys()) if spec_icons else [
        (cls, sp) for cls, specs in ALL_SPECS.items() for sp in specs
    ]
    all_arena_specs = pd.DataFrame(
        [{"character_class": cls, "spec": sp} for (cls, sp) in _spec_source
         if SPEC_ROLES.get((cls, sp), "Unknown") in selected_roles],
        columns=["character_class", "spec"],
    )
    if all_arena_specs.empty:
        spec_data = raw_spec_data.copy()
    else:
        spec_data = all_arena_specs.merge(raw_spec_data, on=["character_class", "spec"], how="left")
    spec_data["players"] = spec_data["players"].fillna(0).astype(int)
    spec_data["avg_rating"] = spec_data["avg_rating"].fillna(0).round(0)
    spec_data["label"] = spec_data["spec"] + " (" + spec_data["character_class"] + ")"

    # Build label -> spec icon map
    label_icon_map = {
        row["label"]: spec_icons.get((row["character_class"], row["spec"]))
        for _, row in spec_data.iterrows()
    }

    legend_style = dict(title="Class", bgcolor="rgba(0,0,0,0)", font=dict(size=11))

    # ── Row 3: Spec Representation % ──────────────
    sd = spec_data.copy()
    sd["pct"] = (sd["players"] / sd["players"].sum() * 100).round(1)
    sd = sd.sort_values("pct", ascending=False)
    ordered_labels = sd["label"].tolist()
    fig = px.bar(sd, x="label", y="pct",
                 color="character_class", color_discrete_map=CLASS_COLORS,
                 category_orders={"label": ordered_labels},
                 title="Spec Representation %",
                 labels={"label": "", "pct": "% of Players"},
                 text=sd["pct"].apply(lambda x: f"{x:.1f}%"),
                 template="plotly_dark")
    fig.update_traces(textposition="outside", textfont=dict(size=13))
    fig.update_layout(showlegend=True, legend=legend_style, yaxis=dict(ticksuffix="%", range=[0, sd["pct"].max() * 1.18]))
    add_bar_icons(fig, ordered_labels, label_icon_map, bottom_margin=140, size_factor=1.0)
    st.plotly_chart(fig, use_container_width=True)

    # ── Row 4: Avg Rating by Spec (exclude no-data specs) ─
    sd = spec_data[spec_data["avg_rating"] > 0].sort_values("avg_rating", ascending=False)
    ordered_labels = sd["label"].tolist()
    _srat_min = sd["avg_rating"].min() if not sd.empty else 1500
    _srat_max = sd["avg_rating"].max() if not sd.empty else 2000
    _srat_floor = max(0, int(_srat_min // 100) * 100 - 50)
    fig = px.bar(sd, x="label", y="avg_rating",
                 color="character_class", color_discrete_map=CLASS_COLORS,
                 category_orders={"label": ordered_labels},
                 title="Avg Rating by Spec",
                 labels={"label": "", "avg_rating": "Avg Rating"},
                 text=sd["avg_rating"].astype(int).apply(lambda x: f"{x:,}"),
                 template="plotly_dark")
    fig.update_traces(textposition="outside", textfont=dict(size=13))
    fig.update_layout(
        showlegend=True, legend=legend_style,
        yaxis=dict(range=[_srat_floor, _srat_max + (_srat_max - _srat_floor) * 0.18]),
    )
    add_bar_icons(fig, ordered_labels, label_icon_map, bottom_margin=140, size_factor=1.0)
    st.plotly_chart(fig, use_container_width=True)

    # ── Row 5: Avg Win Rate by Spec (exclude no-data specs) ─
    raw_wr_spec = (
        df_wr.groupby(["character_class", "spec"])["win_rate"]
        .mean().round(1).reset_index()
        .rename(columns={"win_rate": "avg_win_rate"})
    )
    spec_wr = all_arena_specs.merge(raw_wr_spec, on=["character_class", "spec"], how="left")
    spec_wr["label"] = spec_wr["spec"] + " (" + spec_wr["character_class"] + ")"
    spec_wr = spec_wr[spec_wr["avg_win_rate"].notna()].sort_values("avg_win_rate", ascending=False)
    ordered_wr_labels = spec_wr["label"].tolist()
    _swrmin = spec_wr["avg_win_rate"].min() if not spec_wr.empty else 45
    _swrmax = spec_wr["avg_win_rate"].max() if not spec_wr.empty else 55
    _swrfloor = max(0, min(round(_swrmin) - 2, 47))
    fig = px.bar(spec_wr, x="label", y="avg_win_rate",
                 color="character_class", color_discrete_map=CLASS_COLORS,
                 category_orders={"label": ordered_wr_labels},
                 title=f"Avg Win Rate by Spec (min {min_games} games)",
                 labels={"label": "", "avg_win_rate": "Win Rate %"},
                 text=spec_wr["avg_win_rate"].apply(lambda x: f"{x:.1f}%"),
                 template="plotly_dark")
    fig.update_traces(textposition="outside", textfont=dict(size=13))
    fig.update_layout(
        showlegend=True, legend=legend_style,
        yaxis=dict(range=[_swrfloor, _swrmax + (_swrmax - _swrfloor) * 0.2]),
    )
    fig.add_hline(y=50, line_dash="dash", line_color="rgba(255,255,255,0.25)",
                  annotation_text="50%", annotation_position="right")
    add_bar_icons(fig, ordered_wr_labels, label_icon_map, bottom_margin=140, size_factor=1.0)
    st.plotly_chart(fig, use_container_width=True)

    # ── Historical Trends ─────────────────────────
    st.divider()
    st.subheader("Historical Trends")
    trend_df = load_arena_trends(mode)
    if trend_df.empty or trend_df["snapshot_date"].nunique() < 2:
        st.info("Trends will appear here once multiple days of data have been collected.")
    else:
        trend_df["snapshot_date"] = pd.to_datetime(trend_df["snapshot_date"])
        trend_df["avg_rating"] = pd.to_numeric(trend_df["avg_rating"], errors="coerce")
        trend_df["players"] = pd.to_numeric(trend_df["players"], errors="coerce")

        # Weighted avg rating per class per day
        class_trends = (
            trend_df.groupby(["snapshot_date", "character_class"])
            .apply(lambda g: pd.Series({
                "players": g["players"].sum(),
                "avg_rating": (g["avg_rating"] * g["players"]).sum() / g["players"].sum(),
            }), include_groups=False)
            .reset_index()
        )
        date_totals = class_trends.groupby("snapshot_date")["players"].sum().reset_index(name="total")
        class_trends = class_trends.merge(date_totals, on="snapshot_date")
        class_trends["pct"] = (class_trends["players"] / class_trends["total"] * 100).round(1)

        tc1, tc2 = st.columns(2)
        with tc1:
            fig = px.line(class_trends, x="snapshot_date", y="avg_rating",
                          color="character_class", color_discrete_map=CLASS_COLORS,
                          title="Avg Rating by Class Over Time",
                          labels={"snapshot_date": "", "avg_rating": "Avg Rating", "character_class": "Class"},
                          template="plotly_dark")
            fig.update_layout(legend=dict(title="Class", bgcolor="rgba(0,0,0,0)", font=dict(size=11)))
            st.plotly_chart(fig, use_container_width=True)
        with tc2:
            fig = px.line(class_trends, x="snapshot_date", y="pct",
                          color="character_class", color_discrete_map=CLASS_COLORS,
                          title="Class Representation % Over Time",
                          labels={"snapshot_date": "", "pct": "% of Players", "character_class": "Class"},
                          template="plotly_dark")
            fig.update_layout(
                legend=dict(title="Class", bgcolor="rgba(0,0,0,0)", font=dict(size=11)),
                yaxis=dict(ticksuffix="%"),
            )
            st.plotly_chart(fig, use_container_width=True)

# ─────────────────────────────────────────────
# SOLO SHUFFLE MODE
# ─────────────────────────────────────────────
else:
    color = CLASS_COLORS.get(selected_class, "#888888")

    expected_specs = [
        sp for sp in ALL_SPECS.get(selected_class, [])
        if SPEC_ROLES.get((selected_class, sp), "Unknown") in selected_roles
    ]
    spec_base = pd.DataFrame({"spec": expected_specs})

    # Spec icon map built from all expected specs
    this_spec_icons = {
        sp: spec_icons.get((selected_class, sp))
        for sp in expected_specs
    }

    # ── Row 1: player count + avg rating ──────────
    col1, col2 = st.columns(2)

    with col1:
        counts = df_clean.groupby("spec").size().reset_index(name="players")
        counts = spec_base.merge(counts, on="spec", how="left").fillna({"players": 0})
        counts["players"] = counts["players"].astype(int)
        total = counts["players"].sum()
        counts["pct"] = (counts["players"] / total * 100).round(1) if total > 0 else 0.0
        counts = counts.sort_values("pct", ascending=False)
        fig = px.bar(counts, x="spec", y="pct",
                     title="Spec Representation %",
                     labels={"spec": "", "pct": "% of Players"},
                     text=counts["pct"].apply(lambda x: f"{x:.1f}%"),
                     color_discrete_sequence=[color],
                     template="plotly_dark")
        fig.update_layout(showlegend=False, yaxis=dict(ticksuffix="%", range=[0, counts["pct"].max() * 1.18]))
        fig.update_traces(textposition="outside", textfont=dict(size=13))
        add_bar_icons(fig, counts["spec"].tolist(), this_spec_icons)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        avg_rat = (
            df_clean.groupby("spec")["rating"]
            .mean().round(0).reset_index()
            .rename(columns={"rating": "avg_rating"})
        )
        avg_rat = spec_base.merge(avg_rat, on="spec", how="left")
        avg_rat = avg_rat[avg_rat["avg_rating"].notna()].sort_values("avg_rating", ascending=False)
        _ss_rat_min = avg_rat["avg_rating"].min() if not avg_rat.empty else 1500
        _ss_rat_max = avg_rat["avg_rating"].max() if not avg_rat.empty else 2000
        _ss_rat_floor = max(0, int(_ss_rat_min // 100) * 100 - 50)
        fig = px.bar(avg_rat, x="spec", y="avg_rating",
                     title="Avg Rating by Spec",
                     labels={"spec": "", "avg_rating": "Avg Rating"},
                     text=avg_rat["avg_rating"].astype(int).apply(lambda x: f"{x:,}"),
                     color_discrete_sequence=[color],
                     template="plotly_dark")
        fig.update_traces(textposition="outside", textfont=dict(size=13))
        fig.update_layout(
            showlegend=False,
            yaxis=dict(range=[_ss_rat_floor, _ss_rat_max + (_ss_rat_max - _ss_rat_floor) * 0.18]),
        )
        add_bar_icons(fig, avg_rat["spec"].tolist(), this_spec_icons)
        st.plotly_chart(fig, use_container_width=True)

    # ── Row 2: win rate + rating distribution ─────
    col3, col4 = st.columns(2)

    with col3:
        wr = (
            df_wr.groupby("spec")["win_rate"]
            .mean().round(1).reset_index()
            .rename(columns={"win_rate": "avg_win_rate"})
        )
        wr = spec_base.merge(wr, on="spec", how="left")
        wr = wr[wr["avg_win_rate"].notna()].sort_values("avg_win_rate", ascending=False)
        _ss_wr_min = wr["avg_win_rate"].min() if not wr.empty else 45
        _ss_wr_max = wr["avg_win_rate"].max() if not wr.empty else 55
        _ss_wr_floor = max(0, min(round(_ss_wr_min) - 2, 47))
        fig = px.bar(wr, x="spec", y="avg_win_rate",
                     title=f"Avg Win Rate by Spec (min {min_games} games)",
                     labels={"spec": "", "avg_win_rate": "Win Rate %"},
                     text=wr["avg_win_rate"].apply(lambda x: f"{x:.1f}%"),
                     color_discrete_sequence=[color],
                     template="plotly_dark")
        fig.update_traces(textposition="outside", textfont=dict(size=13))
        fig.update_layout(
            showlegend=False,
            yaxis=dict(range=[_ss_wr_floor, _ss_wr_max + (_ss_wr_max - _ss_wr_floor) * 0.2]),
        )
        add_bar_icons(fig, wr["spec"].tolist(), this_spec_icons)
        st.plotly_chart(fig, use_container_width=True)

    with col4:
        fig = px.histogram(df_clean, x="rating", color="spec",
                           barmode="overlay", opacity=0.7, nbins=40,
                           histnorm="percent",
                           title="Rating Distribution by Spec",
                           labels={"rating": "Rating", "percent": "% of Spec"},
                           template="plotly_dark")
        fig.update_layout(
            legend=dict(title="Spec", bgcolor="rgba(0,0,0,0)"),
            yaxis_title="% of Spec",
        )
        st.plotly_chart(fig, use_container_width=True)

    # ── Row 3: Representation vs Win Rate scatter ──
    scatter_data = counts[["spec", "pct", "players"]].merge(
        wr[["spec", "avg_win_rate"]], on="spec", how="left"
    )
    scatter_data["avg_win_rate"] = scatter_data["avg_win_rate"].fillna(0)
    fig = px.scatter(scatter_data, x="pct", y="avg_win_rate",
                     text="spec", size="players", size_max=40,
                     title="Representation % vs Avg Win Rate",
                     labels={"pct": "Representation %", "avg_win_rate": "Avg Win Rate %"},
                     color_discrete_sequence=[color],
                     template="plotly_dark")
    fig.update_traces(textposition="top center")
    fig.add_hline(y=50, line_dash="dash", line_color="rgba(255,255,255,0.25)",
                  annotation_text="50% (balanced)", annotation_position="right")
    fig.update_layout(showlegend=False)
    st.plotly_chart(fig, use_container_width=True)

    # ── Rating Tier Distribution by Spec ──────────
    df_clean["tier"] = pd.cut(df_clean["rating"], bins=RATING_BINS, labels=RATING_LABELS, right=False)
    spec_tier = df_clean.groupby(["spec", "tier"], observed=True).size().reset_index(name="players")
    spec_tier_totals = spec_tier.groupby("spec")["players"].sum().reset_index(name="total")
    spec_tier = spec_tier.merge(spec_tier_totals, on="spec")
    spec_tier["pct"] = (spec_tier["players"] / spec_tier["total"] * 100).round(1)
    top_spec_order = (
        spec_tier[spec_tier["tier"] == "2400+"]
        .sort_values("pct", ascending=False)["spec"].tolist()
    )
    fig = px.bar(spec_tier, x="spec", y="pct", color="tier",
                 category_orders={"spec": top_spec_order, "tier": RATING_LABELS},
                 color_discrete_map=TIER_COLORS,
                 title="Rating Tier Distribution by Spec  (sorted by % at 2400+)",
                 labels={"spec": "", "pct": "% of Spec Players", "tier": "Tier"},
                 template="plotly_dark", barmode="stack")
    fig.update_layout(yaxis=dict(ticksuffix="%"), legend=dict(title="Tier", bgcolor="rgba(0,0,0,0)"))
    st.plotly_chart(fig, use_container_width=True)

    # ── Historical Trends ─────────────────────────
    st.divider()
    st.subheader("Historical Trends")
    trend_df = load_shuffle_trends(selected_class)
    if trend_df.empty or trend_df["snapshot_date"].nunique() < 2:
        st.info("Trends will appear here once multiple days of data have been collected.")
    else:
        trend_df["snapshot_date"] = pd.to_datetime(trend_df["snapshot_date"])
        trend_df["avg_rating"] = pd.to_numeric(trend_df["avg_rating"], errors="coerce")
        trend_df["avg_win_rate"] = pd.to_numeric(trend_df["avg_win_rate"], errors="coerce")
        trend_df["players"] = pd.to_numeric(trend_df["players"], errors="coerce")
        if selected_roles:
            trend_df = trend_df[trend_df.apply(
                lambda r: SPEC_ROLES.get((r["character_class"], r["spec"]), "Unknown") in selected_roles, axis=1
            )]
        date_totals = trend_df.groupby("snapshot_date")["players"].sum().reset_index(name="total")
        trend_df = trend_df.merge(date_totals, on="snapshot_date")
        trend_df["pct"] = (trend_df["players"] / trend_df["total"] * 100).round(1)

        ts1, ts2 = st.columns(2)
        with ts1:
            fig = px.line(trend_df, x="snapshot_date", y="avg_rating", color="spec",
                          title="Avg Rating by Spec Over Time",
                          labels={"snapshot_date": "", "avg_rating": "Avg Rating", "spec": "Spec"},
                          color_discrete_sequence=px.colors.qualitative.Set2,
                          template="plotly_dark")
            fig.update_layout(legend=dict(title="Spec", bgcolor="rgba(0,0,0,0)"))
            st.plotly_chart(fig, use_container_width=True)
        with ts2:
            fig = px.line(trend_df, x="snapshot_date", y="pct", color="spec",
                          title="Spec Representation % Over Time",
                          labels={"snapshot_date": "", "pct": "% of Players", "spec": "Spec"},
                          color_discrete_sequence=px.colors.qualitative.Set2,
                          template="plotly_dark")
            fig.update_layout(
                legend=dict(title="Spec", bgcolor="rgba(0,0,0,0)"),
                yaxis=dict(ticksuffix="%"),
            )
            st.plotly_chart(fig, use_container_width=True)
