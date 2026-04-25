import os
import requests
import streamlit as st
import pandas as pd
import plotly.express as px
from supabase import create_client
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))

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
    "Warrior":      "#C69B3A",
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

# ─────────────────────────────────────────────
# BLIZZARD ICONS
# ─────────────────────────────────────────────
@st.cache_data(ttl=82800, show_spinner="Loading WoW icons...")
def load_blizzard_icons():
    """Fetch class + spec icon URLs from Blizzard media API. Cached 23 h."""
    token = requests.post(
        "https://us.battle.net/oauth/token",
        data={"grant_type": "client_credentials"},
        auth=(os.environ["BLIZZARD_CLIENT_ID"], os.environ["BLIZZARD_CLIENT_SECRET"]),
    ).json()["access_token"]

    headers = {"Authorization": f"Bearer {token}"}
    params  = {"namespace": "static-us", "locale": "en_US"}
    base    = "https://us.api.blizzard.com/data/wow"

    def icon_from_media(url):
        try:
            assets = requests.get(url, headers=headers, params=params, timeout=10).json().get("assets", [])
            return next((a["value"] for a in assets if a["key"] == "icon"), None)
        except Exception:
            return None

    class_icons = {}   # {class_name: url}
    spec_icons  = {}   # {(class_name, spec_name): url}

    all_classes = requests.get(f"{base}/playable-class/index", headers=headers, params=params).json()

    for cls in all_classes.get("classes", []):
        cls_id, cls_name = cls["id"], cls["name"]

        class_icons[cls_name] = icon_from_media(f"{base}/media/playable-class/{cls_id}")

        cls_data = requests.get(f"{base}/playable-class/{cls_id}", headers=headers, params=params).json()
        for spec in cls_data.get("specializations", []):
            spec_id, spec_name = spec["id"], spec["name"]
            spec_icons[(cls_name, spec_name)] = icon_from_media(f"{base}/media/playable-specialization/{spec_id}")

    return class_icons, spec_icons


def add_bar_icons(fig, categories, icon_map, bottom_margin=95):
    """Overlay icons below each bar in a vertical bar chart."""
    n = len(categories)
    if not n:
        return fig
    icon_w = min(0.065, 0.75 / n)
    for i, cat in enumerate(categories):
        url = icon_map.get(cat)
        if url:
            fig.add_layout_image(
                source=url,
                x=(i + 0.5) / n,
                y=-0.05,
                xref="paper", yref="paper",
                sizex=icon_w, sizey=icon_w * 1.5,
                xanchor="center", yanchor="top",
                layer="above",
            )
    fig.update_layout(margin=dict(b=bottom_margin))
    return fig

# ─────────────────────────────────────────────
# SUPABASE DATA
# ─────────────────────────────────────────────
@st.cache_resource
def get_supabase():
    return create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

@st.cache_data(ttl=3600, show_spinner="Loading leaderboard data...")
def load_bracket(bracket: str) -> pd.DataFrame:
    resp = (
        get_supabase()
        .table("pvp_leaderboard")
        .select("character_class,spec,rating,wins,losses,played,faction")
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
        .select("character_class,spec,rating,wins,losses,played,faction,bracket")
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
mode = st.sidebar.radio("Bracket Type", ["Arena", "Solo Shuffle"])

if mode == "Arena":
    bracket_label = st.sidebar.selectbox("Bracket", ["2v2 Arena", "3v3 Arena"])
    bracket = "2v2" if "2v2" in bracket_label else "3v3"
    df = load_bracket(bracket)
    page_title = f"{bracket_label} — Class & Spec Analytics"
else:
    selected_class = st.sidebar.selectbox("Class", SHUFFLE_CLASSES)
    df = load_shuffle_class(selected_class)
    page_title = f"Solo Shuffle — {selected_class}"

min_games = st.sidebar.slider("Min games played (win rate filter)", 0, 100, 20, step=5)

st.sidebar.divider()
st.sidebar.caption("Data refreshes daily at 6 AM EST via GitHub Actions.")

# ─────────────────────────────────────────────
# LOAD ICONS
# ─────────────────────────────────────────────
class_icons, spec_icons = load_blizzard_icons()

# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
st.title(page_title)

if df.empty:
    st.warning("No data available.")
    st.stop()

df_clean = df.dropna(subset=["character_class", "spec"])
df_wr    = df_clean[df_clean["played"] >= min_games]

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
if mode == "Arena":

    # ── Row 1: player count + avg rating by class ─
    col1, col2 = st.columns(2)

    with col1:
        counts = (
            df_clean.groupby("character_class").size()
            .reset_index(name="players")
            .sort_values("players", ascending=False)
        )
        fig = px.bar(counts, x="character_class", y="players",
                     color="character_class", color_discrete_map=CLASS_COLORS,
                     title="Player Count by Class",
                     labels={"character_class": "", "players": "Players"},
                     template="plotly_dark")
        fig.update_layout(showlegend=False, xaxis_tickangle=-30)
        add_bar_icons(fig, counts["character_class"].tolist(), class_icons)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        avg_rat = (
            df_clean.groupby("character_class")["rating"]
            .mean().round(0).reset_index()
            .rename(columns={"rating": "avg_rating"})
            .sort_values("avg_rating", ascending=False)
        )
        fig = px.bar(avg_rat, x="character_class", y="avg_rating",
                     color="character_class", color_discrete_map=CLASS_COLORS,
                     title="Avg Rating by Class",
                     labels={"character_class": "", "avg_rating": "Avg Rating"},
                     template="plotly_dark")
        fig.update_layout(showlegend=False, xaxis_tickangle=-30)
        add_bar_icons(fig, avg_rat["character_class"].tolist(), class_icons)
        st.plotly_chart(fig, use_container_width=True)

    # ── Row 2: win rate + faction split ───────────
    col3, col4 = st.columns(2)

    with col3:
        wr = (
            df_wr.groupby("character_class")["win_rate"]
            .mean().round(1).reset_index()
            .rename(columns={"win_rate": "avg_win_rate"})
            .sort_values("avg_win_rate", ascending=False)
        )
        fig = px.bar(wr, x="character_class", y="avg_win_rate",
                     color="character_class", color_discrete_map=CLASS_COLORS,
                     title=f"Avg Win Rate by Class (min {min_games} games)",
                     labels={"character_class": "", "avg_win_rate": "Win Rate %"},
                     template="plotly_dark")
        fig.update_layout(showlegend=False, xaxis_tickangle=-30)
        add_bar_icons(fig, wr["character_class"].tolist(), class_icons)
        st.plotly_chart(fig, use_container_width=True)

    with col4:
        faction = (
            df_clean.groupby(["character_class", "faction"])
            .size().reset_index(name="count")
        )
        fig = px.bar(faction, x="character_class", y="count", color="faction",
                     barmode="stack", title="Faction Split by Class",
                     color_discrete_map={"ALLIANCE": "#4a90d9", "HORDE": "#cc3333"},
                     labels={"character_class": "", "count": "Players"},
                     template="plotly_dark")
        fig.update_layout(xaxis_tickangle=-30)
        st.plotly_chart(fig, use_container_width=True)

    # ── Row 3: spec breakdown ──────────────────────
    st.subheader("Spec Breakdown")

    spec_data = (
        df_clean.groupby(["character_class", "spec"])
        .agg(players=("rating", "count"), avg_rating=("rating", "mean"))
        .reset_index()
    )
    spec_data["avg_rating"] = spec_data["avg_rating"].round(0)
    spec_data["label"] = spec_data["spec"] + " (" + spec_data["character_class"] + ")"

    # Build label -> spec icon map
    label_icon_map = {
        row["label"]: spec_icons.get((row["character_class"], row["spec"]))
        for _, row in spec_data.iterrows()
    }

    col5, col6 = st.columns(2)

    with col5:
        sd = spec_data.sort_values("players", ascending=False)
        fig = px.bar(sd, x="label", y="players",
                     color="character_class", color_discrete_map=CLASS_COLORS,
                     title="Player Count by Spec",
                     labels={"label": "", "players": "Players"},
                     template="plotly_dark")
        fig.update_layout(showlegend=False, xaxis_tickangle=-45)
        add_bar_icons(fig, sd["label"].tolist(), label_icon_map, bottom_margin=110)
        st.plotly_chart(fig, use_container_width=True)

    with col6:
        sd = spec_data.sort_values("avg_rating", ascending=False)
        fig = px.bar(sd, x="label", y="avg_rating",
                     color="character_class", color_discrete_map=CLASS_COLORS,
                     title="Avg Rating by Spec",
                     labels={"label": "", "avg_rating": "Avg Rating"},
                     template="plotly_dark")
        fig.update_layout(showlegend=False, xaxis_tickangle=-45)
        add_bar_icons(fig, sd["label"].tolist(), label_icon_map, bottom_margin=110)
        st.plotly_chart(fig, use_container_width=True)

# ─────────────────────────────────────────────
# SOLO SHUFFLE MODE
# ─────────────────────────────────────────────
else:
    color = CLASS_COLORS.get(selected_class, "#888888")

    # Spec icon map for this class
    this_spec_icons = {
        spec_name: spec_icons.get((selected_class, spec_name))
        for spec_name in df_clean["spec"].dropna().unique()
    }

    # ── Row 1: player count + avg rating ──────────
    col1, col2 = st.columns(2)

    with col1:
        counts = (
            df_clean.groupby("spec").size()
            .reset_index(name="players")
            .sort_values("players", ascending=False)
        )
        fig = px.bar(counts, x="spec", y="players",
                     title="Player Count by Spec",
                     labels={"spec": "", "players": "Players"},
                     color_discrete_sequence=[color],
                     template="plotly_dark")
        fig.update_layout(showlegend=False)
        add_bar_icons(fig, counts["spec"].tolist(), this_spec_icons)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        avg_rat = (
            df_clean.groupby("spec")["rating"]
            .mean().round(0).reset_index()
            .rename(columns={"rating": "avg_rating"})
            .sort_values("avg_rating", ascending=False)
        )
        fig = px.bar(avg_rat, x="spec", y="avg_rating",
                     title="Avg Rating by Spec",
                     labels={"spec": "", "avg_rating": "Avg Rating"},
                     color_discrete_sequence=[color],
                     template="plotly_dark")
        fig.update_layout(showlegend=False)
        add_bar_icons(fig, avg_rat["spec"].tolist(), this_spec_icons)
        st.plotly_chart(fig, use_container_width=True)

    # ── Row 2: win rate + rating distribution ─────
    col3, col4 = st.columns(2)

    with col3:
        wr = (
            df_wr.groupby("spec")["win_rate"]
            .mean().round(1).reset_index()
            .rename(columns={"win_rate": "avg_win_rate"})
            .sort_values("avg_win_rate", ascending=False)
        )
        fig = px.bar(wr, x="spec", y="avg_win_rate",
                     title=f"Avg Win Rate by Spec (min {min_games} games)",
                     labels={"spec": "", "avg_win_rate": "Win Rate %"},
                     color_discrete_sequence=[color],
                     template="plotly_dark")
        fig.update_layout(showlegend=False)
        add_bar_icons(fig, wr["spec"].tolist(), this_spec_icons)
        st.plotly_chart(fig, use_container_width=True)

    with col4:
        fig = px.box(df_clean, x="spec", y="rating",
                     title="Rating Distribution by Spec",
                     labels={"spec": "Spec", "rating": "Rating"},
                     color_discrete_sequence=[color],
                     template="plotly_dark")
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    # ── Row 3: faction split ───────────────────────
    faction = (
        df_clean.groupby(["spec", "faction"])
        .size().reset_index(name="count")
    )
    fig = px.bar(faction, x="spec", y="count", color="faction",
                 barmode="stack", title="Faction Split by Spec",
                 color_discrete_map={"ALLIANCE": "#4a90d9", "HORDE": "#cc3333"},
                 labels={"spec": "Spec", "count": "Players"},
                 template="plotly_dark")
    st.plotly_chart(fig, use_container_width=True)
