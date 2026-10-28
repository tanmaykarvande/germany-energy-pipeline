import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from supabase import create_client, Client

# ---------------------------------------------------------------------------
# Page configuration (must be the first Streamlit call)
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Smart Factory: Understanding the Energy Grid",
    layout="wide",
    page_icon="⚡",
)

TZ = "Europe/Berlin"
BLACK = "#000000"
ACTUAL_COLOR = "#1f3a5f"      # deep navy
FORECAST_COLOR = "#d97706"    # amber
ZONE_COLORS = {
    "Expensive": "#c0392b",
    "Normal": "#e0a100",
    "Cheap": "#2e8b57",
    "(?)": "#d9d9d9",
}

# ---------------------------------------------------------------------------
# Styling: white background, black text, bordered charts and cards
# ---------------------------------------------------------------------------
CSS = """
.stApp, [data-testid="stAppViewContainer"], [data-testid="stHeader"] {
    background-color: #ffffff !important;
}
[data-testid="stSidebar"], [data-testid="stSidebar"] > div {
    background-color: #f4f4f4 !important;
    border-right: 1px solid #000000;
}
.stApp, .stApp p, .stApp span, .stApp label, .stApp li,
h1, h2, h3, h4, h5, h6,
[data-testid="stMarkdownContainer"], [data-testid="stCaptionContainer"] {
    color: #000000 !important;
}
[data-testid="stSidebar"] [data-baseweb="select"],
[data-testid="stSidebar"] [data-baseweb="select"] > div,
[data-testid="stSidebar"] [data-baseweb="select"] input {
    background-color: #ffffff !important;
    color: #000000 !important;
}
[data-testid="stSidebar"] [data-baseweb="select"] > div {
    border: 2px solid #0a1a4f !important;
    border-radius: 8px !important;
}
[data-baseweb="select"] svg {
    fill: #0a1a4f !important;
    color: #0a1a4f !important;
}
.stApp [data-baseweb="select"] [data-baseweb="tag"],
.stApp span[data-baseweb="tag"] {
    background-color: #e3f2ff !important;
    border: 1px solid #0a1a4f !important;
    border-radius: 6px !important;
    color: #0a1a4f !important;
    font-weight: 600 !important;
}
[data-baseweb="select"] [data-baseweb="tag"] * {
    background-color: transparent !important;
    color: #0a1a4f !important;
    fill: #0a1a4f !important;
    font-weight: 600 !important;
}
[data-baseweb="popover"], [data-baseweb="popover"] ul,
[data-baseweb="popover"] li, [data-baseweb="menu"] {
    background-color: #ffffff !important;
    color: #000000 !important;
}
[data-baseweb="popover"] li:hover { background-color: #e3f2ff !important; }

/* ---------- Hero header ---------- */
.hero {
    display: flex; flex-wrap: wrap; align-items: center;
    justify-content: space-between; gap: 28px;
    padding: 38px 42px;
    border: 3px solid transparent;
    border-radius: 18px;
    background:
        linear-gradient(135deg, #e3f2ff 0%, #ffffff 70%) padding-box,
        linear-gradient(90deg, #d6126f, #7b2ff7) border-box;
}
.hero-left { flex: 1 1 440px; }
.hero-right { flex: 0 1 340px; min-width: 260px; }
.stApp .hero .hero-eyebrow {
    font-size: 0.8rem; font-weight: 700; letter-spacing: 0.14em;
    text-transform: uppercase; color: #7b2ff7 !important;
}
.stApp .hero .hero-title {
    font-size: clamp(2rem, 4.2vw, 3.3rem); font-weight: 800;
    line-height: 1.1; color: #0a1a4f !important; margin: 8px 0 14px 0;
}
.stApp .hero .hero-sub {
    font-size: 1.05rem; color: #0a1a4f !important;
    max-width: 640px; margin-bottom: 22px;
}
.stApp .hero .pill-grad {
    display: inline-block; color: #ffffff !important; font-weight: 700;
    background: linear-gradient(90deg, #d6126f, #7b2ff7);
    padding: 10px 20px; border-radius: 8px; margin: 0 10px 10px 0;
}
.stApp .hero .pill-out {
    display: inline-block; color: #0a1a4f !important; font-weight: 600;
    background: #ffffff; border: 2px solid #0a1a4f;
    padding: 8px 16px; border-radius: 8px; margin: 0 10px 10px 0;
}
.hero-frame {
    border: 3px solid #0a1a4f; border-radius: 12px;
    background: #ffffff; padding: 12px 14px;
}
.stApp .hero .hero-frame-label {
    font-size: 0.72rem; font-weight: 700; letter-spacing: 0.08em;
    text-transform: uppercase; color: #0a1a4f !important;
}
.stApp .hero .hero-frame-foot {
    font-size: 0.78rem; color: #444444 !important; margin-top: 4px;
}
[data-testid="stPlotlyChart"] {
    border: 1px solid #000000;
    border-radius: 6px;
    padding: 8px;
    background-color: #ffffff;
}
hr { border-color: #000000 !important; opacity: 0.25; }

.kpi {
    border: 1px solid #000000;
    border-radius: 6px;
    padding: 14px 16px;
    background: #ffffff;
    height: 100%;
}
.kpi-label { font-size: 0.85rem; font-weight: 600; letter-spacing: 0.02em; text-transform: uppercase; }
.kpi-value { font-size: 1.9rem; font-weight: 700; line-height: 1.25; }
.kpi-sub   { font-size: 0.8rem; color: #444444 !important; }

.opt-box {
    border: 2px solid #000000;
    border-radius: 8px;
    padding: 18px 22px;
    background: #ffffff;
}
.opt-title {
    font-size: 1.35rem;
    font-weight: 800;
    border-bottom: 2px solid #000000;
    padding-bottom: 8px;
    margin-bottom: 12px;
}
.opt-row {
    display: flex;
    justify-content: space-between;
    flex-wrap: wrap;
    padding: 6px 0;
    border-bottom: 1px solid #dddddd;
    font-size: 1.05rem;
}
.opt-row:last-child { border-bottom: none; }
.opt-label { font-weight: 600; }
.opt-value { font-weight: 700; }
"""
st.markdown(f"<style>{CSS}</style>", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Database connection
# ---------------------------------------------------------------------------
@st.cache_resource
def get_client() -> Client:
    url = st.secrets["SUPABASE_URL"].strip()
    key = st.secrets["SUPABASE_KEY"].strip()
    return create_client(url, key)


def categorize(price: float) -> str:
    if price > 120:
        return "Expensive"
    if price < 60:
        return "Cheap"
    return "Normal"


@st.cache_data(ttl=60)
def load_all_data():
    sb = get_client()
    res_a = (
        sb.table("energy_prices").select("*")
        .order("timestamp", desc=True).limit(1500).execute()
    )
    res_p = (
        sb.table("price_predictions").select("*")
        .order("timestamp", desc=True).limit(300).execute()
    )

    # Actual prices
    df_a = pd.DataFrame(res_a.data)
    if not df_a.empty:
        df_a["timestamp"] = pd.to_datetime(df_a["timestamp"], utc=True).dt.tz_convert(TZ)
        df_a["price_eur_mwh"] = pd.to_numeric(df_a["price_eur_mwh"], errors="coerce")
        df_a = df_a.sort_values("timestamp").reset_index(drop=True)
        df_a["price_eur_mwh"] = df_a["price_eur_mwh"].ffill().fillna(0)
        df_a["Year"] = df_a["timestamp"].dt.year
        df_a["Month"] = df_a["timestamp"].dt.strftime("%B")
        df_a["Day"] = df_a["timestamp"].dt.day_name()
        df_a["Hour"] = df_a["timestamp"].dt.hour
        df_a["Status_Zone"] = df_a["price_eur_mwh"].apply(categorize)

    # Forecast
    df_p = pd.DataFrame(res_p.data) if res_p.data else pd.DataFrame()
    if not df_p.empty:
        df_p["timestamp"] = pd.to_datetime(df_p["timestamp"], utc=True).dt.tz_convert(TZ)
        df_p["predicted_price"] = pd.to_numeric(df_p["predicted_price"], errors="coerce").ffill()
        df_p = df_p.sort_values("timestamp").reset_index(drop=True)

    return df_a, df_p


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def card(label: str, value: str, sub: str = "") -> str:
    return (
        f'<div class="kpi"><div class="kpi-label">{label}</div>'
        f'<div class="kpi-value">{value}</div>'
        f'<div class="kpi-sub">{sub}</div></div>'
    )


def style_fig(fig, x_title=None, y_title=None, height=430, axes=True):
    """Apply the white / black / bordered look to a Plotly figure."""
    fig.update_layout(
        template="plotly_white",
        paper_bgcolor="white",
        plot_bgcolor="white",
        font=dict(color=BLACK, size=13),
        height=height,
        margin=dict(l=60, r=30, t=50, b=60),
        legend=dict(
            orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0,
            bgcolor="white", bordercolor=BLACK, borderwidth=1,
            font=dict(color=BLACK),
        ),
        hoverlabel=dict(bgcolor="white", font_color=BLACK),
    )
    if axes:
        axis_style = dict(
            showline=True, linecolor=BLACK, linewidth=1, mirror=True,
            gridcolor="#e6e6e6", ticks="outside",
            tickfont=dict(color=BLACK), title_font=dict(color=BLACK),
        )
        fig.update_xaxes(title_text=x_title, **axis_style)
        fig.update_yaxes(title_text=y_title, **axis_style)
    return fig


def compute_reliability(actuals: pd.DataFrame, preds: pd.DataFrame):
    """Return (merged dataframe, reliability %, MAE) for overlapping hours."""
    if preds.empty:
        return pd.DataFrame(), None, None
    merged = pd.merge(
        actuals[["timestamp", "price_eur_mwh"]],
        preds[["timestamp", "predicted_price"]],
        on="timestamp", how="inner",
    )
    if merged.empty:
        return merged, None, None
    mae = (merged["price_eur_mwh"] - merged["predicted_price"]).abs().mean()
    mean_price = actuals["price_eur_mwh"].mean()
    reliability = max(0.0, 100 - (mae / mean_price * 100)) if mean_price else 0.0
    return merged, reliability, mae


def sparkline_svg(values, w=320, h=110, pad=8) -> str:
    """Tiny inline SVG line chart for the hero panel."""
    lo, hi = min(values), max(values)
    span = (hi - lo) or 1
    n = len(values) - 1
    pts = [
        (pad + i * (w - 2 * pad) / n, pad + (hi - v) / span * (h - 2 * pad))
        for i, v in enumerate(values)
    ]
    line = " ".join(f"{x:.1f},{y:.1f}" for x, y in pts)
    area = f"{pad},{h - pad} {line} {w - pad},{h - pad}"
    lx, ly = pts[-1]
    return (
        f'<svg viewBox="0 0 {w} {h}" width="100%" xmlns="http://www.w3.org/2000/svg">'
        f'<polygon points="{area}" fill="#d6126f" fill-opacity="0.12"/>'
        f'<polyline points="{line}" fill="none" stroke="#0a1a4f" stroke-width="2.5" stroke-linejoin="round"/>'
        f'<circle cx="{lx:.1f}" cy="{ly:.1f}" r="5" fill="#d6126f" stroke="#ffffff" stroke-width="2"/>'
        "</svg>"
    )


def build_hero(actuals: pd.DataFrame, now: pd.Timestamp) -> str:
    live_pill = ""
    panel = ""
    if not actuals.empty:
        past = actuals[actuals["timestamp"] <= now]
        base = past if len(past) >= 2 else actuals
        live = base.iloc[-1]
        zone = categorize(live["price_eur_mwh"])
        live_pill = (
            f'<span class="pill-grad">Live now: {live["price_eur_mwh"]:,.0f} € / MWh &nbsp;|&nbsp; {zone}</span>'
        )
        recent = base["price_eur_mwh"].tail(48).tolist()
        if len(recent) >= 2:
            panel = (
                '<div class="hero-right"><div class="hero-frame">'
                '<div class="hero-frame-label">Last 48 hours, €/MWh</div>'
                f"{sparkline_svg(recent)}"
                f'<div class="hero-frame-foot">Low {min(recent):,.0f} € &nbsp;|&nbsp; High {max(recent):,.0f} €</div>'
                "</div></div>"
            )
    return (
        '<div class="hero"><div class="hero-left">'
        '<div class="hero-eyebrow">Energy intelligence &middot; Germany</div>'
        '<div class="hero-title">Smart Factory: Understanding the Energy Grid</div>'
        '<div class="hero-sub">Hourly day-ahead electricity prices with a 48-hour forecast, '
        "so heavy machinery can run when power is cheapest.</div>"
        f"{live_pill}"
        '<span class="pill-out">Source: SMARD, Bundesnetzagentur</span>'
        '<span class="pill-out">Updated daily</span>'
        '<span class="pill-out">Berlin time</span>'
        f"</div>{panel}</div>"
    )


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------
def main():
    actuals, preds = load_all_data()

    now = pd.Timestamp.now(tz=TZ)
    st.markdown(build_hero(actuals, now), unsafe_allow_html=True)
    st.write("")

    if actuals.empty:
        st.warning("No price data found yet. Run the 'Daily data sync' workflow on GitHub and refresh.")
        return

    # ---- Sidebar filters ----
    st.sidebar.header("Filters")
    years = sorted(actuals["Year"].unique())
    selected_year = st.sidebar.multiselect("Year", years, default=years)
    months = list(actuals["Month"].unique())
    selected_month = st.sidebar.multiselect("Month", months, default=months)

    mask = actuals["Year"].isin(selected_year) & actuals["Month"].isin(selected_month)
    fa = actuals.loc[mask].copy().reset_index(drop=True)
    fa["Hours"] = 1

    if fa.empty:
        st.warning("No data for the selected filters. Choose at least one year and one month.")
        return

    now = pd.Timestamp.now(tz=TZ)
    merged, reliability, mae = compute_reliability(actuals, preds)

    # ---- KPI cards ----
    past = fa[fa["timestamp"] <= now]
    live_row = past.iloc[-1] if not past.empty else fa.iloc[-1]
    peak_row = fa.loc[fa["price_eur_mwh"].idxmax()]
    last_ts = actuals["timestamp"].max()

    c1, c2, c3, c4 = st.columns(4)
    c1.markdown(
        card("Live Market Price", f"{live_row['price_eur_mwh']:,.0f} €",
             f"€/MWh at {live_row['timestamp'].strftime('%H:%M, %d %b')}"),
        unsafe_allow_html=True,
    )
    c2.markdown(
        card("Filtered Average", f"{fa['price_eur_mwh'].mean():,.0f} €", "€/MWh for selected period"),
        unsafe_allow_html=True,
    )
    c3.markdown(
        card("Peak Price", f"{peak_row['price_eur_mwh']:,.0f} €",
             f"€/MWh on {peak_row['timestamp'].strftime('%d %b, %H:%M')}"),
        unsafe_allow_html=True,
    )
    c4.markdown(
        card("Data Last Updated", last_ts.strftime("%d %b %Y"),
             f"Latest hour loaded: {last_ts.strftime('%H:%M %Z')}"),
        unsafe_allow_html=True,
    )

    # ---- Optimal window box ----
    st.write("")
    future = preds[preds["timestamp"] > now] if not preds.empty else pd.DataFrame()
    if not future.empty:
        best = future.sort_values("predicted_price").iloc[0]
        when = best["timestamp"].strftime("%H:%M, %A, %d %B %Y")
        price_txt = f"{best['predicted_price']:,.0f} € / MWh"
        rel_txt = f"{reliability:.1f}%" if reliability is not None else "Syncing..."
        st.markdown(
            '<div class="opt-box">'
            '<div class="opt-title">Optimal Window</div>'
            f'<div class="opt-row"><span class="opt-label">Estimated Time and Day</span><span class="opt-value">{when}</span></div>'
            f'<div class="opt-row"><span class="opt-label">Estimated Price</span><span class="opt-value">{price_txt}</span></div>'
            f'<div class="opt-row"><span class="opt-label">Forecast Reliability</span><span class="opt-value">{rel_txt}</span></div>'
            "</div>",
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            '<div class="opt-box"><div class="opt-title">Optimal Window</div>'
            '<div class="opt-row"><span class="opt-label">No upcoming forecast available yet</span></div></div>',
            unsafe_allow_html=True,
        )

    # ---- Trend chart ----
    st.divider()
    st.subheader("Main Energy Trend")
    fig_trend = go.Figure()
    fig_trend.add_trace(go.Scatter(
        x=fa["timestamp"], y=fa["price_eur_mwh"], mode="lines",
        name="Actual price", line=dict(color=ACTUAL_COLOR, width=2),
        hovertemplate="%{x|%d %b %H:%M}<br>%{y:,.1f} €/MWh<extra>Actual</extra>",
    ))
    if not preds.empty:
        fut = preds[preds["timestamp"] > fa["timestamp"].max()]
        if not fut.empty:
            fig_trend.add_trace(go.Scatter(
                x=fut["timestamp"], y=fut["predicted_price"], mode="lines",
                name="Forecast", line=dict(color=FORECAST_COLOR, width=2, dash="dash"),
                hovertemplate="%{x|%d %b %H:%M}<br>%{y:,.1f} €/MWh<extra>Forecast</extra>",
            ))
    low_row = fa.loc[fa["price_eur_mwh"].idxmin()]
    fig_trend.add_trace(go.Scatter(
        x=[peak_row["timestamp"]], y=[peak_row["price_eur_mwh"]],
        mode="markers+text", name="Peak",
        marker=dict(color=ZONE_COLORS["Expensive"], size=11, line=dict(color=BLACK, width=1)),
        text=[f"Peak: {peak_row['price_eur_mwh']:,.0f} €"], textposition="top center",
        textfont=dict(color=BLACK),
    ))
    fig_trend.add_trace(go.Scatter(
        x=[low_row["timestamp"]], y=[low_row["price_eur_mwh"]],
        mode="markers+text", name="Lowest",
        marker=dict(color=ZONE_COLORS["Cheap"], size=11, line=dict(color=BLACK, width=1)),
        text=[f"Low: {low_row['price_eur_mwh']:,.0f} €"], textposition="bottom center",
        textfont=dict(color=BLACK),
    ))
    fig_trend.add_hline(y=120, line_dash="dot", line_color=ZONE_COLORS["Expensive"],
                        annotation_text="Expensive above 120 €", annotation_position="top left",
                        annotation_font_color=BLACK)
    fig_trend.add_hline(y=60, line_dash="dot", line_color=ZONE_COLORS["Cheap"],
                        annotation_text="Cheap below 60 €", annotation_position="bottom left",
                        annotation_font_color=BLACK)
    style_fig(fig_trend, x_title="Time", y_title="Price (€/MWh)", height=480)
    st.plotly_chart(fig_trend, use_container_width=True)

    # ---- Actual vs predicted ----
    st.divider()
    st.subheader("Model Performance: Forecast vs. Actual (Historical)")
    if not merged.empty:
        fig_acc = go.Figure()
        fig_acc.add_trace(go.Scatter(
            x=merged["timestamp"], y=merged["price_eur_mwh"], mode="lines",
            name="Actual price", line=dict(color=ACTUAL_COLOR, width=2),
        ))
        fig_acc.add_trace(go.Scatter(
            x=merged["timestamp"], y=merged["predicted_price"], mode="lines",
            name="Forecast", line=dict(color=FORECAST_COLOR, width=2, dash="dot"),
        ))
        style_fig(fig_acc, x_title="Time", y_title="Price (€/MWh)")
        st.plotly_chart(fig_acc, use_container_width=True)
        st.caption(
            f"Mean absolute error: {mae:,.1f} €/MWh over {len(merged):,} overlapping hours "
            f"(reliability {reliability:.1f}%)."
        )
    else:
        st.info("The comparison chart will fill in once actual prices catch up with the stored forecasts.")

    # ---- Sunburst and treemap ----
    st.divider()
    st.caption("Price zones: Cheap = below 60 €/MWh, Normal = 60 to 120 €/MWh, Expensive = above 120 €/MWh.")
    col_l, col_r = st.columns(2)
    with col_l:
        st.subheader("Hierarchy (Sunburst)")
        sun_df = fa.assign(Year=fa["Year"].astype(str))
        fig_sun = px.sunburst(
            sun_df, path=["Year", "Month", "Status_Zone"], values="Hours",
            color="Status_Zone", color_discrete_map=ZONE_COLORS,
        )
        fig_sun.update_traces(
            textinfo="label+percent parent",
            marker=dict(line=dict(color="white", width=1)),
            hovertemplate="%{label}<br>%{value} hours<extra></extra>",
        )
        style_fig(fig_sun, height=450, axes=False)
        st.plotly_chart(fig_sun, use_container_width=True)
    with col_r:
        st.subheader("Distribution (Treemap)")
        fig_tree = px.treemap(
            fa, path=["Status_Zone"], values="Hours",
            color="Status_Zone", color_discrete_map=ZONE_COLORS,
        )
        fig_tree.update_traces(
            textinfo="label+value+percent root",
            textfont=dict(size=16, color=BLACK),
            marker=dict(line=dict(color="white", width=2)),
            hovertemplate="%{label}<br>%{value} hours<extra></extra>",
        )
        style_fig(fig_tree, height=450, axes=False)
        st.plotly_chart(fig_tree, use_container_width=True)

    # ---- Heatmap ----
    st.divider()
    st.subheader("Price Intensity Heatmap (average €/MWh)")
    day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    heat = fa.pivot_table(index="Hour", columns="Day", values="price_eur_mwh", aggfunc="mean")
    heat = heat.reindex(columns=[d for d in day_order if d in heat.columns])
    fig_heat = px.imshow(
        heat, text_auto=".0f", color_continuous_scale="RdYlGn_r", aspect="auto",
        labels=dict(x="Weekday", y="Hour of day", color="€/MWh"),
    )
    fig_heat.update_yaxes(dtick=1)
    style_fig(fig_heat, x_title="Weekday", y_title="Hour of day", height=720)
    fig_heat.update_layout(coloraxis_colorbar=dict(
        title=dict(text="€/MWh", font=dict(color=BLACK)),
        tickfont=dict(color=BLACK), outlinecolor=BLACK, outlinewidth=1,
    ))
    st.plotly_chart(fig_heat, use_container_width=True)


try:
    main()
except Exception as e:
    st.error(f"Dashboard Error: {e}")