import os
from datetime import date, datetime, time

import pandas as pd
import streamlit as st
import altair as alt
from concurrent.futures import ThreadPoolExecutor
from nsepython import nse_quote_ltp

# ---------------------------------
# ✅ Streamlit page config
# ---------------------------------
st.set_page_config(page_title="NSE Live Turnover Tracker", layout="wide")
st.title("📈 NSE Live Turnover Tracker (With 20‑Day History)")

# ---------------------------------
# ✅ Your stock list
# ---------------------------------
stocks = [
    "RELIANCE", "TCS", "HDFCBANK", "INFY", "ICICIBANK",
    "SBIN", "BHARTIARTL", "KOTAKBANK", "LT", "ITC",
]

# ---------------------------------
# ✅ History file config
# ---------------------------------
HISTORY_FILE = "data/history.csv"
FLAG_FILE = "data/last_store.txt"


# ---------------------------------
# ✅ Helper: Check if today's snapshot is saved
# ---------------------------------
def is_today_saved():
    today_str = date.today().strftime("%Y-%m-%d")
    if os.path.exists(FLAG_FILE):
        with open(FLAG_FILE, "r") as f:
            return f.read().strip() == today_str
    return False


# ---------------------------------
# ✅ Helper: Should store missed 3:30 PM snapshot?
# ---------------------------------
def should_store_if_missed():
    now = datetime.now()
    today_str = now.strftime("%Y-%m-%d")

    # Already saved today?
    if is_today_saved():
        return False

    # If time is AFTER 3:30 PM → store once
    if now.time() >= time(15, 30):
        os.makedirs("data", exist_ok=True)
        with open(FLAG_FILE, "w") as f:
            f.write(today_str)
        return True

    return False


# ---------------------------------
# ✅ Single symbol fetch
# ---------------------------------
def fetch_single(symbol: str) -> dict:
    try:
        data = nse_quote_ltp(symbol)
        price_info = data.get("priceInfo", {})

        current_price = price_info.get("lastPrice", None)
        previous_close = price_info.get("previousClose", None)
        volume = price_info.get("totalTradedVolume", None)

        percent_change = None
        if current_price is not None and previous_close not in (None, 0):
            percent_change = ((current_price - previous_close) / previous_close) * 100

        value_cr = None
        if current_price is not None and volume is not None:
            value_cr = (current_price * volume) / 10000000  # 1 Cr = 10,000,000

        return {
            "Symbol": symbol,
            "Current Price": current_price,
            "Previous Close": previous_close,
            "Percent Change (%)": round(percent_change, 2) if percent_change else None,
            "Volume": volume,
            "Value (Cr)": round(value_cr, 2) if value_cr else None,
        }

    except Exception as e:
        return {
            "Symbol": symbol,
            "Current Price": None,
            "Previous Close": None,
            "Percent Change (%)": None,
            "Volume": None,
            "Value (Cr)": None,
            "Error": str(e),
        }


# ---------------------------------
# ✅ Fetch all data (parallel + cache)
# ---------------------------------
@st.cache_data(show_spinner=True, ttl=10)
def fetch_data(symbols):
    with ThreadPoolExecutor(max_workers=20) as executor:
        results = list(executor.map(fetch_single, symbols))
    return pd.DataFrame(results)


# ---------------------------------
# ✅ History helpers
# ---------------------------------
def save_daily_history(df: pd.DataFrame):
    df_to_save = df.copy()
    df_to_save["Date"] = date.today()

    os.makedirs("data", exist_ok=True)

    if os.path.exists(HISTORY_FILE):
        df_to_save.to_csv(HISTORY_FILE, mode="a", header=False, index=False)
    else:
        df_to_save.to_csv(HISTORY_FILE, index=False)


def load_history() -> pd.DataFrame:
    if not os.path.exists(HISTORY_FILE):
        return pd.DataFrame()

    df_hist = pd.read_csv(HISTORY_FILE)

    if "Date" not in df_hist.columns:
        return pd.DataFrame()

    df_hist["Date"] = pd.to_datetime(df_hist["Date"])
    if df_hist["Date"].empty:
        return pd.DataFrame()

    cutoff = df_hist["Date"].max() - pd.Timedelta(days=20)
    df_hist = df_hist[df_hist["Date"] >= cutoff]

    return df_hist


# ---------------------------------
# ✅ Top control bar
# ---------------------------------
col1, col2, col3 = st.columns([2, 1, 1])

with col1:
    st.caption("Live NSE turnover with 20‑day average comparison")

with col2:
    refresh_clicked = st.button("🔄 Refresh Now")

with col3:
    view_mode = st.selectbox(
        "View mode",
        ["All", "Top Turnover vs Avg (High)", "Top Turnover vs Avg (Low)"],
        index=0,
    )

if refresh_clicked:
    fetch_data.clear()


# ✅ (Delete history manual block removed here)


# ---------------------------------
# ✅ Fetch live data
# ---------------------------------
df = fetch_data(stocks)

# ✅ Auto-store missed 3:30 PM snapshot
if should_store_if_missed():
    save_daily_history(df)
    st.success("✅ Stored missed 3:30 PM snapshot (system was offline earlier)")


# ✅ UI indicator for today's snapshot
if is_today_saved():
    st.success("✅ Today's 3:30 PM snapshot is saved")
else:
    st.warning("⚠️ Today's 3:30 PM snapshot is NOT saved yet")


st.caption(f"Last updated: {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}")


# ---------------------------------
# ✅ Load history and compute 20‑day turnover average
# ---------------------------------
history_df = load_history()

if history_df.empty:
    st.warning("Not enough history yet. Data will build over the next 20 days.")
    st.dataframe(df, use_container_width=True)

else:
    avg_turnover = (
        history_df.groupby("Symbol")["Value (Cr)"]
        .mean()
        .reset_index()
        .rename(columns={"Value (Cr)": "Avg 20D Turnover (Cr)"})
    )

    comparison_df = df.merge(avg_turnover, on="Symbol", how="left")

    comparison_df["Turnover vs Avg (%)"] = (
        (comparison_df["Value (Cr)"] - comparison_df["Avg 20D Turnover (Cr)"])
        / comparison_df["Avg 20D Turnover (Cr)"]
    ) * 100

    comparison_df["TradingView"] = comparison_df["Symbol"].apply(
        lambda s: f'<a href="https://www.tradingview.com/chart/?symbol=NSE:{s}" target="_blank">📈 Chart</a>'
    )

    df_view = comparison_df.copy()

    if view_mode == "Top Turnover vs Avg (High)":
        df_view = df_view.sort_values("Turnover vs Avg (%)", ascending=False)
    elif view_mode == "Top Turnover vs Avg (Low)":
        df_view = df_view.sort_values("Turnover vs Avg (%)", ascending=True)
    else:
        df_view = df_view.sort_values("Symbol")

    st.subheader("📈 Today vs 20‑Day Average Turnover (Vertical Layout)")

    chart_df = df_view[["Symbol", "Value (Cr)", "Avg 20D Turnover (Cr)"]].dropna()

    if not chart_df.empty:
        chart_long = chart_df.melt("Symbol", var_name="Type", value_name="Turnover")

        chart = (
            alt.Chart(chart_long)
            .mark_bar()
            .encode(
                y=alt.Y("Symbol:N", sort="-x", title="Stock Symbol"),
                x=alt.X("Turnover:Q", title="Turnover (Cr)"),
                color=alt.Color(
                    "Type:N",
                    scale=alt.Scale(range=["#4CAF50", "#2196F3"]),
                    title=""
                ),
                tooltip=["Symbol", "Type", "Turnover"],
            )
            .properties(
                width="container",
                height=30 * len(chart_df) if len(chart_df) > 0 else 400,
            )
        )

        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("No valid turnover data available to plot.")

    st.subheader("📊 Turnover vs 20‑Day Average + TradingView")

    table_cols = [
        "Symbol",
        "Current Price",
        "Percent Change (%)",
        "Volume",
        "Value (Cr)",
        "Avg 20D Turnover (Cr)",
        "Turnover vs Avg (%)",
        "TradingView",
    ]

    st.write(
        df_view[table_cols].to_html(escape=False, index=False),
        unsafe_allow_html=True,
    )

    with st.expander("📅 Raw 20‑Day History (debug/analysis)"):
        st.dataframe(history_df, use_container_width=True)


# ---------------------------------
# ✅ View older history (beyond 20 days)
# ---------------------------------
with st.expander("📜 View Older History (Beyond 20 Days)"):
    if os.path.exists(HISTORY_FILE):
        full_hist = pd.read_csv(HISTORY_FILE)
        st.dataframe(full_hist, use_container_width=True)

        csv_data = full_hist.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="⬇️ Download Full History CSV",
            data=csv_data,
            file_name="full_history.csv",
            mime="text/csv"
        )
    else:
        st.info("No history file found.")