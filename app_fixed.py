
import io
import math
import pandas as pd
import numpy as np
import streamlit as st
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

# ---------------------------
# UI CONFIG
# ---------------------------
st.set_page_config(page_title="Solar Average Performance Analyzer", layout="wide")
st.title("🔆 Solar Average Performance Analyzer")
st.caption("Upload your solar dataset + sunrise/sunset file, set parameters, and generate the 5 summary graphs & report.")

# ---------------------------
# HELPERS
# ---------------------------
CANON = {
    "date": "Date",
    "time (utc)": "Time (UTC)",
    "air temperature": "Air Temperature",
    "panel irradiance": "Panel Irradiance",
    "control meter (kw total)": "Control Meter (kW Total)",
    "kw total": "Control Meter (kW Total)",
    "energy gen (kwh)": "Energy GEN (kWh)",
    "energy gen kwh": "Energy GEN (kWh)",
    "date time (utc)": "Date Time (UTC)",
    "datetime (utc)": "Date Time (UTC)",
    "timestamp (utc)": "Date Time (UTC)",
}

NUMERIC_ORDER = [
    "Air Temperature",
    "Panel Irradiance",
    "Control Meter (kW Total)",
    "Energy GEN (kWh)",
    "Actual Generation (kWh)",
    "Expected Generation (kWh)",
    "Performance Ratio",
    "Contractual Performance Ratio",
]

def normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    rename = {}
    for c in df.columns:
        key = str(c).strip().lower()
        if key in CANON:
            rename[c] = CANON[key]
    out = df.rename(columns=rename)
    return out

def ensure_required_columns(df: pd.DataFrame) -> pd.DataFrame:
    if "Date Time (UTC)" in df.columns and (("Date" not in df.columns) or ("Time (UTC)" not in df.columns)):
        dt = pd.to_datetime(df["Date Time (UTC)"], errors="coerce", utc=True)
        if dt.dt.tz is None:
            dt = dt.dt.tz_localize("UTC")
        df["Date"] = dt.dt.tz_convert("UTC").dt.tz_localize(None).dt.date
        df["Time (UTC)"] = dt.dt.tz_convert("UTC").dt.tz_localize(None).dt.time
    return df

def parse_date_time(df: pd.DataFrame) -> pd.DataFrame:
    if "Date" not in df.columns or "Time (UTC)" not in df.columns:
        raise ValueError("Data must contain 'Date' and 'Time (UTC)' (or 'Date Time (UTC)') column.")
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.date
    df["Time (UTC)"] = pd.to_datetime(df["Time (UTC)"].astype(str), errors="coerce").dt.time
    df = df.dropna(subset=["Date", "Time (UTC)"])
    df = df.sort_values(["Date", "Time (UTC)"]).reset_index(drop=True)
    df["timestamp_utc"] = pd.to_datetime(df["Date"].astype(str) + " " + df["Time (UTC)"].astype(str), errors="coerce")
    return df

def detect_interval_hours(ts: pd.Series) -> float:
    diffs = ts.diff().dropna().dt.total_seconds() / 3600.0
    if diffs.empty:
        return 0.08333333
    med = diffs.median()
    if pd.isna(med) or med <= 0:
        return 0.08333333
    return float(med)

def per_day_3x_iqr_clean(df: pd.DataFrame, numeric_cols: list, date_col: str = "Date") -> pd.DataFrame:
    out = df.copy()
    for col in numeric_cols:
        if col not in out.columns:
            continue
        grp = out.groupby(date_col)[col]
        q1 = grp.transform(lambda s: s.quantile(0.25))
        q3 = grp.transform(lambda s: s.quantile(0.75))
        iqr = q3 - q1
        low = q1 - 3.0 * iqr
        high = q3 + 3.0 * iqr
        mask = (out[col] < low) | (out[col] > high)
        out.loc[mask, col] = np.nan
    return out

def build_line(fig_df: pd.DataFrame, xcol: str, ycols: list, title: str, ylabel: str):
    import plotly.express as px
    if len(ycols) == 1:
        fig = px.line(fig_df, x=xcol, y=ycols[0], title=title)
    else:
        m = fig_df.melt(id_vars=xcol, value_vars=ycols, var_name="Series", value_name="Value")
        fig = px.line(m, x=xcol, y="Value", color="Series", title=title)
    fig.update_layout(xaxis_title=xcol, yaxis_title=ylabel)
    return fig

def build_bar(cat_df: pd.DataFrame, cat_col: str, val_col: str, title: str):
    import plotly.express as px
    cat_df = cat_df.sort_values(val_col, ascending=False)
    fig = px.bar(cat_df, x=cat_col, y=val_col, title=title)
    fig.update_layout(xaxis_title=cat_col, yaxis_title=val_col)
    return fig

def to_excel_download(processed: pd.DataFrame, averages: pd.DataFrame, excl_counts: pd.DataFrame) -> bytes:
    with pd.ExcelWriter(io.BytesIO(), engine="xlsxwriter") as writer:
        processed.to_excel(writer, index=False, sheet_name="Data (Processed)")
        averages.to_excel(writer, index=False, sheet_name="Averages by Time")
        excl_counts.to_excel(writer, index=False, sheet_name="Exclusion Reasons")
        writer.book.formats[0].set_text_wrap()
        return writer.book.filename.getvalue()

# ---------------------------
# SIDEBAR PARAMETERS
# ---------------------------
with st.sidebar:
    st.header("⚙️ Parameters")
    capacity_kw = st.number_input("Site Capacity (kW)", value=1500.4, min_value=0.0, step=0.1)
    min_irr = st.number_input("CPR: Min Irradiance (W/m²)", value=100.0, min_value=0.0, step=10.0)
    max_ctrl = st.number_input("CPR: Max Control Meter (kW)", value=665.0, min_value=0.0, step=5.0)
    min_pr = st.number_input("CPR: Min PR", value=0.60, min_value=0.0, max_value=5.0, step=0.05)
    apply_iqr = st.checkbox("Apply per-day 3×IQR outlier cleaning (per column)", value=True)

# ---------------------------
# FILE INPUTS
# ---------------------------
col1, col2 = st.columns(2)
with col1:
    file_main = st.file_uploader("📄 Upload Solar Data (Excel/CSV)", type=["xlsx", "xls", "csv"])
with col2:
    file_sun = st.file_uploader("🌅 Upload Sunrise/Sunset (Excel/CSV)", type=["xlsx", "xls", "csv"])

st.write("")
run = st.button("🚀 Run Analysis")

if run:
    try:
        if file_main is None or file_sun is None:
            st.error("Please upload both the Solar Data file and the Sunrise/Sunset file.")
            st.stop()

        # ---- Load main data ----
        if file_main.name.lower().endswith(".csv"):
            df0 = pd.read_csv(file_main)
        else:
            df0 = normalize_headers(pd.read_excel(file_main, sheet_name=0))
        df0 = ensure_required_columns(df0)
        df0 = parse_date_time(df0)
        for c in df0.columns:
            if c not in ["Date","Time (UTC)","timestamp_utc","Date Time (UTC)"]:
                df0[c] = pd.to_numeric(df0[c], errors="coerce")

        interval_hours = detect_interval_hours(df0["timestamp_utc"])

        # ---- Load sunrise/sunset ----
        if file_sun.name.lower().endswith(".csv"):
            sun_raw = pd.read_csv(file_sun)
        else:
            sun_raw = pd.read_excel(file_sun)
        sun_cols = {c.strip().lower(): c for c in sun_raw.columns}
        if "date" not in sun_cols:
            st.error("Sunrise/Sunset file must include a 'Date' column.")
            st.stop()
        sun_raw["Date"] = pd.to_datetime(sun_raw[sun_cols["date"]], errors="coerce").dt.date
        sunrise_col = next((c for c in sun_raw.columns if "sunrise" in c.lower()), None)
        sunset_col  = next((c for c in sun_raw.columns if "sunset" in c.lower()), None)
        if sunrise_col is None or sunset_col is None:
            st.error("Sunrise/Sunset file must have 'Sunrise' and 'Sunset' columns (BST).")
            st.stop()
        sun_raw["Sunrise (BST)"] = pd.to_datetime(sun_raw[sunrise_col].astype(str), errors="coerce").dt.time
        sun_raw["Sunset (BST)"]  = pd.to_datetime(sun_raw[sunset_col].astype(str), errors="coerce").dt.time
        sun = sun_raw[["Date","Sunrise (BST)","Sunset (BST)"]]
        sun_map = sun.set_index("Date")[["Sunrise (BST)","Sunset (BST)"]]

        # ---- BST time & daylight ----
        df = df0.copy()
        df["time_bst"] = df.apply(lambda r: datetime.combine(r["Date"], r["Time (UTC)"]).replace(tzinfo=ZoneInfo("UTC")).astimezone(ZoneInfo("Europe/London")).time(), axis=1)
        df["Sunrise (BST)"] = df["Date"].map(sun_map["Sunrise (BST)"])
        df["Sunset (BST)"]  = df["Date"].map(sun_map["Sunset (BST)"])
        df["is_daylight"] = df.apply(lambda r: r["Sunrise (BST)"] <= r["time_bst"] <= r["Sunset (BST)"] if pd.notna(r["Sunrise (BST)"]) and pd.notna(r["Sunset (BST)"]) else False, axis=1)

        # ---- Daylight corrections ----
        if "Panel Irradiance" in df.columns:
            df.loc[~df["is_daylight"], "Panel Irradiance"] = 0.0
            df.loc[df["Panel Irradiance"] < 0, "Panel Irradiance"] = 0.0

        # ---- Flatten nighttime energy ----
        if "Energy GEN (kWh)" in df.columns:
            e = df["Energy GEN (kWh)"].astype(float).values
            for i in range(1, len(df)):
                if (not df.loc[i, "is_daylight"]) and (e[i] > e[i-1]):
                    e[i] = e[i-1]
            for i in range(len(e)):
                if i > 0 and (not np.isfinite(e[i]) or e[i] <= 0):
                    e[i] = e[i-1]
                elif i == 0 and (not np.isfinite(e[i]) or e[i] < 0):
                    e[i] = 0.0
            df["Energy GEN (kWh)"] = e

        # ---- Control meter ----
        pair_hours = df["timestamp_utc"].diff().dt.total_seconds().fillna(interval_hours*3600)/3600.0
        delta_e = np.insert(np.diff(df["Energy GEN (kWh)"].values.astype(float)), 0, 0.0)
        ctrl = delta_e / pair_hours.values
        ctrl[(~np.isfinite(ctrl)) | (ctrl < 0)] = 0.0
        df["Control Meter (kW Total)"] = ctrl

        # ---- Generation ----
        delta_e[delta_e < 0] = 0.0
        df["Actual Generation (kWh)"] = delta_e
        irr = df["Panel Irradiance"].fillna(0.0).clip(lower=0.0)
        df["Expected Generation (kWh)"] = capacity_kw * (irr/1000.0) * pair_hours.values
        mask_exp = df["Expected Generation (kWh)"] > 0
        df["Performance Ratio"] = np.where(mask_exp, df["Actual Generation (kWh)"]/df["Expected Generation (kWh)"], np.nan)

        # ---- CPR ----
        cpr_mask = (df["Panel Irradiance"] >= min_irr) & (df["Control Meter (kW Total)"] <= max_ctrl) & (df["Performance Ratio"] >= min_pr) & (df["Expected Generation (kWh)"] > 0)
        df["Contractual Performance Ratio"] = np.where(cpr_mask, df["Performance Ratio"], np.nan)

        # ---- Exclusion Reason ----
        reasons = []
        for _, row in df.iterrows():
            r = []
            if not (row["Expected Generation (kWh)"] > 0):
                r.append("Expected Generation ≤ 0 or missing data")
            if row["Panel Irradiance"] < min_irr:
                r.append(f"Irradiance below {min_irr}")
            if row["Control Meter (kW Total)"] > max_ctrl:
                r.append(f"Control Meter above {max_ctrl}")
            if row["Expected Generation (kWh)"] > 0 and row["Performance Ratio"] < min_pr:
                r.append(f"Performance Ratio below {min_pr}")
            reasons.append("; ".join(r))
        df["Exclusion Reason"] = reasons

        # ---- Outlier cleaning ----
        df_clean = df.copy()
        if apply_iqr:
            df_clean = per_day_3x_iqr_clean(df_clean, [c for c in NUMERIC_ORDER if c in df_clean.columns])

        # ---- Averages ----
        avg = df_clean.assign(**{"Time (UTC)": df_clean["Time (UTC)"].astype(str)}).groupby("Time (UTC)")[["Panel Irradiance","Control Meter (kW Total)","Actual Generation (kWh)","Expected Generation (kWh)","Performance Ratio","Contractual Performance Ratio"]].mean().reset_index()

        # ---- Exclusion counts ----
        excl_counts = df_clean["Exclusion Reason"].replace("", "No Exclusion").value_counts().reset_index()
        excl_counts.columns = ["Exclusion Reason", "Count"]

        # ---- Graphs ----
        st.subheader("📈 Average Graphs")
        c1, c2 = st.columns(2)
        with c1:
            st.plotly_chart(build_line(avg, "Time (UTC)", ["Panel Irradiance"], "Average Irradiance", "W/m²"), use_container_width=True)
        with c2:
            st.plotly_chart(build_line(avg, "Time (UTC)", ["Control Meter (kW Total)"], "Average Control Meter", "kW"), use_container_width=True)
        st.plotly_chart(build_line(avg, "Time (UTC)", ["Actual Generation (kWh)","Expected Generation (kWh)"], "Average Actual vs Expected Generation", "kWh"), use_container_width=True)
        st.plotly_chart(build_line(avg, "Time (UTC)", ["Performance Ratio","Contractual Performance Ratio"], "Average PR & CPR", "Ratio"), use_container_width=True)
        st.subheader("📊 Exclusion Reasons")
        st.plotly_chart(build_bar(excl_counts, "Exclusion Reason", "Count", "Exclusion Reasons"), use_container_width=True)

        # ---- Download ----
        excel_bytes = to_excel_download(df_clean, avg, excl_counts)
        st.download_button("⬇️ Download Excel Report", data=excel_bytes, file_name=f"Average_Performance_Report_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    except Exception as e:
        st.error(f"Processing failed: {e}")
        st.stop()
