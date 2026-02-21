"""
Chargeback Report Dashboard Generator
======================================
Queries fraud.fought_cbs_followup WHERE rechargeApi = 9 and generates
a self-contained HTML dashboard inspired by the Kloutit design.

Usage:
    python generate_dashboard.py
"""

import json
import os
import subprocess
from datetime import datetime
from decimal import Decimal
from collections import defaultdict

import pandas as pd
from db_connection import get_db_connection


# ---------------------------------------------------------------------------
# Status mapping  (DB status  -->  display label & colour)
# ---------------------------------------------------------------------------
STATUS_CONFIG = {
    "Won":                  {"label": "Ganado",          "color": "#4CAF50", "bg": "#E8F5E9"},
    "Accepted":             {"label": "Perdido",        "color": "#FF9800", "bg": "#FFF3E0"},
    "Documents submitted":  {"label": "Docs Enviados",   "color": "#2196F3", "bg": "#E3F2FD"},
    "Active":               {"label": "Activo",          "color": "#9C27B0", "bg": "#F3E5F5"},
    "N/A (fought)":         {"label": "N/A (Peleado)",   "color": "#FFC107", "bg": "#FFF8E1"},
    "N/A (non-fought)":     {"label": "N/A (No Peleado)", "color": "#9E9E9E", "bg": "#F5F5F5"},
    None:                   {"label": "N/A",             "color": "#607D8B", "bg": "#ECEFF1"},
}

STATUS_ORDER = ["Won", "Accepted", "Documents submitted", "Active", "N/A (fought)", "N/A (non-fought)", None]


def fetch_data():
    """Fetch all chargeback records using a single unified query."""
    conn = get_db_connection()
    if conn is None:
        raise ConnectionError("Could not connect to the database.")
    try:
        query = """
        SELECT
        cbs.user_id, cbs.amount, cbs.operator, cbs.credit_card, bl.type, bl.standard_bank_name AS bank, bl.country, cbs.payment_date, cbs.chargeback_received_date, IF(fcbs.sift_id IS NOT NULL, 1, 0) AS is_fought, fcbs.status, fcbs.created_at AS submission_date, fcbs.result_date
        FROM fraud.cb_payments AS cbs
        LEFT JOIN saldogra_gamma.binlist AS bl
        ON LEFT(cbs.credit_card,6) = bl.card_first_6
        LEFT JOIN fraud.fought_cbs_followup AS fcbs
        ON cbs.id = fcbs.payment_id
        WHERE cbs.created_at > CURDATE() - INTERVAL 6 MONTH
        AND cbs.rechargeApi = 9
        """
        df = pd.read_sql(query, conn)
        return df
    finally:
        conn.close()


def process_data(df: pd.DataFrame) -> dict:
    """
    Aggregate the raw dataframe into summary structures
    consumed by the HTML template.
    """
    # Normalise
    df["amount"] = df["amount"].astype(float)
    df["status"] = df["status"].where(df["status"].notna(), None)
    
    # Dashboard-specific status split
    df["dashboard_status"] = df["status"]
    df.loc[df["status"].isna() & (df["is_fought"] == 1), "dashboard_status"] = "N/A (fought)"
    df.loc[df["status"].isna() & (df["is_fought"] == 0), "dashboard_status"] = "N/A (non-fought)"
    
    # We use submission_date for the month aggregations. 
    # Use payment_date as fallback if there is no submission_date (unfought cb)
    df["dashboard_date"] = pd.to_datetime(df["submission_date"])
    df["dashboard_date"] = df["dashboard_date"].fillna(pd.to_datetime(df["payment_date"]))
    df["month"] = df["dashboard_date"].dt.to_period("M")

def _aggregate_dashboard_metrics(df: pd.DataFrame) -> dict:
    """Helper to aggregate all dashboard charts for a given dataframe slice."""
    total_records = len(df)
    total_amount = df["amount"].sum()

    won_mask = df["status"] == "Won"
    lost_mask = df["status"] == "Accepted"
    fought_mask = df["is_fought"] == 1
    
    recovered_amount = df.loc[won_mask, "amount"].sum()
    lost_amount = df.loc[lost_mask, "amount"].sum()
    fought_amount = df.loc[fought_mask, "amount"].sum()
    
    won_count = won_mask.sum()
    lost_count = lost_mask.sum()
    fought_count = fought_mask.sum()
    success_rate = (won_count / (won_count + lost_count) * 100) if (won_count + lost_count) else 0

    status_summary = {}
    for s in STATUS_ORDER[:-1]: # exclude raw None
        mask = df["dashboard_status"] == s
        status_summary[s] = {
            "count": int(mask.sum()),
            "amount": float(df.loc[mask, "amount"].sum()),
        }

    def _get_time_series(date_col):
        df_temp = df.copy()
        df_temp["temp_month"] = pd.to_datetime(df_temp[date_col]).dt.to_period("M")
        df_temp = df_temp.dropna(subset=["temp_month"])
        months_sorted = sorted(df_temp["temp_month"].unique())
        month_labels = [str(m) for m in months_sorted]
        
        by_status = {}
        count_by_status = {}
        for s in STATUS_ORDER[:-1]:
            by_status[s] = []
            count_by_status[s] = []
            for m in months_sorted:
                mask = (df_temp["temp_month"] == m) & (df_temp["dashboard_status"] == s)
                by_status[s].append(float(df_temp.loc[mask, "amount"].sum()))
                count_by_status[s].append(int(mask.sum()))

        success = []
        for m in months_sorted:
            mask_month = df_temp["temp_month"] == m
            lost_m = (mask_month & (df_temp["status"] == "Accepted")).sum()
            won_m = (mask_month & (df_temp["status"] == "Won")).sum()
            total_m = lost_m + won_m
            success.append(round(won_m / total_m * 100, 1) if total_m else 0)
            
        return month_labels, by_status, count_by_status, success

    month_labels_payment, by_status_payment, count_by_status_payment, success_payment = _get_time_series("payment_date")
    month_labels_cb, by_status_cb, count_by_status_cb, success_cb = _get_time_series("chargeback_received_date")

    # --- Top 10 Operators ---
    top_operators = (
        df.groupby("operator")
        .agg(count=("operator", "size"), total=("amount", "sum"))
        .sort_values("count", ascending=False)
        .head(10)
        .reset_index()
    )
    
    # --- Top 10 Operators (Won) ---
    top_operators_won = (
        df[df["status"] == "Won"].groupby("operator")
        .agg(count=("operator", "size"), total=("amount", "sum"))
        .sort_values("count", ascending=False)
        .head(10)
        .reset_index()
    )

    # --- Top 10 Banks (Total CBs) ---
    top_banks_total = (
        df.groupby("bank")
        .agg(count=("bank", "size"), total=("amount", "sum"))
        .sort_values("count", ascending=False)
        .head(10)
        .reset_index()
    )
    
    # --- Top 10 Banks (Won CBs) ---
    top_banks_won = (
        df[df["status"] == "Won"].groupby("bank")
        .agg(count=("bank", "size"), total=("amount", "sum"))
        .sort_values("count", ascending=False)
        .head(10)
        .reset_index()
    )

    # --- Types Donut Data ---
    # e.g 'visa', 'mastercard', 'amex'
    types_raw = df["type"].fillna("desconocido").str.lower()
    
    # We want to map standard names simply
    def map_cc_type(t):
        if 'visa' in t: return 'VISA'
        if 'mastercard' in t or 'master' in t: return 'MasterCard'
        if 'amex' in t or 'american' in t: return 'AMEX'
        return 'Otro'
        
    mapped_types = types_raw.apply(map_cc_type)
    type_counts = mapped_types.value_counts()
    
    type_donut_labels = list(type_counts.index)
    type_donut_values = [int(v) for v in type_counts.values]
    
    # Colors for the CC types
    cc_colors = {
        'VISA': '#1A1F71',
        'MasterCard': '#EB001B',
        'AMEX': '#002663',
        'Otro': '#9E9E9E'
    }
    type_donut_colors = [cc_colors.get(l, '#9E9E9E') for l in type_donut_labels]

    # --- Country Donut Data ---
    country_raw = df["country"].fillna("Desconocido").str.upper()
    country_counts = country_raw.value_counts()
    
    # Keep top 4 countries, group rest as 'OTRO'
    if len(country_counts) > 4:
        top_countries = country_counts.head(4)
        otro_count = country_counts.iloc[4:].sum()
        country_counts = top_countries
        country_counts["OTRO"] = otro_count
        
    country_donut_labels = list(country_counts.index)
    country_donut_values = [int(v) for v in country_counts.values]
    
    # Colors for the countries (dynamic mapping or pre-defined palette)
    palette = ['#009FDD', '#9C27B0', '#4CAF50', '#FF9800', '#607D8B', '#E57373']
    country_donut_colors = palette[:len(country_donut_labels)]

    # Status Donut Data
    status_donut_labels = []
    status_donut_values = []
    status_donut_colors = []
    for s in STATUS_ORDER[:-1]:
        cfg = STATUS_CONFIG[s]
        status_donut_labels.append(cfg["label"])
        status_donut_values.append(status_summary[s]["count"])
        status_donut_colors.append(cfg["color"])

    return {
        "total_records": int(total_records),
        "total_amount": float(total_amount),
        "fought_amount": float(fought_amount),
        "recovered_amount": float(recovered_amount),
        "lost_amount": float(lost_amount),
        "fought_count": int(fought_count),
        "lost_count": int(lost_count),
        "won_count": int(won_count),
        "success_rate": round(float(success_rate), 1),
        "status_summary": status_summary,
        "month_labels_payment": month_labels_payment,
        "monthly_by_status_payment": by_status_payment,
        "monthly_count_by_status_payment": count_by_status_payment,
        "monthly_success_payment": success_payment,
        "month_labels_cb": month_labels_cb,
        "monthly_by_status_cb": by_status_cb,
        "monthly_count_by_status_cb": count_by_status_cb,
        "monthly_success_cb": success_cb,
        "top_operators": top_operators.to_dict("records"),
        "top_operators_won": top_operators_won.to_dict("records"),
        "top_banks_total": top_banks_total.to_dict("records"),
        "top_banks_won": top_banks_won.to_dict("records"),
        "status_donut_labels": status_donut_labels,
        "status_donut_values": status_donut_values,
        "status_donut_colors": status_donut_colors,
        "type_donut_labels": type_donut_labels,
        "type_donut_values": type_donut_values,
        "type_donut_colors": type_donut_colors,
        "country_donut_labels": country_donut_labels,
        "country_donut_values": country_donut_values,
        "country_donut_colors": country_donut_colors,
    }

def process_data(df: pd.DataFrame) -> dict:
    """
    Aggregate the raw dataframe into summary structures
    consumed by the HTML template. Builds both all & fought datasets.
    """
    # Normalise
    df["amount"] = df["amount"].astype(float)
    df["status"] = df["status"].where(df["status"].notna(), None)
    
    # Dashboard-specific status split
    df["dashboard_status"] = df["status"]
    df.loc[df["status"].isna() & (df["is_fought"] == 1), "dashboard_status"] = "N/A (fought)"
    df.loc[df["status"].isna() & (df["is_fought"] == 0), "dashboard_status"] = "N/A (non-fought)"
    
    # We use submission_date for the month aggregations. 
    # Use payment_date as fallback if there is no submission_date (unfought cb)
    df["dashboard_date"] = pd.to_datetime(df["submission_date"])
    df["dashboard_date"] = df["dashboard_date"].fillna(pd.to_datetime(df["payment_date"]))
    df["month"] = df["dashboard_date"].dt.to_period("M")

    # Metrics for all chargebacks
    all_metrics = _aggregate_dashboard_metrics(df)
    
    # Metrics specifically for fought cb
    fought_df = df[df["is_fought"] == 1].copy()
    fought_metrics = _aggregate_dashboard_metrics(fought_df)


    # --- Format Casos data for table ---
    # Need to handle NaT and NaN appropriately
    casos_rows = []
    
    if not df.empty:
        # Fill missing values for cleaner display
        df_display = df.copy()
        df_display = df_display.fillna({
            'operator': '',
            'credit_card': '',
            'type': '',
            'bank': '',
            'country': '',
            'status': 'N/A'
        })
        
        for idx, row in df_display.iterrows():
            payment_date = row['payment_date'].strftime('%d/%m/%Y %H:%M') if pd.notnull(row['payment_date']) else '-'
            cb_date = row['chargeback_received_date'].strftime('%d/%m/%Y') if pd.notnull(row['chargeback_received_date']) else '-'
            sub_date = row['submission_date'].strftime('%d/%m/%Y') if pd.notnull(row['submission_date']) else '-'
            res_date = row['result_date'].strftime('%d/%m/%Y') if pd.notnull(row['result_date']) else '-'
            
            # Format status badge if it exists in config
            status_text = row['status']
            badge_html = status_text
            if status_text in STATUS_CONFIG:
                cfg = STATUS_CONFIG[status_text]
                badge_html = f'<span style="background:{cfg["bg"]}; color:{cfg["color"]}; padding:4px 8px; border-radius:4px; font-weight:600; font-size:0.8rem; border:1px solid {cfg["color"]}">{cfg["label"]}</span>'
            elif status_text != 'N/A' and status_text is not None:
                # Fallback for unmapped statuses
                badge_html = f'<span style="background:#ECEFF1; color:#607D8B; padding:4px 8px; border-radius:4px; font-weight:600; font-size:0.8rem; border:1px solid #607D8B">{status_text}</span>'
            
            is_fought_html = '<td><span style="color:#4CAF50;font-weight:bold">Sí</span></td>' if row['is_fought'] else '<td><span style="color:#F44336;font-weight:bold">No</span></td>'
            
            html_row = f"""
            <tr>
                <td>{row['user_id']}</td>
                <td style="font-weight:600">${float(row['amount']):,.2f}</td>
                <td>{row['operator']}</td>
                <td><span style="font-family:monospace; background:#f0eef5; padding:2px 4px; border-radius:4px">{row['credit_card'] if row['credit_card'] and len(row['credit_card']) >= 4 else ''}</span></td>
                <td>{str(row["type"]).capitalize() if row["type"] else "-"}</td>
                <td>{row['bank']}</td>
                <td data-sort="{row['payment_date'].timestamp() if pd.notnull(row['payment_date']) else 0}">{payment_date}</td>
                <td>{cb_date}</td>
                {is_fought_html}
                <td>{badge_html}</td>
                <td data-sort="{row['submission_date'].timestamp() if pd.notnull(row['submission_date']) else 0}">{sub_date}</td>
                <td data-sort="{row['result_date'].timestamp() if pd.notnull(row['result_date']) else 0}">{res_date}</td>
            </tr>
            """
            casos_rows.append(html_row)

    # -- Extract Unique Filter Values --
    casos_filters = {
        "operators": [],
        "types": [],
        "banks": [],
        "statuses": []
    }
    
    if not df.empty:
        # Extract sorted unique non-empty values
        ops = [str(x) for x in df_display['operator'].dropna().unique() if str(x).strip()]
        typs = [str(x).capitalize() for x in df_display['type'].dropna().unique() if str(x).strip()]
        bnks = [str(x) for x in df_display['bank'].dropna().unique() if str(x).strip()]
        
        # Determine actual rendered statuses
        stss = set()
        for idx, row in df_display.iterrows():
            stss.add(row['status'] if row['status'] in STATUS_CONFIG else (row['status'] if pd.notnull(row['status']) else 'N/A'))
            
        casos_filters = {
            "operators": sorted(ops),
            "types": sorted(list(set(typs))), # Set to remove duplicate caps
            "banks": sorted(bnks),
            "statuses": sorted(list(stss))
        }

    return {
        "all": all_metrics,
        "fought": fought_metrics,
        "casos_table_rows": "".join(casos_rows),
        "casos_filters": casos_filters
    }

def _json(obj):
    """Safely serialise for embedding in JS."""
    import math
    
    # helper to clean objects so JSON doesn't crash on NaNs
    def clean_obj(o):
        if isinstance(o, float):
            return None if math.isnan(o) or math.isinf(o) else o
        if isinstance(o, dict):
            return {k: clean_obj(v) for k, v in o.items()}
        if isinstance(o, list):
            return [clean_obj(v) for v in o]
        return o
        
    return json.dumps(clean_obj(obj), ensure_ascii=False)


def generate_html(data: dict) -> str:
    """Return the full HTML string for the dashboard."""

    now = datetime.now().strftime("%d/%m/%Y %H:%M")

    # Pass configuration dynamically to JS
    js_status_config = {s: {"label": STATUS_CONFIG[s]["label"], "color": STATUS_CONFIG[s]["color"]} for s in STATUS_ORDER[:-1]}

    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Chargeback Dashboard — Unlimit (rechargeApi 9)</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Figtree:wght@300;400;500;600;700&display=swap" rel="stylesheet">
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.7/dist/chart.umd.min.js"></script>
<style>
/* ── Reset & Base ──────────────────────────────────────────── */
*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
html{{font-size:14px}}
body{{
  font-family:'Figtree',system-ui,sans-serif;
  background:#F4F2F7;
  color:#1e1e2f;
  min-height:100vh;
}}

/* ── Sidebar ───────────────────────────────────────────────── */
.sidebar{{
  position:fixed;top:0;left:0;bottom:0;width:230px;
  background:#0B1E3E; /* UnDosTres Navy Blue */
  color:#fff;padding:28px 20px;z-index:100;
  display:flex;flex-direction:column;
}}
.sidebar .logo-container{{
  background:#fff;
  padding:12px 16px;
  border-radius:8px;
  margin-bottom:40px;
  display:inline-block;
}}
.sidebar .logo-img{{
  max-width:140px;
  display:block;
}}
.sidebar nav a{{
  display:flex;align-items:center;gap:10px;
  padding:11px 14px;border-radius:10px;
  color:#b3c4d6;text-decoration:none;font-weight:500;
  transition:all .2s;margin-bottom:4px;font-size:.95rem;
}}
.sidebar nav a:hover,.sidebar nav a.active{{
  background:rgba(255,255,255,.1);color:#fff;
}}
.sidebar nav a.active{{background:rgba(0,159,221,.25); color:#009FDD;}}
.sidebar nav a svg{{width:20px;height:20px;flex-shrink:0}}

/* ── Main Content ──────────────────────────────────────────── */
.main{{margin-left:230px;padding:28px 32px 40px}}

/* Header */
.header {{
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 30px;
}}
.header-actions {{
  display: flex;
  align-items: center;
  gap: 16px;
}}
/* Toggle Switch CSS */
.toggle-wrapper {{
  display: flex;
  align-items: center;
  background: white;
  padding: 8px 16px;
  border-radius: 8px;
  box-shadow: 0 2px 8px rgba(0,0,0,0.05);
  gap: 12px;
}}
.toggle-label {{
  font-weight: 500;
  color: #333;
  font-size: 0.9rem;
}}
.switch {{
  position: relative;
  display: inline-block;
  width: 44px;
  height: 24px;
}}
.switch input {{ 
  opacity: 0;
  width: 0;
  height: 0;
}}
.slider {{
  position: absolute;
  cursor: pointer;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  background-color: #ccc;
  transition: .4s;
  border-radius: 24px;
}}
.slider:before {{
  position: absolute;
  content: "";
  height: 18px;
  width: 18px;
  left: 3px;
  bottom: 3px;
  background-color: white;
  transition: .4s;
  border-radius: 50%;
}}
input:checked + .slider {{
  background-color: #009FDD; 
}}
input:checked + .slider:before {{
  transform: translateX(20px);
}}
.title-group h1 {{ font-size:1.8rem; font-weight:700; color:#1a0533; margin:0 }}
.title-group p {{ color:#757575; margin:4px 0 0 0 }}
.timestamp{{
  font-size:.85rem;color:#7a7a8c;
  background:#fff;padding:6px 14px;border-radius:8px;
  box-shadow:0 1px 3px rgba(0,0,0,.06);
}}

/* ── Global Stats Cards ───────────────────────────────────── */
.stats-row{{
  display:grid;
  gap:18px;margin-bottom:28px;
}}
.stats-row.top-row{{ grid-template-columns:repeat(2,1fr); }}
.stats-row.bottom-row{{ grid-template-columns:repeat(4,1fr); }}
.stat-card{{
  background:#fff;border-radius:14px;padding:22px 24px;
  box-shadow:0 2px 12px rgba(26,5,51,.06);
  position:relative;overflow:hidden;
  transition:transform .2s,box-shadow .2s;
}}
.stat-card:hover{{transform:translateY(-3px);box-shadow:0 6px 20px rgba(26,5,51,.1)}}
.stat-card .accent{{
  position:absolute;top:0;left:0;right:0;height:4px;
  border-radius:14px 14px 0 0;
}}
.stat-card .label{{
  font-size:.78rem;text-transform:uppercase;letter-spacing:.8px;
  color:#7a7a8c;font-weight:600;margin-bottom:6px;
}}
.stat-card .value{{
  font-size:1.75rem;font-weight:700;color:#1A0533;
}}
.stat-card .sub{{font-size:.82rem;color:#999;margin-top:4px}}

/* ── Chart Grid ───────────────────────────────────────────── */
.chart-grid {{
  display:grid;grid-template-columns:1fr;gap:24px;margin-bottom:28px;
}}
.chart-grid-3 {{
  display:grid;grid-template-columns:1fr 1fr 1fr;gap:24px;margin-bottom:28px;
}}
.chart-card{{
  background:#fff;border-radius:14px;padding:24px;
  box-shadow:0 2px 12px rgba(26,5,51,.06);
}}
.chart-card-header {{
  display:flex;justify-content:space-between;align-items:center;margin-bottom:16px;
}}
.chart-card-header h2 {{
  font-size:1.05rem;font-weight:700;margin:0;
  color:#1A0533;text-transform:uppercase;letter-spacing:.5px;
}}
.chart-metric-select {{
  padding: 6px 12px;
  border-radius: 6px;
  border: 1px solid #e8e6ed;
  font-family: 'Figtree', sans-serif;
  font-size: 0.9rem;
  color: #1e1e2f;
  background: #fcfcfd;
  outline: none;
}}
.chart-canvas-wrap{{position:relative;width:100%;}}
.chart-canvas-wrap.tall{{height:350px}}
.chart-canvas-wrap.med{{height:280px}}

.full-width{{grid-column:1/-1}}

/* ── Status Mini-Cards ────────────────────────────────────── */
.status-grid{{
  display:grid;grid-template-columns:1fr;gap:10px;
}}
.status-card{{
  padding:14px 16px;border-radius:10px;
  display:flex;align-items:center;gap:14px;
  transition:transform .15s;
}}
.status-card:hover{{transform:translateX(4px)}}
.status-card-label{{
  font-weight:600;font-size:.88rem;min-width:100px;
}}
.status-card-count{{
  font-weight:700;font-size:1.15rem;min-width:50px;text-align:right;
}}
.status-card-amount{{
  font-size:.85rem;color:#555;margin-left:auto;
}}

/* ── Operators Table ──────────────────────────────────────── */
.op-row{{
  display:flex;align-items:center;gap:12px;
  padding:9px 0;border-bottom:1px solid #f0eef5;
}}
.op-row:last-child{{border-bottom:none}}
.op-rank{{
  width:24px;height:24px;border-radius:50%;
  background:#E3F2FD;color:#0B1E3E;
  display:flex;align-items:center;justify-content:center;
  font-size:.75rem;font-weight:700;flex-shrink:0;
}}
.op-name{{font-weight:600;min-width:110px;font-size:.9rem}}
.op-bar-wrapper{{
  flex:1;height:8px;background:#f0eef5;border-radius:4px;overflow:hidden;
}}
.op-bar{{
  height:100%;border-radius:4px;
  background:#009FDD;
  transition:width .6s ease;
}}
.op-count{{font-weight:700;min-width:36px;text-align:right;font-size:.9rem}}
.op-amount{{color:#7a7a8c;min-width:90px;text-align:right;font-size:.85rem}}

/* ── Donut Legend ─────────────────────────────────────────── */
.donut-legend{{display:flex;flex-wrap:wrap;gap:10px;margin-top:12px;justify-content:center}}
.donut-legend span{{
  display:flex;align-items:center;gap:5px;font-size:.82rem;font-weight:500;
}}
.donut-legend .dot{{
  width:10px;height:10px;border-radius:50%;flex-shrink:0;
}}

/* ── Casos Filters ────────────────────────────────────────── */
.filters-bar{{
  background:#fff;border-radius:14px;padding:20px;
  box-shadow:0 2px 12px rgba(26,5,51,.06);
  margin-bottom:20px;
  display:flex;flex-wrap:wrap;gap:15px;align-items:end;
}}
.filter-group{{ display:flex;flex-direction:column;gap:6px;flex:1;min-width:180px; }}
.op-list {{ display:flex; flex-direction:column; gap:12px; max-height: 400px; overflow-y: auto;}}
.filter-group label{{ font-size:.8rem;color:#7a7a8c;font-weight:600;text-transform:uppercase; }}
.filter-input{{
  padding:8px 12px;border:1px solid #e8e6ed;border-radius:8px;
  font-family:'Figtree',sans-serif;font-size:.9rem;color:#1e1e2f;
  background:#fcfcfd;outline:none;transition:border-color .2s;
}}
.filter-input:focus{{border-color:#009FDD;}}
.filter-row{{display:flex;gap:8px;align-items:center;width:100%;}}
.filter-row span{{color:#7a7a8c;font-size:.85rem;}}

/* ── Casos Table ──────────────────────────────────────────── */
.table-container{{
  background:#fff;border-radius:14px;
  box-shadow:0 2px 12px rgba(26,5,51,.06);
  overflow-x:auto;
  position: relative;
}}
.casos-table{{
  width:100%;border-collapse:collapse;text-align:left;
  font-size:.9rem;
}}
.casos-table th{{
  background:#f4f2f7;color:#7a7a8c;font-weight:600;font-size:.8rem;
  text-transform:uppercase;letter-spacing:.5px;
  padding:16px;white-space:nowrap;
}}
.casos-table td{{
  padding:14px 16px;border-bottom:1px solid #f0eef5;
  white-space:nowrap;
}}
.casos-table tbody tr:hover{{background:#faf9fc}}

/* Sorting Icons */
.sortable{{ cursor: pointer; user-select: none; transition: background 0.2s; }}
.sortable:hover{{ background: #e8e6ed; color: #0B1E3E; }}
.sortable::after{{
  content: '↕'; margin-left: 6px; font-size: 0.9em; opacity: 0.4;
}}
.sortable.asc::after{{ content: '↑'; opacity: 1; color: #009FDD; }}
.sortable.desc::after{{ content: '↓'; opacity: 1; color: #009FDD; }}

/* Pagination */
.pagination-controls{{
  display: flex; align-items: center; justify-content: space-between;
  padding: 16px 24px; border-top: 1px solid #f0eef5;
  background: #fff; border-radius: 0 0 14px 14px;
}}
.page-info{{ font-size: 0.9rem; color: #7a7a8c; }}
.page-buttons{{ display: flex; gap: 8px; }}
.page-btn{{
  background: #f4f2f7; border: none; padding: 6px 14px;
  border-radius: 6px; font-weight: 600; color: #0B1E3E;
  cursor: pointer; transition: all 0.2s; font-family:'Figtree', sans-serif;
}}
.page-btn:hover:not(:disabled){{ background: #009FDD; color: #fff; }}
.page-btn.active{{ background: #0B1E3E; color: #fff; }}
.page-btn:disabled{{ opacity: 0.4; cursor: not-allowed; }}

/* ── Responsive ───────────────────────────────────────────── */
@media(max-width:992px){{
  .stats-row.bottom-row{{grid-template-columns:repeat(2,1fr)}}
  .chart-grid, .chart-grid-3 {{grid-template-columns:1fr}}
}}
@media(max-width:768px){{
  .sidebar{{display:none}}
  .main{{margin-left:0}}
  .header {{flex-direction: column; align-items: flex-start; gap: 16px;}}
  .stats-row.top-row, .stats-row.bottom-row{{grid-template-columns:1fr}}
}}
</style>
</head>
<body>

<!-- ── Sidebar ──────────────────────────────────────────────── -->
<aside class="sidebar">
  <div class="logo-container">
    <img src="logo.png" alt="UnDosTres" class="logo-img">
  </div>
  <nav>
    <a href="#" class="nav-link active" data-target="dashboard-view">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="3" width="7" height="7" rx="1"/><rect x="14" y="3" width="7" height="7" rx="1"/><rect x="3" y="14" width="7" height="7" rx="1"/><rect x="14" y="14" width="7" height="7" rx="1"/></svg>
      Dashboard
    </a>
    <a href="#" class="nav-link" data-target="casos-view">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z"/><path d="M14 2v6h6M16 13H8M16 17H8M10 9H8"/></svg>
      Casos
    </a>
  </nav>
</aside>

<!-- ── Main Content ────────────────────────────────────────── -->
<div class="main">

  <div class="header">
    <div class="title-group">
      <h1>Performance Contracargos Unlimit</h1>
      <p>Reporte actualizado al {now}</p>
    </div>
    
    <div class="header-actions">
      <div id="foughtToggleWrapper" class="toggle-wrapper">
        <span class="toggle-label">Solo casos peleados</span>
        <label class="switch">
          <input type="checkbox" id="foughtToggle">
          <span class="slider"></span>
        </label>
      </div>
    </div>
  </div>

  <!-- DASHBOARD VIEW -->
  <div id="dashboard-view" class="view-content active">

  <!-- Global Stats -->
  <div class="stats-row top-row">
    <div class="stat-card">
      <div class="accent" style="background:#0B1E3E"></div>
      <div class="label">Total Chargebacks</div>
      <div class="value" id="val_total_records">-</div>
      <div class="sub">Contracargos recibidos</div>
    </div>
    <div class="stat-card">
      <div class="accent" style="background:#009FDD"></div>
      <div class="label">Importe Total en CBs</div>
      <div class="value" id="val_total_amount">-</div>
      <div class="sub">MXN</div>
    </div>
  </div>
  
  <div class="stats-row bottom-row">
    <div class="stat-card">
      <div class="accent" style="background:linear-gradient(90deg,#9C27B0,#BA68C8)"></div>
      <div class="label">Importe Peleado</div>
      <div class="value" id="val_fought_amount">-</div>
      <div class="sub"><span id="val_fought_count">-</span> casos peleados</div>
    </div>
    <div class="stat-card">
      <div class="accent" style="background:linear-gradient(90deg,#4CAF50,#81C784)"></div>
      <div class="label">Importe Recuperado</div>
      <div class="value" id="val_recovered_amount">-</div>
      <div class="sub"><span id="val_won_count">-</span> casos ganados</div>
    </div>
    <div class="stat-card">
      <div class="accent" style="background:linear-gradient(90deg,#F44336,#E57373)"></div>
      <div class="label">Importe Perdido</div>
      <div class="value" id="val_lost_amount">-</div>
      <div class="sub"><span id="val_lost_count">-</span> casos perdidos</div>
    </div>
    <div class="stat-card">
      <div class="accent" style="background:linear-gradient(90deg,#00BCD4,#4DD0E1)"></div>
      <div class="label">Tasa de Éxito</div>
      <div class="value" id="val_success_rate">-</div>
      <div class="sub">Ganados / Ganados + Perdidos</div>
    </div>
  </div>

  <div class="chart-grid">
    <div class="chart-card">
      <div class="chart-card-header">
          <div style="display:flex; align-items:center; gap: 15px;">
              <h2 id="mainChartTitle" style="margin:0;">Importe por Mes (MXN)</h2>
          </div>
          <div style="display:flex;gap:10px;">
              <select id="dateChartToggle" class="chart-metric-select">
                  <option value="payment">Fecha de Pago</option>
                  <option value="cb">Fecha de Recepción CB</option>
              </select>
              <select id="mainChartToggle" class="chart-metric-select">
                  <option value="amount">Importe (MXN)</option>
                  <option value="count">Cantidad</option>
              </select>
          </div>
      </div>
      <div class="chart-canvas-wrap tall"><canvas id="chartMain"></canvas></div>
    </div>
  </div>

  <div class="chart-grid-3">
    <div class="chart-card" style="display:flex;flex-direction:column;align-items:center;">
      <h2 style="align-self:flex-start">Distribución del Estatus</h2>
      <div class="chart-canvas-wrap med"><canvas id="chartStatus"></canvas></div>
    </div>
    <div class="chart-card" style="display:flex;flex-direction:column;align-items:center;">
      <h2 style="align-self:flex-start">Tasa de Éxito Mensual (%)</h2>
      <div class="chart-canvas-wrap med" style="width:100%"><canvas id="chartSuccess"></canvas></div>
    </div>
    <div class="chart-card">
      <h2>Desglose de Estatus</h2>
      <div class="status-stack" id="status_cards_container" style="display:flex;flex-direction:column;gap:10px;"></div>
    </div>
  </div>

  <div class="chart-grid-3">
    <div class="chart-card">
      <h2>Top 10 Bancos (Volumen Total)</h2>
      <div class="op-list" id="list_banks_total"></div>
    </div>
    <div class="chart-card">
      <h2>Top 10 Bancos (Ganados)</h2>
      <div class="op-list" id="list_banks_won"></div>
    </div>
    <div class="chart-card" style="display:flex;flex-direction:column;align-items:center;">
      <h2 style="align-self:flex-start">Tipo de Tarjeta</h2>
      <div class="chart-canvas-wrap med"><canvas id="chartType"></canvas></div>
    </div>
  </div>

  <div class="chart-grid-3">
    <div class="chart-card">
      <h2>Top 10 Operadores</h2>
      <div class="op-list" id="list_operators"></div>
    </div>
    <div class="chart-card">
      <h2>Top 10 Operadores (Ganados)</h2>
      <div class="op-list" id="list_operators_won"></div>
    </div>
    <div class="chart-card" style="display:flex;flex-direction:column;align-items:center;">
      <h2 style="align-self:flex-start">Distribución por País</h2>
      <div class="chart-canvas-wrap med"><canvas id="chartCountry"></canvas></div>
    </div>
  </div>

  </div><!-- /dashboard-view -->

  <!-- CASOS VIEW -->
  <div id="casos-view" class="view-content" style="display: none;">
    <!-- Filters Bar -->
    <div class="filters-bar" id="casos-filters">
        
        <div class="filter-group">
            <label>Monto</label>
            <div class="filter-row">
                <input type="number" id="f-amount-min" class="filter-input" placeholder="Min $" style="width:50%">
                <span>-</span>
                <input type="number" id="f-amount-max" class="filter-input" placeholder="Max $" style="width:50%">
            </div>
        </div>

        <div class="filter-group">
            <label>Operador</label>
            <select id="f-operator" class="filter-input">
                <option value="">Todos</option>
                {''.join(f'<option value="{op}">{op}</option>' for op in data['casos_filters']['operators'])}
            </select>
        </div>

        <div class="filter-group">
            <label>Tipo Tarjeta</label>
            <select id="f-type" class="filter-input">
                <option value="">Todos</option>
                {''.join(f'<option value="{typ}">{typ}</option>' for typ in data['casos_filters']['types'])}
            </select>
        </div>

        <div class="filter-group">
            <label>Banco</label>
            <select id="f-bank" class="filter-input">
                <option value="">Todos</option>
                {''.join(f'<option value="{bnk}">{bnk}</option>' for bnk in data['casos_filters']['banks'])}
            </select>
        </div>

        <div class="filter-group">
            <label>Estado</label>
            <select id="f-status" class="filter-input">
                <option value="">Todos</option>
                {''.join(f'<option value="{sts}">{sts}</option>' for sts in data['casos_filters']['statuses'])}
            </select>
        </div>

        <div class="filter-group">
            <label>Peleado</label>
            <select id="f-fought" class="filter-input">
                <option value="">Todos</option>
                <option value="Sí">Sí</option>
                <option value="No">No</option>
            </select>
        </div>

        <!-- Date Filters -->
        <div class="filter-group" style="min-width:280px">
            <label>Fecha Pago</label>
            <div class="filter-row">
                <input type="date" id="f-date-pay-start" class="filter-input">
                <span> a </span>
                <input type="date" id="f-date-pay-end" class="filter-input">
            </div>
        </div>

        <div class="filter-group" style="min-width:280px">
            <label>Fecha Chargeback</label>
            <div class="filter-row">
                <input type="date" id="f-date-cb-start" class="filter-input">
                <span> a </span>
                <input type="date" id="f-date-cb-end" class="filter-input">
            </div>
        </div>

        <div class="filter-group" style="min-width:280px">
            <label>Fecha Envío Docs</label>
            <div class="filter-row">
                <input type="date" id="f-date-sub-start" class="filter-input">
                <span> a </span>
                <input type="date" id="f-date-sub-end" class="filter-input">
            </div>
        </div>

        <div class="filter-group" style="min-width:280px">
            <label>Fecha Resultado</label>
            <div class="filter-row">
                <input type="date" id="f-date-res-start" class="filter-input">
                <span> a </span>
                <input type="date" id="f-date-res-end" class="filter-input">
            </div>
        </div>

        <div class="filter-group" style="flex: 0 0 auto;">
            <button id="btn-reset-filters" class="page-btn" style="padding:10px 16px; margin-bottom: 2px;">Limpiar Filtros</button>
        </div>
    </div>

    <div class="table-container">
      <table class="casos-table">
        <thead>
          <tr>
            <th>User ID</th>
            <th class="sortable" data-type="amount" data-col="1">Monto</th>
            <th>Operador</th>
            <th>Tarjeta</th>
            <th>Tipo</th>
            <th>Banco</th>
            <th class="sortable" data-col="6">Fecha Pago</th>
            <th>Fecha CB</th>
            <th>Peleado</th>
            <th>Estado</th>
            <th class="sortable" data-col="10">Fecha Envío</th>
            <th class="sortable" data-col="11">Fecha Res.</th>
          </tr>
        </thead>
        <tbody id="casos-tbody">
          {data['casos_table_rows']}
        </tbody>
      </table>
      
      <!-- Pagination Controls -->
      <div class="pagination-controls">
        <div class="page-info" id="page-info">Mostrando 1-50 de 0 casos</div>
        <div class="page-buttons" id="page-buttons">
            <!-- Buttons generated by JS -->
        </div>
      </div>
      
    </div>
  </div><!-- /casos-view -->

</div><!-- /main -->

<!-- ── Scripts ─────────────────────────────────────────────── -->
<script>
/* ── Navigation Logic ───────────────────────────────────────── */
document.querySelectorAll('.nav-link').forEach(link => {{
  link.addEventListener('click', function(e) {{
    e.preventDefault();
    
    // Update active class on nav
    document.querySelectorAll('.nav-link').forEach(l => l.classList.remove('active'));
    this.classList.add('active');
    
    // Switch views
    const targetId = this.getAttribute('data-target');
    document.querySelectorAll('.view-content').forEach(v => v.style.display = 'none');
    document.getElementById(targetId).style.display = 'block';
    
    // Update title
    document.getElementById('page-title').innerText = targetId === 'dashboard-view' ? 'Chargeback Dashboard' : 'Casos Detallados';
  }});
}});

/* ── Casos Table Sorting, Pagination & Filtering ────────── */
const rowsPerPage = 50;
let currentPage = 1;
const tbody = document.getElementById('casos-tbody');
const allRows = Array.from(tbody.querySelectorAll('tr'));
let currentRows = [...allRows]; 

// Filter Elements
const fAmountMin = document.getElementById('f-amount-min');
const fAmountMax = document.getElementById('f-amount-max');
const fOperator = document.getElementById('f-operator');
const fType = document.getElementById('f-type');
const fBank = document.getElementById('f-bank');
const fStatus = document.getElementById('f-status');
const fFought = document.getElementById('f-fought');

const fDatePayStart = document.getElementById('f-date-pay-start');
const fDatePayEnd = document.getElementById('f-date-pay-end');
const fDateCbStart = document.getElementById('f-date-cb-start');
const fDateCbEnd = document.getElementById('f-date-cb-end');
const fDateSubStart = document.getElementById('f-date-sub-start');
const fDateSubEnd = document.getElementById('f-date-sub-end');
const fDateResStart = document.getElementById('f-date-res-start');
const fDateResEnd = document.getElementById('f-date-res-end');

// Helper to parse DD/MM/YYYY into JS Date
function parseDMY(dateStr) {{
    if (!dateStr || dateStr === '-') return null;
    const parts = dateStr.split(' ')[0].split('/'); // handles DD/MM/YYYY HH:MM and DD/MM/YYYY
    if (parts.length === 3) {{
        // Months are 0-indexed in JS
        return new Date(parts[2], parseInt(parts[1])-1, parts[0]);
    }}
    return null;
}}

// Helper to parse YYYY-MM-DD from input type="date"
function parseYMD(dateStr) {{
    if (!dateStr) return null;
    const parts = dateStr.split('-');
    if (parts.length === 3) {{
        return new Date(parts[0], parseInt(parts[1])-1, parts[2]);
    }}
    return null;
}}

function applyFilters() {{
  const minAmt = parseFloat(fAmountMin.value);
  const maxAmt = parseFloat(fAmountMax.value);
  const operatorVal = fOperator.value.toLowerCase();
  const typeVal = fType.value.toLowerCase();
  const bankVal = fBank.value.toLowerCase();
  const statusVal = fStatus.value.toLowerCase();
  const foughtVal = fFought.value;

  const dpStart = parseYMD(fDatePayStart.value);
  const dpEnd = parseYMD(fDatePayEnd.value);
  if (dpEnd) dpEnd.setHours(23, 59, 59, 999);

  const dcbStart = parseYMD(fDateCbStart.value);
  const dcbEnd = parseYMD(fDateCbEnd.value);
  if (dcbEnd) dcbEnd.setHours(23, 59, 59, 999);

  const dsubStart = parseYMD(fDateSubStart.value);
  const dsubEnd = parseYMD(fDateSubEnd.value);
  if (dsubEnd) dsubEnd.setHours(23, 59, 59, 999);

  const dresStart = parseYMD(fDateResStart.value);
  const dresEnd = parseYMD(fDateResEnd.value);
  if (dresEnd) dresEnd.setHours(23, 59, 59, 999);

  currentRows = allRows.filter(tr => {{
      const cells = tr.cells;
      
      // Amount (Col 1)
      const amtStr = cells[1].textContent.replace(/[^0-9.-]+/g, '');
      const amt = parseFloat(amtStr) || 0;
      if (!isNaN(minAmt) && amt < minAmt) return false;
      if (!isNaN(maxAmt) && amt > maxAmt) return false;

      // Operator (Col 2)
      if (operatorVal && cells[2].textContent.trim().toLowerCase() !== operatorVal) return false;
      
      // Type (Col 4)
      if (typeVal && cells[4].textContent.trim().toLowerCase() !== typeVal) return false;
      
      // Bank (Col 5)
      if (bankVal && cells[5].textContent.trim().toLowerCase() !== bankVal) return false;

      // Fought (Col 8)
      if (foughtVal && cells[8].textContent.trim() !== foughtVal) return false;

      // Status (Col 9)
      if (statusVal && cells[9].textContent.trim().toLowerCase() !== statusVal) return false;

      // Date Filters (Col 6, 7, 10, 11)
      try {{
          const dp = parseDMY(cells[6].textContent.trim());
          if (dpStart && (!dp || dp < dpStart)) return false;
          if (dpEnd && (!dp || dp > dpEnd)) return false;

          const dcb = parseDMY(cells[7].textContent.trim());
          if (dcbStart && (!dcb || dcb < dcbStart)) return false;
          if (dcbEnd && (!dcb || dcb > dcbEnd)) return false;

          const dsub = parseDMY(cells[10].textContent.trim());
          if (dsubStart && (!dsub || dsub < dsubStart)) return false;
          if (dsubEnd && (!dsub || dsub > dsubEnd)) return false;

          const dres = parseDMY(cells[11].textContent.trim());
          if (dresStart && (!dres || dres < dresStart)) return false;
          if (dresEnd && (!dres || dres > dresEnd)) return false;
      }} catch (e) {{
          console.error("Filter date parse error", e);
      }}

      return true;
  }});

  // Reset to first page after filtering
  currentPage = 1;

  // Preserve existing sort order if active
  const activeSort = document.querySelector('.sortable.asc, .sortable.desc');
  if (activeSort) {{
      // Temporarily trigger click to re-sort (which flips it), so we manually enforce it
      // Actually, better to just let standard re-render happen, standard users just click headers again
  }}

  renderTable();
}}

// Attach event listeners to all filter inputs
document.querySelectorAll('#casos-filters input, #casos-filters select').forEach(el => {{
    el.addEventListener('input', applyFilters);
    el.addEventListener('change', applyFilters);
}});

document.getElementById('btn-reset-filters').addEventListener('click', () => {{
    document.querySelectorAll('#casos-filters input, #casos-filters select').forEach(el => el.value = '');
    applyFilters();
}});


function renderTable() {{
  const totalPages = Math.ceil(currentRows.length / rowsPerPage);
  if (currentPage < 1) currentPage = 1;
  if (currentPage > totalPages && totalPages > 0) currentPage = totalPages;

  // Hide all rows
  allRows.forEach(tr => tr.style.display = 'none');
  
  // Detach sorted rows conceptually, and re-append them in correct order
  // For performance with many rows, we just hide/show, but since we MIGHT be sorted, we must append them in order to tbody.
  tbody.innerHTML = '';
  currentRows.forEach(tr => tbody.appendChild(tr));

  const startIdx = (currentPage - 1) * rowsPerPage;
  const endIdx = Math.min(startIdx + rowsPerPage, currentRows.length);

  // Show only page rows
  for (let i = startIdx; i < endIdx; i++) {{
    currentRows[i].style.display = '';
  }}

  // Update info
  const infoEl = document.getElementById('page-info');
  infoEl.innerText = `Mostrando ${{currentRows.length > 0 ? startIdx + 1 : 0}}-${{endIdx}} de ${{currentRows.length}} casos`;

  // Update buttons
  const btnContainer = document.getElementById('page-buttons');
  btnContainer.innerHTML = '';

  // Prev
  const prevBtn = document.createElement('button');
  prevBtn.className = 'page-btn';
  prevBtn.innerText = 'Ant';
  prevBtn.disabled = currentPage === 1;
  prevBtn.onclick = () => {{ currentPage--; renderTable(); }};
  btnContainer.appendChild(prevBtn);

  // Page Numbers (Truncated logic for simplicity)
  let startPage = Math.max(1, currentPage - 2);
  let endPage = Math.min(totalPages, currentPage + 2);
  
  if (startPage > 1) {{
      const firstBtn = document.createElement('button');
      firstBtn.className = 'page-btn';
      firstBtn.innerText = '1';
      firstBtn.onclick = () => {{ currentPage = 1; renderTable(); }};
      btnContainer.appendChild(firstBtn);
      if (startPage > 2) {{
         const dots = document.createElement('span');
         dots.innerText = '...';
         dots.style.margin = '0 4px';
         btnContainer.appendChild(dots);
      }}
  }}

  for (let i = startPage; i <= endPage; i++) {{
    const btn = document.createElement('button');
    btn.className = `page-btn ${{i === currentPage ? 'active' : ''}}`;
    btn.innerText = i;
    btn.onclick = () => {{ currentPage = i; renderTable(); }};
    btnContainer.appendChild(btn);
  }}
  
  if (endPage < totalPages) {{
      if (endPage < totalPages - 1) {{
         const dots = document.createElement('span');
         dots.innerText = '...';
         dots.style.margin = '0 4px';
         btnContainer.appendChild(dots);
      }}
      const lastBtn = document.createElement('button');
      lastBtn.className = 'page-btn';
      lastBtn.innerText = totalPages;
      lastBtn.onclick = () => {{ currentPage = totalPages; renderTable(); }};
      btnContainer.appendChild(lastBtn);
  }}

  // Next
  const nextBtn = document.createElement('button');
  nextBtn.className = 'page-btn';
  nextBtn.innerText = 'Sig';
  nextBtn.disabled = currentPage === totalPages || totalPages === 0;
  nextBtn.onclick = () => {{ currentPage++; renderTable(); }};
  btnContainer.appendChild(nextBtn);
}}

// Initialize Table
renderTable();

// Sorting Logic
document.querySelectorAll('.sortable').forEach(th => {{
  th.addEventListener('click', function() {{
    const colIdx = parseInt(this.getAttribute('data-col'));
    const isAsc = this.classList.contains('asc');
    const isAmount = this.getAttribute('data-type') === 'amount';

    // Reset all headers
    document.querySelectorAll('.sortable').forEach(h => {{
      h.classList.remove('asc', 'desc');
    }});

    // Set new direction
    const direction = isAsc ? -1 : 1;
    this.classList.add(isAsc ? 'desc' : 'asc');

    currentRows.sort((a, b) => {{
      const cellA = a.cells[colIdx];
      const cellB = b.cells[colIdx];
      
      let valA, valB;
      
      if (isAmount) {{
         // Parse "$1,234.56" to 1234.56
         valA = parseFloat(cellA.textContent.replace(/[^0-9.-]+/g, '')) || 0;
         valB = parseFloat(cellB.textContent.replace(/[^0-9.-]+/g, '')) || 0;
      }} else {{
         // Fallback to data-sort timestamp for dates
         valA = parseFloat(cellA.getAttribute('data-sort')) || 0;
         valB = parseFloat(cellB.getAttribute('data-sort')) || 0;
      }}

      return valA < valB ? -1 * direction : (valA > valB ? 1 * direction : 0);
    }});

    currentPage = 1;
    renderTable();
  }});
}});

/* ── Initial Render ────────────────────────── */
// Ensure we run the render loop after defining everything
const RAW_DATA = {_json(data)};
const STATUS_CONFIG = {_json(js_status_config)};

let chartObjMain = null;
let chartObjStatus = null;
let chartObjSuccess = null;
let chartObjType = null;
let chartObjCountry = null;

const fmtNum = new Intl.NumberFormat('en-US');
const fmtCur = new Intl.NumberFormat('en-US', {{ style: 'currency', currency: 'USD' }});

function buildStackedDatasets(dataObj, metricKey) {{
    const ds = [];
    for (const status in STATUS_CONFIG) {{
        ds.push({{
            label: STATUS_CONFIG[status].label,
            data: dataObj[metricKey][status],
            backgroundColor: STATUS_CONFIG[status].color,
            borderRadius: 4
        }});
    }}
    return ds;
}}

function buildListHTML(listData, totalRecords, nameKey, showPct = false) {{
    let html = '';
    listData.forEach((item, i) => {{
        const pct = totalRecords ? (item.count / totalRecords * 100) : 0;
        const pctStr = showPct ? ` <span style="font-size:0.8rem;color:#7a7a8c;font-weight:normal;margin-left:4px">(${{pct.toFixed(1)}}%)</span>` : '';
        html += `
        <div class="op-row">
            <span class="op-rank">${{i+1}}</span>
            <span class="op-name">${{item[nameKey]}}</span>
            <div class="op-bar-wrapper">
                <div class="op-bar" style="width:${{pct.toFixed(1)}}%"></div>
            </div>
            <span class="op-count">${{fmtNum.format(item.count)}}${{pctStr}}</span>
            <span class="op-amount">${{fmtCur.format(item.total)}}</span>
        </div>`;
    }});
    return html;
}}

function renderDashboardMode(isFoughtOnly) {{
    const key = isFoughtOnly ? 'fought' : 'all';
    const st = RAW_DATA[key];
    
    // 1. Update Stat Cards Text
    document.getElementById('val_total_records').innerText = fmtNum.format(st.total_records);
    document.getElementById('val_total_amount').innerText = fmtCur.format(st.total_amount);
    document.getElementById('val_fought_amount').innerText = fmtCur.format(st.fought_amount);
    document.getElementById('val_fought_count').innerText = fmtNum.format(st.fought_count);
    document.getElementById('val_recovered_amount').innerText = fmtCur.format(st.recovered_amount);
    document.getElementById('val_won_count').innerText = fmtNum.format(st.won_count);
    document.getElementById('val_lost_amount').innerText = fmtCur.format(st.lost_amount);
    document.getElementById('val_lost_count').innerText = fmtNum.format(st.lost_count);
    document.getElementById('val_success_rate').innerText = st.success_rate + '%';
    
    // 2. Build Status Breakdown HTML
    let st_html = '';
    for (const status in STATUS_CONFIG) {{
        const cfg = STATUS_CONFIG[status];
        const info = st.status_summary[status];
        st_html += `
        <div class="status-card" style="border-left: 4px solid ${{cfg.color}}; background:${{cfg.color}}15">
            <div class="status-card-label">${{cfg.label}}</div>
            <div class="status-card-count">${{fmtNum.format(info.count)}}</div>
            <div class="status-card-amount">${{fmtCur.format(info.amount)}}</div>
        </div>`;
    }}
    document.getElementById('status_cards_container').innerHTML = st_html;

    // 3. Build List HTML dynamically
    document.getElementById('list_operators').innerHTML = buildListHTML(st.top_operators, st.total_records, 'operator', true);
    document.getElementById('list_operators_won').innerHTML = buildListHTML(st.top_operators_won, st.won_count || 1, 'operator');
    document.getElementById('list_banks_total').innerHTML = buildListHTML(st.top_banks_total, st.total_records, 'bank', true);
    document.getElementById('list_banks_won').innerHTML = buildListHTML(st.top_banks_won, st.won_count || 1, 'bank'); // pass won_count as baseline for pct

    // 4. Destroy & Recreate Charts
    const fontFam = "'Figtree', system-ui, sans-serif";
    Chart.defaults.font.family = fontFam;
    
    // Determine which date grouping suffix to use
    const dateMetric = document.getElementById('dateChartToggle').value;
    const suffix = dateMetric === 'payment' ? '_payment' : '_cb';
    
    // Determine which metric array is selected for the main chart
    const mainMetric = document.getElementById('mainChartToggle').value;
    const arrayName = mainMetric === 'amount' ? `monthly_by_status${{suffix}}` : `monthly_count_by_status${{suffix}}`;
    
    if(chartObjMain) chartObjMain.destroy();
    chartObjMain = new Chart(document.getElementById('chartMain'), {{
        type: 'bar',
        data: {{
            labels: st[`month_labels${{suffix}}`],
            datasets: buildStackedDatasets(st, arrayName)
        }},
        options: {{
            responsive: true, maintainAspectRatio: false,
            plugins: {{ 
                legend: {{ position: 'top', labels: {{font: {{family: fontFam}}}}}}, 
                tooltip: {{ mode: 'index', intersect: false }} 
            }},
            scales: {{ x: {{ stacked: true, grid: {{ display: false }} }}, y: {{ stacked: true, beginAtZero: true }} }}
        }}
    }});

    if(chartObjStatus) chartObjStatus.destroy();
    chartObjStatus = new Chart(document.getElementById('chartStatus'), {{
        type: 'doughnut',
        data: {{
            labels: st.status_donut_labels,
            datasets: [{{ data: st.status_donut_values, backgroundColor: st.status_donut_colors }}]
        }},
        options: {{
            responsive: true, maintainAspectRatio: false,
            plugins: {{ legend: {{ position: 'bottom', labels: {{font: {{family: fontFam}}}}}} }}
        }}
    }});

    if(chartObjType) chartObjType.destroy();
    chartObjType = new Chart(document.getElementById('chartType'), {{
        type: 'doughnut',
        data: {{
            labels: st.type_donut_labels,
            datasets: [{{ data: st.type_donut_values, backgroundColor: st.type_donut_colors }}]
        }},
        options: {{
            responsive: true, maintainAspectRatio: false,
            plugins: {{ 
                legend: {{ position: 'bottom', labels: {{font: {{family: fontFam}}}}}} 
            }}
        }}
    }});

    if(chartObjCountry) chartObjCountry.destroy();
    chartObjCountry = new Chart(document.getElementById('chartCountry'), {{
        type: 'doughnut',
        data: {{
            labels: st.country_donut_labels,
            datasets: [{{ data: st.country_donut_values, backgroundColor: st.country_donut_colors }}]
        }},
        options: {{
            responsive: true, maintainAspectRatio: false,
            plugins: {{ 
                legend: {{ position: 'bottom', labels: {{font: {{family: fontFam}}}}}} 
            }}
        }}
    }});

    if(chartObjSuccess) chartObjSuccess.destroy();
    chartObjSuccess = new Chart(document.getElementById('chartSuccess'), {{
        type: 'line',
        data: {{
            labels: st[`month_labels${{suffix}}`],
            datasets: [{{
                label: 'Tasa de Éxito',
                data: st[`monthly_success${{suffix}}`],
                borderColor: '#2196F3',
                backgroundColor: 'rgba(33, 150, 243, 0.1)',
                tension: 0.4,
                fill: true
            }}]
        }},
        options: {{
            responsive: true, maintainAspectRatio: false,
            plugins: {{ legend: {{ display: false }} }},
            scales: {{ y: {{ max: 100, min: 0 }} }}
        }}
    }});
}}

const toggleEl = document.getElementById('foughtToggle');
toggleEl.addEventListener('change', (e) => {{
    renderDashboardMode(e.target.checked);
}});

const mainChartToggleEl = document.getElementById('mainChartToggle');
mainChartToggleEl.addEventListener('change', (e) => {{
    // Update the title
    if (e.target.value === 'amount') {{
        document.getElementById('mainChartTitle').innerText = 'Importe por Mes (MXN)';
    }} else {{
        document.getElementById('mainChartTitle').innerText = 'Cantidad por Mes';
    }}
    // Re-render everything to pick up the new toggle value
    renderDashboardMode(document.getElementById('foughtToggle').checked);
}});

const dateChartToggleEl = document.getElementById('dateChartToggle');
dateChartToggleEl.addEventListener('change', (e) => {{
    renderDashboardMode(document.getElementById('foughtToggle').checked);
}});
renderDashboardMode(false);
</script>
</body>
</html>"""
    return html


# ---------------------------------------------------------------------------
# GitHub Integration
# ---------------------------------------------------------------------------
def push_to_github(file_path):
    print("\n[4/4] Committing to GitHub...")
    try:
        # Check if git is available
        subprocess.run(["git", "--version"], check=True, capture_output=True)
        
        # Check if repo exists
        git_check = subprocess.run(["git", "status"], capture_output=True, text=True)
        if "not a git repository" in git_check.stderr.lower() or "not recognized" in git_check.stderr.lower():
            print("      -> WARNING: This folder is not a valid git repository or git is not installed.")
            return

        # Add the file
        subprocess.run(["git", "add", file_path], check=True, capture_output=True)
        
        # Commit
        commit_msg = f"Auto-update: Dashboard generated {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        subprocess.run(["git", "commit", "-m", commit_msg], check=True, capture_output=True)
        
        # Push
        print("      -> Pushing to remote repository...")
        subprocess.run(["git", "push"], check=True, capture_output=True)
        print("      -> Successfully committed and pushed to GitHub!")
        
    except FileNotFoundError:
        print("      -> ERROR: Git executable not found on the system pathway.")
    except subprocess.CalledProcessError as e:
        # Check if there was simply nothing to commit
        if "nothing to commit" in getattr(e, 'stdout', b'').decode('utf-8').lower() or \
           "nothing to commit" in getattr(e, 'stderr', b'').decode('utf-8').lower():
            print("      -> No changes detected in the dashboard HTML. Skipping commit.")
        else:
            print(f"      -> ERROR during Git operations: {e}")

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    print("[1/3] Fetching data from database...")
    df = fetch_data()
    print(f"      -> {len(df)} chargeback records loaded.")

    print("[2/3] Processing data...")
    data = process_data(df)

    print("[3/3] Generating HTML dashboard...")
    html = generate_html(data)

    out_path = os.path.join(os.path.dirname(__file__), "dashboard_output.html")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"DONE! Dashboard saved to: {out_path}")
    print("      Open the file in your browser to view the dashboard.")

    # Automate github submission
    push_to_github(out_path)


if __name__ == "__main__":
    main()
