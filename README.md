# 💧 Jala-Tara Digital Water Ledger

> **MindMatrix VTU Internship Program — Data Project 02**
> Built by **Ragavarshini Besta Sudha** | Data Analyst Intern (Gen AI)
> Platform: Google Cloud (BigQuery + Looker Studio + Python)

---

## 📋 Table of Contents
1. [Problem Statement](#problem-statement)
2. [Solution Overview](#solution-overview)
3. [System Architecture](#system-architecture)
4. [Tech Stack](#tech-stack)
5. [Project Structure](#project-structure)
6. [Data Model](#data-model)
7. [SQL Views](#sql-views)
8. [ML Anomaly Detection](#ml-anomaly-detection)
9. [Dashboard](#dashboard)
10. [Dataset Status](#dataset-status)
11. [Key Findings](#key-findings)
12. [What Makes This Different](#what-makes-this-different)
13. [How to Run](#how-to-run)
14. [Success Criteria Checklist](#success-criteria-checklist)
15. [Confidentiality Notice](#confidentiality-notice)

---

## Problem Statement

Rural Panchayats in India pump water into overhead tanks based on **fixed timers** — with no visibility into actual household demand. This causes:

- 🚰 **Tank overflows** at the source (water waste)
- 🚱 **Dry taps** at tail-end households (water scarcity)
- 🕵️ **Invisible leaks** and unauthorized tapping going undetected for months
- 💡 **Unnecessary electricity costs** from pumps running when tanks are full

The Panchayat has no data to act on. Water is the most precious resource yet the least measured.

---

## Solution Overview

**Jala-Tara** builds a **Village Water Accounting System** that treats every litre like a rupee.

```
Raw Flow Data → BigQuery → SQL Views → ML Model → Looker Studio Dashboard
                                                          ↓
                                            Panchayat takes action
```

The system:
1. Ingests pump and household meter data into BigQuery
2. Calculates **Non-Revenue Water (NRW)** — the gap between what's pumped and what's consumed
3. Detects leaks, unauthorized tapping, and pump failures using **Isolation Forest ML**
4. Presents all insights on a **5-page mobile-responsive dashboard**

---

## System Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    DATA GENERATION LAYER                     │
│  generate_data.py (Python + Pandas + NumPy)                  │
│  • 3,600 pump inflow rows (5 wards × 24 hrs × 30 days)      │
│  • 72,000 household consumption rows (20 HH per ward)        │
│  • 3 injected anomalies for ML validation                    │
└──────────────────────────┬──────────────────────────────────┘
                           │ BigQuery Load Job (Python SDK)
┌──────────────────────────▼──────────────────────────────────┐
│                     STORAGE LAYER                            │
│  Google BigQuery — Project: jala-tara-prod                   │
│  Dataset: jala_tara                                          │
│  • pump_inflow          (3,600 rows)                         │
│  • household_consumption (72,000 rows)                       │
│  • anomaly_flags         (144 rows — ML output)              │
└──────────────────────────┬──────────────────────────────────┘
                           │ CREATE OR REPLACE VIEW
┌──────────────────────────▼──────────────────────────────────┐
│                  TRANSFORMATION LAYER                        │
│  4 BigQuery SQL Views                                        │
│  • v_water_balance    — NRW reconciliation + leakage %      │
│  • v_midnight_flow    — 1–4 AM leak detection               │
│  • v_peak_demand      — RANK() hourly demand analysis       │
│  • v_waste_index      — 0–5 waste scoring + category        │
└──────────────────────────┬──────────────────────────────────┘
                           │
           ┌───────────────┴───────────────┐
           │                               │
┌──────────▼──────────┐       ┌────────────▼────────────────┐
│    ML / AI LAYER     │       │    VISUALIZATION LAYER       │
│  anomaly_detection   │       │  Looker Studio — 5 pages    │
│  .py (scikit-learn)  │       │  • Executive Summary        │
│  Isolation Forest    │       │  • Leakage Heatmap          │
│  200 estimators      │       │  • Peak Demand Hours        │
│  contamination=0.04  │       │  • Midnight Flow Detection  │
│  144 anomalies → BQ  │       │  • AI Anomaly Intelligence  │
└─────────────────────┘       └─────────────────────────────┘
```

---

## Tech Stack

| Layer | Tool | Purpose |
|---|---|---|
| Storage | Google BigQuery (Sandbox) | 3 tables, SQL views, ML output |
| Data Generation | Python 3.12 + Pandas + NumPy | Realistic mock data with anomalies |
| ML / AI | scikit-learn (Isolation Forest) | Unsupervised anomaly detection |
| Visualization | Looker Studio (free) | 5-page interactive dashboard |
| Modeling | LookML | Waste Index measure + parameters |
| CLI | Google Cloud Shell | Script execution, BigQuery SDK |
| Version Control | GitHub | Project repository |

---

## Project Structure

```
jala-tara/
├── data/
│   ├── pump_inflow.csv              # 3,600 rows mock pump data
│   └── household_consumption.csv   # 72,000 rows mock HH data
├── sql/
│   ├── create_pump_inflow.sql
│   ├── create_household_consumption.sql
│   ├── create_anomaly_flags.sql
│   ├── 01_water_balance.sql
│   ├── 02_midnight_flow.sql
│   ├── 03_peak_demand.sql
│   └── 04_waste_index.sql
├── ml/
│   ├── generate_data.py             # Data generation + BigQuery load
│   └── anomaly_detection.py        # Isolation Forest ML model
├── lookml/
│   ├── water_ledger.model.lkml     # Explores + joins
│   └── water_ledger.views.lkml    # All 5 views with measures + parameters
├── Jala-Tara_Dashboard_Link.txt   # Dashboard page Link
├── Jala_Tara_Dashboard.pdf        # Dashboard page pdf
└── README.md
```

---

## Data Model

### Table: `pump_inflow` (3,600 rows)
| Column | Type | Description |
|---|---|---|
| date | DATE | Reading date |
| hour | INT64 | Hour of day (0–23) |
| village_ward | STRING | Ward_A through Ward_E |
| pump_id | STRING | Pump identifier (e.g. PUMP_A01) |
| liters_pumped | FLOAT64 | Litres pumped this hour |
| power_kwh | FLOAT64 | Electricity consumed (liters × 0.0008) |

### Table: `household_consumption` (72,000 rows)
| Column | Type | Description |
|---|---|---|
| date | DATE | Reading date |
| hour | INT64 | Hour of day |
| village_ward | STRING | Ward identifier |
| meter_id | STRING | Meter identifier (e.g. MTR_A01) |
| household_id | STRING | Household ID (e.g. HH_A01) |
| liters_consumed | FLOAT64 | Litres consumed this hour |

### Table: `anomaly_flags` (144 rows — ML generated)
| Column | Type | Description |
|---|---|---|
| date | DATE | Anomaly date |
| hour | INT64 | Hour of anomaly |
| village_ward | STRING | Affected ward |
| anomaly_type | STRING | midnight_flow / meter_spike / pump_dropout / unclassified |
| severity | FLOAT64 | Isolation Forest severity (0–1) |
| ml_confidence | FLOAT64 | Model confidence score |

**Join key across all tables:** `date + hour + village_ward`

---

## SQL Views

### `v_water_balance` — Core Reconciliation
```sql
SELECT
  p.date, p.village_ward,
  SUM(p.liters_pumped)                          AS total_pumped,
  SUM(h.liters_consumed)                        AS total_consumed,
  SUM(p.liters_pumped) - SUM(h.liters_consumed) AS non_revenue_water,
  ROUND(SAFE_DIVIDE(
    SUM(p.liters_pumped) - SUM(h.liters_consumed),
    SUM(p.liters_pumped)) * 100, 2)             AS leakage_pct
FROM pump_inflow p
LEFT JOIN household_consumption h
  ON p.date = h.date AND p.hour = h.hour AND p.village_ward = h.village_ward
GROUP BY 1, 2
```

### `v_midnight_flow` — Leak Detection
Flags water consumption between 1–4 AM as `CONFIRMED_LEAK` (>50L), `SUSPECTED_LEAK` (>10L), or `NORMAL`.

### `v_peak_demand` — Demand Analysis
Uses `RANK() OVER (PARTITION BY village_ward ORDER BY AVG(liters_consumed) DESC)` to identify top consumption hours per ward.

### `v_waste_index` — Scoring
Scores each ward-day on a 0–5 waste scale: `ACCEPTABLE / MODERATE / HIGH / CRITICAL`.

---

## ML Anomaly Detection

**Algorithm:** Isolation Forest (scikit-learn)

**Why Isolation Forest?**
- Unsupervised — no labelled training data needed
- Designed for tabular time-series anomaly detection
- Handles high-dimensional feature spaces efficiently

**Features used:**
| Feature | Rationale |
|---|---|
| liters_pumped | Raw pump volume |
| total_consumed | Aggregated household demand |
| loss | Absolute NRW per hour |
| loss_ratio | Normalised loss as fraction of pumped |
| hour_sin / hour_cos | Cyclical encoding of hour — preserves 23→0 continuity |
| is_night | Binary flag for hours 10 PM – 5 AM |

**Model config:**
```python
IsolationForest(n_estimators=200, contamination=0.04, random_state=42)
```

**Results:**
| Anomaly Type | Count | Avg Severity |
|---|---|---|
| unclassified | 131 | 0.5999 |
| meter_spike | 10 | 0.6392 |
| midnight_flow | 3 | 0.6614 |
| **Total** | **144** | **0.61** |

**Injected anomalies all detected:**
- ✅ Ward_B Jun 15 — midnight_flow (320L pumped at 1–3 AM)
- ✅ Ward_D Jun 22 — meter_spike (HH_D05 4× normal consumption)
- ✅ Ward_C Jun 28 — pump_dropout (0L during peak hours 6–12 AM)

---

## Dashboard

**Tool:** Looker Studio (free tier)
**Pages:** 5 | **Data sources:** 5 | **Cache:** 12 hours

| Page | Data Source | Key Insight |
|---|---|---|
| Executive Summary | v_water_balance | 11.6M L pumped, 95.78% NRW, pump dropout visible Jun 28 |
| Leakage Heatmap | v_waste_index | Color-coded leakage table + waste index bar chart |
| Peak Demand Hours | v_peak_demand | 6–8 AM peak clearly visible, demand_rank by ward |
| Midnight Flow Detection | v_midnight_flow | Ward_B Jun 15 CONFIRMED_LEAK (392L) spike visible |
| AI Anomaly Intelligence | anomaly_flags | 144 anomalies, pie chart, severity table, time series |

---

## Dataset Status

| Dataset | Rows | Location | Status |
|---|---|---|---|
| pump_inflow.csv | 3,600 | `jala-tara-prod.jala_tara.pump_inflow` | ✅ Loaded |
| household_consumption.csv | 72,000 | `jala-tara-prod.jala_tara.household_consumption` | ✅ Loaded |
| anomaly_flags | 144 | `jala-tara-prod.jala_tara.anomaly_flags` | ✅ ML Generated |

**Note:** All data is synthetic/mock. No real personal or sensitive data used.

---

## Key Findings

1. **Ward_B on June 15** had 392L of water consumption between 1–4 AM — classified as `CONFIRMED_LEAK` with the highest ML severity score (0.6614). This is consistent with a broken pipe or open valve scenario.

2. **Ward_C on June 28** shows a sharp dip in the Executive Summary time series — the pump dropout during 6–12 AM peak hours left the ward without water during morning demand peak.

3. **6 AM and 7 AM** are the peak demand hours across all 5 wards (demand_rank = 1), confirming that pumping should be concentrated in this window rather than running throughout the day.

4. **Meter spike anomalies (10 records)** were detected around Ward_D June 22, corresponding to the injected 4× consumption event — consistent with unauthorized tapping or meter tampering.

---

## What Makes This Different

| Feature | Standard Project | This Project |
|---|---|---|
| Schema design | Single flat table | 3 normalized tables with composite join keys |
| Anomaly detection | None | Isolation Forest ML model (scikit-learn) |
| Dashboard pages | 1–2 pages | 5 pages including AI Anomaly Intelligence |
| Data realism | Random numbers | Peak/off-peak patterns, weekend uplift, injected anomalies |
| Cost optimization | Not addressed | 12-hour cache on all 5 data sources |
| Feature engineering | N/A | Cyclical hour encoding (sin/cos), loss_ratio, is_night |
| LookML | Not included | Full model with Waste Index measure + leakage alert parameter |

---

## How to Run

### Prerequisites
- Google Cloud account (free sandbox)
- Python 3.9+
- Cloud Shell or local environment with gcloud SDK

### Step 1 — Setup GCP
```bash
gcloud config set project jala-tara-prod
```

### Step 2 — Create BigQuery dataset and tables
Run in BigQuery Console:
```sql
CREATE SCHEMA IF NOT EXISTS `jala-tara-prod.jala_tara`
OPTIONS (location = 'asia-south1');
```
Then run `sql/create_pump_inflow.sql`, `sql/create_household_consumption.sql`, `sql/create_anomaly_flags.sql`

### Step 3 — Install dependencies
```bash
pip install pandas numpy scikit-learn google-cloud-bigquery db-dtypes
```

### Step 4 — Generate and load data
```bash
python ml/generate_data.py
```

### Step 5 — Create SQL views
Run in BigQuery Console (in order):
```
sql/01_water_balance.sql
sql/02_midnight_flow.sql
sql/03_peak_demand.sql
sql/04_waste_index.sql
```

### Step 6 — Run ML anomaly detection
```bash
python ml/anomaly_detection.py
```

### Step 7 — Verify
```sql
SELECT anomaly_type, COUNT(*) as count
FROM `jala-tara-prod.jala_tara.anomaly_flags`
GROUP BY anomaly_type;
-- Expected: midnight_flow=3, meter_spike=10, unclassified=131
```

### Step 8 — Dashboard
Open [lookerstudio.google.com](https://lookerstudio.google.com), connect all 5 BigQuery views, and build the 5-page report.

---

## Success Criteria Checklist

| Criterion | Status | Evidence |
|---|---|---|
| % Leakage calculated per ward | ✅ | `v_water_balance.leakage_pct` — Page 2 Leakage Heatmap |
| Dashboard shows Peak Demand Hours | ✅ | `v_peak_demand` with RANK() — Page 3 bar chart |
| $0 BigQuery query cost | ✅ | No partitioning (sandbox) + 12-hour Looker cache |

---

## Confidentiality Notice

This project was developed as part of the **MindMatrix VTU Internship Program**. All data used is mock/synthetic. Proprietary project scenarios must not be shared publicly without written consent. The developer retains the right to cite this work for academic and internship completion purposes only.

---

*Built with ❤️ for rural water governance | Jala-Tara means "Water Star" in Sanskrit*
