import pandas as pd
import numpy as np
from datetime import date, timedelta
from google.cloud import bigquery

PROJECT = "jala-tara-prod"
DATASET = "jala_tara"
client  = bigquery.Client(project=PROJECT)

wards = ['Ward_A', 'Ward_B', 'Ward_C', 'Ward_D', 'Ward_E']
dates = [date(2024, 6, 1) + timedelta(d) for d in range(30)]
np.random.seed(42)

# ── PUMP INFLOW ──────────────────────────────────────────
pump_rows = []
for d in dates:
    for h in range(24):
        for w in wards:
            peak = 1.0 if h in range(6, 9) or h in range(17, 20) else 0.1
            liters = max(0, np.random.normal(500 * peak, 40 * peak))

            # anomaly: pump dropout Ward_C on day 28 (hours 6–12)
            if w == 'Ward_C' and d == date(2024, 6, 28) and h in range(6, 13):
                liters = 0.0

            # anomaly: midnight flow Ward_B on day 15
            if w == 'Ward_B' and d == date(2024, 6, 15) and h in range(1, 4):
                liters = 320.0

            pump_rows.append({
                'date':          d,
                'hour':          h,
                'village_ward':  w,
                'pump_id':       f'PUMP_{w[-1]}01',
                'liters_pumped': round(liters, 2),
                'power_kwh':     round(liters * 0.0008, 4)
            })

pump_df = pd.DataFrame(pump_rows)
pump_df['date'] = pd.to_datetime(pump_df['date'])
pump_df.to_csv('data/pump_inflow.csv', index=False)
print(f"pump_inflow rows: {len(pump_df)}")

# ── HOUSEHOLD CONSUMPTION ────────────────────────────────
hh_rows = []
households_per_ward = 20

for d in dates:
    for h in range(24):
        for w in wards:
            for hh in range(1, households_per_ward + 1):
                peak = 1.0 if h in range(6, 9) or h in range(17, 20) else 0.05
                liters = max(0, np.random.normal(20 * peak, 3 * peak))

                # anomaly: consumption spike Ward_D on day 22
                if w == 'Ward_D' and d == date(2024, 6, 22) and hh == 5:
                    liters = liters * 10

                # anomaly: midnight consumption Ward_B on day 15 (leak)
                if w == 'Ward_B' and d == date(2024, 6, 15) and h in range(1, 4):
                    liters = np.random.uniform(3, 8)

                hh_rows.append({
                    'date':            d,
                    'hour':            h,
                    'village_ward':    w,
                    'meter_id':        f'MTR_{w[-1]}{hh:02d}',
                    'household_id':    f'HH_{w[-1]}{hh:02d}',
                    'liters_consumed': round(liters, 2)
                })

hh_df = pd.DataFrame(hh_rows)
hh_df['date'] = pd.to_datetime(hh_df['date'])
hh_df.to_csv('data/household_consumption.csv', index=False)
print(f"household_consumption rows: {len(hh_df)}")

# ── LOAD BOTH CSVs INTO BIGQUERY ─────────────────────────
job_config = bigquery.LoadJobConfig(
    write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE,
    skip_leading_rows = 1,
    source_format     = bigquery.SourceFormat.CSV,
    autodetect        = True,
    time_partitioning = bigquery.TimePartitioning(
        type_  = bigquery.TimePartitioningType.DAY,
        field  = "date"
    )
)

for table_name, df, csv_path in [
    ("pump_inflow",           pump_df, "data/pump_inflow.csv"),
    ("household_consumption", hh_df,   "data/household_consumption.csv"),
]:
    table_id = f"{PROJECT}.{DATASET}.{table_name}"
    with open(csv_path, "rb") as f:
        job = client.load_table_from_file(f, table_id, job_config=job_config)
    job.result()
    table = client.get_table(table_id)
    print(f"Loaded {table.num_rows} rows into {table_id}")

print("\nPhase 2 complete!")
