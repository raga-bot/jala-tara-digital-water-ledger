import pandas as pd
import numpy as np
from google.cloud import bigquery
from sklearn.ensemble import IsolationForest

PROJECT = "jala-tara-prod"
DATASET = "jala_tara"
client  = bigquery.Client(project=PROJECT)

print("Fetching data from BigQuery...")
query = """
SELECT
  p.date, p.hour, p.village_ward,
  p.liters_pumped,
  SUM(h.liters_consumed) AS total_consumed,
  p.liters_pumped - SUM(h.liters_consumed) AS loss
FROM `jala-tara-prod.jala_tara.pump_inflow` p
LEFT JOIN `jala-tara-prod.jala_tara.household_consumption` h
  ON p.date = h.date AND p.hour = h.hour AND p.village_ward = h.village_ward
GROUP BY 1, 2, 3, 4
"""
df = client.query(query).to_dataframe()
print(f"Fetched {len(df):,} rows")

df['hour_sin']   = np.sin(2 * np.pi * df['hour'] / 24)
df['hour_cos']   = np.cos(2 * np.pi * df['hour'] / 24)
df['loss_ratio'] = df['loss'] / (df['liters_pumped'] + 1)
df['is_night']   = (df['hour'].between(22, 24) | df['hour'].between(0, 5)).astype(int)

features = ['liters_pumped','total_consumed','loss','loss_ratio','hour_sin','hour_cos','is_night']
X = df[features].fillna(0)

print("Training Isolation Forest...")
model = IsolationForest(n_estimators=200, contamination=0.04, random_state=42, n_jobs=-1)
df['anomaly_score'] = model.fit_predict(X)
df['ml_confidence'] = model.score_samples(X)
df['is_anomaly']    = df['anomaly_score'] == -1
print(f"Total anomalies detected: {df['is_anomaly'].sum()}")

def classify(row):
    h = row['hour']
    if h in range(22,24) or h in range(0,6):
        if row['total_consumed'] > 20:
            return 'midnight_flow'
    if row['total_consumed'] > row['liters_pumped'] * 1.3:
        return 'meter_spike'
    if row['liters_pumped'] == 0 and h in range(6,20):
        return 'pump_dropout'
    return 'unclassified'

anomalies = df[df['is_anomaly']].copy()
anomalies['anomaly_type'] = anomalies.apply(classify, axis=1)
anomalies['severity']     = anomalies['ml_confidence'].abs()
print("\nAnomaly breakdown:")
print(anomalies['anomaly_type'].value_counts())

result = anomalies[['date','hour','village_ward','anomaly_type','severity','ml_confidence']].copy()
result['date'] = pd.to_datetime(result['date']).dt.date

table_id = f"{PROJECT}.{DATASET}.anomaly_flags"
job_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    schema=[
        bigquery.SchemaField("date",         "DATE"),
        bigquery.SchemaField("hour",         "INTEGER"),
        bigquery.SchemaField("village_ward", "STRING"),
        bigquery.SchemaField("anomaly_type", "STRING"),
        bigquery.SchemaField("severity",     "FLOAT"),
        bigquery.SchemaField("ml_confidence","FLOAT"),
    ]
)
job = client.load_table_from_dataframe(result, table_id, job_config=job_config)
job.result()
table = client.get_table(table_id)
print(f"\nLoaded {table.num_rows} anomaly records → {table_id}")
print("\nPhase 4 complete!")
