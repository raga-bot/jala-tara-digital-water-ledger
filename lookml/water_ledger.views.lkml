# ============================================================
# Jala-Tara Digital Water Ledger — LookML Views
# ============================================================

# ── VIEW: water_balance ──────────────────────────────────────
view: water_balance {
  sql_table_name: `jala-tara-prod.jala_tara.v_water_balance` ;;

  # ── DIMENSIONS ──────────────────────────────────────────
  dimension: date {
    type:        date
    sql:         ${TABLE}.date ;;
    label:       "Date"
    description: "Date of water balance reading"
  }

  dimension: village_ward {
    type:        string
    sql:         ${TABLE}.village_ward ;;
    label:       "Village Ward"
    description: "Ward identifier (Ward_A through Ward_E)"
  }

  dimension: non_revenue_water {
    type:        number
    sql:         ${TABLE}.non_revenue_water ;;
    label:       "Non-Revenue Water (L)"
    description: "Water pumped but not metered at household level — includes leaks and unauthorized tapping"
    value_format: "#,##0.00"
  }

  dimension: leakage_pct {
    type:        number
    sql:         ${TABLE}.leakage_pct ;;
    label:       "Leakage %"
    description: "Percentage of pumped water that did not reach household meters"
    value_format: "0.00\"%\""
  }

  # ── MEASURES ────────────────────────────────────────────
  measure: total_pumped {
    type:        sum
    sql:         ${TABLE}.total_pumped ;;
    label:       "Total Pumped (L)"
    value_format: "#,##0"
  }

  measure: total_consumed {
    type:        sum
    sql:         ${TABLE}.total_consumed ;;
    label:       "Total Consumed (L)"
    value_format: "#,##0"
  }

  measure: avg_leakage_pct {
    type:        average
    sql:         ${TABLE}.leakage_pct ;;
    label:       "Avg Leakage %"
    value_format: "0.00\"%\""
  }

  measure: total_non_revenue_water {
    type:        sum
    sql:         ${TABLE}.non_revenue_water ;;
    label:       "Total Non-Revenue Water (L)"
    description: "Total water lost across all wards for selected period"
    value_format: "#,##0"
  }

  # ── PARAMETERS ──────────────────────────────────────────
  parameter: leakage_alert_threshold {
    type:          number
    default_value: "20"
    label:         "Leakage Alert Threshold (%)"
    description:   "Set the % leakage above which a ward is considered critical. Default: 20%"
  }

  # ── CALCULATED DIMENSION using parameter ────────────────
  dimension: leakage_alert_status {
    type: string
    sql:
      CASE
        WHEN ${TABLE}.leakage_pct > {% parameter leakage_alert_threshold %} THEN 'ABOVE THRESHOLD'
        ELSE 'WITHIN LIMIT'
      END ;;
    label:       "Leakage Alert Status"
    description: "Flags wards exceeding the user-defined leakage threshold"
  }

  # ── PUMP SCHEDULE RECOMMENDATION ────────────────────────
  dimension: pump_schedule_recommendation {
    type: string
    sql:
      CASE
        WHEN ${TABLE}.leakage_pct > 30 THEN 'PUMP ON — CRITICAL LOSS DETECTED'
        WHEN ${TABLE}.leakage_pct > 20 THEN 'PUMP ON — HIGH LOSS, INVESTIGATE'
        WHEN ${TABLE}.leakage_pct > 10 THEN 'PUMP STANDBY — MODERATE LOSS'
        ELSE 'PUMP STANDBY — NORMAL'
      END ;;
    label:       "Pump Schedule Recommendation"
    description: "Demand-based pump scheduling logic derived from leakage percentage"
  }
}


# ── VIEW: waste_index ────────────────────────────────────────
view: waste_index {
  sql_table_name: `jala-tara-prod.jala_tara.v_waste_index` ;;

  dimension: date {
    type: date
    sql:  ${TABLE}.date ;;
  }

  dimension: village_ward {
    type: string
    sql:  ${TABLE}.village_ward ;;
  }

  dimension: waste_category {
    type:  string
    sql:   ${TABLE}.waste_category ;;
    label: "Waste Category"
    description: "ACCEPTABLE / MODERATE / HIGH / CRITICAL based on leakage %"
  }

  # ── WASTE INDEX MEASURE (core LookML requirement) ────────
  measure: waste_index_score {
    type:        average
    sql:         ${TABLE}.waste_index_score ;;
    label:       "Waste Index Score"
    description: "0–5 scale composite waste score. Score >3 = critical intervention needed. Score <1 = acceptable."
    value_format: "0.00"
  }

  measure: avg_leakage_pct {
    type:        average
    sql:         ${TABLE}.leakage_pct ;;
    label:       "Avg Leakage %"
    value_format: "0.00\"%\""
  }
}


# ── VIEW: anomaly_flags ──────────────────────────────────────
view: anomaly_flags {
  sql_table_name: `jala-tara-prod.jala_tara.anomaly_flags` ;;

  dimension: date {
    type: date
    sql:  ${TABLE}.date ;;
  }

  dimension: hour {
    type:  number
    sql:   ${TABLE}.hour ;;
    label: "Hour of Day"
  }

  dimension: village_ward {
    type: string
    sql:  ${TABLE}.village_ward ;;
  }

  dimension: anomaly_type {
    type:  string
    sql:   ${TABLE}.anomaly_type ;;
    label: "Anomaly Type"
    description: "midnight_flow | meter_spike | pump_dropout | unclassified"
  }

  dimension: severity_tier {
    type: string
    sql:
      CASE
        WHEN ${TABLE}.severity > 0.65 THEN 'HIGH'
        WHEN ${TABLE}.severity > 0.55 THEN 'MEDIUM'
        ELSE 'LOW'
      END ;;
    label:       "Severity Tier"
    description: "HIGH / MEDIUM / LOW based on Isolation Forest severity score"
  }

  measure: total_anomalies {
    type:        count
    label:       "Total Anomalies Detected"
    description: "Count of all ML-flagged anomaly records"
  }

  measure: avg_severity {
    type:        average
    sql:         ${TABLE}.severity ;;
    label:       "Avg Severity Score"
    value_format: "0.0000"
  }

  measure: avg_ml_confidence {
    type:        average
    sql:         ${TABLE}.ml_confidence ;;
    label:       "Avg ML Confidence"
    value_format: "0.0000"
  }
}


# ── VIEW: midnight_flow ──────────────────────────────────────
view: midnight_flow {
  sql_table_name: `jala-tara-prod.jala_tara.v_midnight_flow` ;;

  dimension: date {
    type: date
    sql:  ${TABLE}.date ;;
  }

  dimension: village_ward {
    type: string
    sql:  ${TABLE}.village_ward ;;
  }

  dimension: leak_status {
    type:  string
    sql:   ${TABLE}.leak_status ;;
    label: "Leak Status"
    description: "CONFIRMED_LEAK | SUSPECTED_LEAK | NORMAL"
  }

  measure: total_midnight_consumption {
    type:        sum
    sql:         ${TABLE}.midnight_consumption ;;
    label:       "Total Midnight Water Loss (L)"
    description: "Total water consumed between 1–4 AM across all wards — non-zero = leak indicator"
    value_format: "#,##0.00"
  }

  measure: confirmed_leak_days {
    type:  count
    filters: [leak_status: "CONFIRMED_LEAK"]
    label: "Confirmed Leak Days"
  }
}


# ── VIEW: peak_demand ────────────────────────────────────────
view: peak_demand {
  sql_table_name: `jala-tara-prod.jala_tara.v_peak_demand` ;;

  dimension: hour {
    type:  number
    sql:   ${TABLE}.hour ;;
    label: "Hour of Day"
  }

  dimension: village_ward {
    type: string
    sql:  ${TABLE}.village_ward ;;
  }

  dimension: demand_rank {
    type:  number
    sql:   ${TABLE}.demand_rank ;;
    label: "Demand Rank"
    description: "1 = highest demand hour for this ward (RANK OVER PARTITION BY village_ward)"
  }

  measure: avg_consumption {
    type:        average
    sql:         ${TABLE}.avg_consumption ;;
    label:       "Avg Consumption (L/hr)"
    value_format: "0.00"
  }
}
