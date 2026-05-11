CREATE OR REPLACE VIEW `jala-tara-prod.jala_tara.v_water_balance` AS
SELECT
  p.date,
  p.village_ward,
  SUM(p.liters_pumped)                          AS total_pumped,
  SUM(h.liters_consumed)                        AS total_consumed,
  SUM(p.liters_pumped) - SUM(h.liters_consumed) AS non_revenue_water,
  ROUND(
    SAFE_DIVIDE(
      SUM(p.liters_pumped) - SUM(h.liters_consumed),
      SUM(p.liters_pumped)
    ) * 100, 2
  ) AS leakage_pct
FROM `jala-tara-prod.jala_tara.pump_inflow` p
LEFT JOIN `jala-tara-prod.jala_tara.household_consumption` h
  ON p.date = h.date
  AND p.hour = h.hour
  AND p.village_ward = h.village_ward
GROUP BY 1, 2;
