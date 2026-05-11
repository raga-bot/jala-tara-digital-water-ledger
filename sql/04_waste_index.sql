CREATE OR REPLACE VIEW `jala-tara-prod.jala_tara.v_waste_index` AS
SELECT
  date,
  village_ward,
  leakage_pct,
  CASE
    WHEN leakage_pct > 30 THEN 'CRITICAL'
    WHEN leakage_pct > 20 THEN 'HIGH'
    WHEN leakage_pct > 10 THEN 'MODERATE'
    ELSE 'ACCEPTABLE'
  END AS waste_category,
  ROUND(leakage_pct * 0.01 * 5, 2) AS waste_index_score
FROM `jala-tara-prod.jala_tara.v_water_balance`;
