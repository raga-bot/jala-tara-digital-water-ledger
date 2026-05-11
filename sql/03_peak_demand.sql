CREATE OR REPLACE VIEW `jala-tara-prod.jala_tara.v_peak_demand` AS
SELECT
  hour,
  village_ward,
  ROUND(AVG(liters_consumed), 2) AS avg_consumption,
  RANK() OVER (
    PARTITION BY village_ward
    ORDER BY AVG(liters_consumed) DESC
  ) AS demand_rank
FROM `jala-tara-prod.jala_tara.household_consumption`
GROUP BY 1, 2;
