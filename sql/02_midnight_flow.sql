CREATE OR REPLACE VIEW `jala-tara-prod.jala_tara.v_midnight_flow` AS
SELECT
  date,
  village_ward,
  SUM(liters_consumed)         AS midnight_consumption,
  COUNTIF(liters_consumed > 0) AS active_meters_at_night,
  CASE
    WHEN SUM(liters_consumed) > 50 THEN 'CONFIRMED_LEAK'
    WHEN SUM(liters_consumed) > 10 THEN 'SUSPECTED_LEAK'
    ELSE 'NORMAL'
  END AS leak_status
FROM `jala-tara-prod.jala_tara.household_consumption`
WHERE hour BETWEEN 1 AND 4
GROUP BY 1, 2
ORDER BY midnight_consumption DESC;
