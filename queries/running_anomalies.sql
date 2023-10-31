CREATE OR REPLACE transactions.running_health ( 
with all_times_and_models as (
SELECT distinct
  t1.time,
  t2.model_name,
  t2.model_type,
FROM (SELECT DISTINCT TIME from transactions.transactions_raw) t1, (SELECT DISTINCT model_name, model_type from transactions.anomalies_raw) t2
ORDER BY time asc)


SELECT 
*,
SUM(has_ocurence) OVER (partition by model_name order by time,model_name asc ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) running_count_10,
FROM (
  SELECT 
  tm.*,
  COUNT(an.model_name) as has_ocurence,
  FROM all_times_and_models  tm
  LEFT JOIN transactions.anomalies_raw an ON tm.time = an.time and tm.model_name = an.model_name 
  GROUP BY 1,2,3
  order by time asc)
order by time asc)
