ma.portals_unique_acct_key AS portals_unique_acct_key,
ma.activated_experiments AS activated_experiments,
ma.technology_type AS technology_type,
COALESCE(cwpv.calls,0) as calls_within_24_hrs,
ma.denver_date AS denver_date
FROM (
  SELECT * FROM
  quantum_metric_agg_portals
  WHERE (denver_date >= ('${hiveconf:visit_start_date}') AND denver_date < ('${hiveconf:end_date}'))
  ) ma
LEFT
JOIN (
  SELECT visit_id, count(DISTINCT call_inbound_key) as calls
  FROM `${env:CPV}`
  -- FROM cs_calls_with_prior_visits for debugging
  WHERE (call_date >= ('${hiveconf:call_start_date}') AND call_date < ('${hiveconf:end_date}'))
  GROUP BY visit_id) cwpv
  on cwpv.visit_id=ma.visit_id
WHERE 1=1
AND (denver_date >= ('${hiveconf:call_start_date}') AND denver_date < ('${hiveconf:end_date}'))

;

MSCK REPAIR TABLE quantum_metric_agg_portals;

SELECT denver_date, count(denver_date)
FROM quantum_metric_agg_portals
WHERE (denver_date >='${hiveconf:call_start_date}'
  AND  denver_date <  '${hiveconf:end_date}')
AND calls_within_24_hrs > 0
GROUP BY denver_date
ORDER BY denver_date desc
LIMIT 10;
