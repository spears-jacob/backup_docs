
COALESCE(cwpv.calls_within_24_hrs,0) as calls_within_24_hrs,
partition_date_utc
FROM
asp_m2dot0_metric_agg ma
LEFT
JOIN (
  SELECT visit_id, count(DISTINCT call_inbound_key) as calls_within_24_hrs
  FROM cs_calls_with_prior_visits
  WHERE (call_date >= ('${hiveconf:call_start_date}') AND call_date < ('${hiveconf:end_date}'))
  GROUP BY visit_id) cwpv
  on cwpv.visit_id=ma.visit_id
AND (partition_date_utc >= ('${hiveconf:visit_start_date}') AND partition_date_utc < ('${hiveconf:end_date}'))
;

SELECT partition_date_utc, count(partition_date_utc)
FROM m2dot0_metric_agg
WHERE partition_date_utc >='${hiveconf:call_start_date}'
AND calls_within_24_hrs > 0
GROUP BY partition_date_utc
ORDER BY partition_date_utc desc
LIMIT 10;
