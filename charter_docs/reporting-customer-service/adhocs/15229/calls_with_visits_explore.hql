set hive.cli.print.header=true; 

SELECT 
visit_type
,substr(call_date,0,7) as call_month
,count(call_inbound_key) as segments
,count(DISTINCT call_inbound_key) as calls 
,count(call_inbound_key)/count(DISTINCT call_inbound_key) as segment_call_ratio
FROM prod.cs_calls_with_visits
WHERE call_date>='2018-12-01'
AND call_date <'2019-01-01'
GROUP BY visit_type, substr(call_date,0,7)
--limit 1
;
