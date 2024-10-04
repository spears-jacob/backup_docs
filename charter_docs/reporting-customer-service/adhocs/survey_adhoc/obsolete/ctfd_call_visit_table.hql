set hive.cli.print.header=true;
SELECT DISTINCT
visit_id
, received__timestamp
,prod.epoch_datehour(received__timestamp*1000, 'America/Denver')
,call_start_timestamp_utc
,prod.epoch_datehour(call_start_timestamp_utc,'America/Denver')
 FROM dev_tmp.cs_calls_with_prior_visit_adhoc WHERE call_end_date_utc>='2019-04-01' 
ORDER BY (call_start_timestamp_utc - received__timestamp) desc
LIMIT 10;

SELECT MAX (call_start_timestamp_utc - received__timestamp*1000)
FROM dev_tmp.cs_calls_with_prior_visit_adhoc;
