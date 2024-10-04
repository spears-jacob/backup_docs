SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.resultset.use.unique.column.names=false;
set hive.cli.print.header=true;

-- SET new_visits_table=stg_dasp.cs_calls_with_prior_visits;
-- SET new_call_table=stg_red.atom_cs_call_care_data_3;
-- SET test_date=2021-11-24;

--Assumption: Regardless of what else we've changed, the number of _handled_ segments for each call should be the same between
----- the call_care_data table and the calls_with_prior_visits table per visit
--All Segments Included (as)
------ Same number of handled segments (as.ns)

--discrepancy should be 0 for every call_inbound_key/customer_type/visit combo key

SELECT * FROM(
SELECT
      ccd.call_inbound_key
      , ccd.customer_type
      , visit_type
      , visit_id
      , call_date
      , call_segment_count
      , cwv_segment_count
      , (call_segment_count - cwv_segment_count) as discrepancy
      , "this should return 5 records" as should_return_5_records
FROM (
 --count of rows by calls in call_care_data
  --customer type because there are duplicates with different types
     SELECT
           CALL_inbound_key
           , customer_type
           , count(distinct segment_id) as call_segment_count
      FROM `${hiveconf:new_call_table}`
     WHERE segment_handled_flag=1
       AND call_end_date_utc='${hiveconf:test_date}'
     GROUP BY call_inbound_key,customer_type
 ) ccd
INNER JOIN (
 --count of rows by call in calls_with_prior_visits
  --cusotmer type is ncessary because there are duplicate with different types
     SELECT
            call_inbound_key
            , visit_type
            , visit_id
            , customer_type
            , count(call_date) as cwv_segment_count
            , call_date
      FROM `${hiveconf:new_visits_table}`
     WHERE call_date ='${hiveconf:test_date}'
     GROUP BY call_inbound_key, customer_type, visit_type, visit_id, call_date
 ) cwpv
   ON ccd.call_inbound_key=cwpv.call_inbound_key AND ccd.customer_type=cwpv.customer_type ) disc
  --WHERE call_inbound_key in ('2061130742975')
limit 5
;

SELECT * FROM(
SELECT
      ccd.call_inbound_key
      , ccd.customer_type
      , visit_type
      , visit_id
      , call_date
      , call_segment_count
      , cwv_segment_count
      , (call_segment_count - cwv_segment_count) as discrepancy
      , "this should return 0 records" as should_return_0_records
FROM (
 --count of rows by calls in call_care_data
  --customer type because there are duplicates with different types
     SELECT
           CALL_inbound_key
           , customer_type
           , count(distinct segment_id) as call_segment_count
      FROM `${hiveconf:new_call_table}`
     WHERE segment_handled_flag=1
       AND call_end_date_utc='${hiveconf:test_date}'
     GROUP BY call_inbound_key,customer_type
 ) ccd
INNER JOIN (
 --count of rows by call in calls_with_prior_visits
  --cusotmer type is ncessary because there are duplicate with different types
     SELECT
            call_inbound_key
            , visit_type
            , visit_id
            , customer_type
            , count(call_date) as cwv_segment_count
            , call_date
      FROM `${hiveconf:new_visits_table}`
     WHERE call_date ='${hiveconf:test_date}'
     GROUP BY call_inbound_key, customer_type, visit_type, visit_id, call_date
 ) cwpv
   ON ccd.call_inbound_key=cwpv.call_inbound_key AND ccd.customer_type=cwpv.customer_type ) disc
WHERE discrepancy<>0
  --AND call_inbound_key in ('2061130742975')
limit 10
;
