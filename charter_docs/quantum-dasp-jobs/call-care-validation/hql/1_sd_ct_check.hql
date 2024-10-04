SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.cli.print.header=true;

--SET new_call_table=387455165365/prod.atom_cs_call_care_data_3;
-- SET new_call_table=stg_red.atom_cs_call_care_data_3;
-- SET test_date=2021-11-18;
--Assuption: Duplicate segments should only exist because of an account key with
--- multiple account types.  Therefore duplicate segments should exist because of
--- different customer types and different customer types only.

-- Same Data (sd)
----- Customer Type (sd_ct)
--should return no records, since the only reason for duplication is multiple customer types on an account

SELECT
      segment_id
      ,customer_type
      ,count(segment_id) as should_return_5_records
 FROM `${hiveconf:new_call_table}` new
 WHERE call_end_date_utc >='${hiveconf:test_date}'
-- WHERE call_end_date_utc>='2020-12-15' AND call_end_date_utc<='2021-01-14'
GROUP BY segment_id, customer_type
limit 5
;

SELECT
      segment_id
      ,customer_type
      ,count(segment_id) as count_should_return_0_records
 FROM `${hiveconf:new_call_table}` new
 WHERE call_end_date_utc >='${hiveconf:test_date}'
-- WHERE call_end_date_utc>='2020-12-15' AND call_end_date_utc<='2021-01-14'
GROUP BY segment_id, customer_type
HAVING count_should_return_0_records>1
limit 50
;
