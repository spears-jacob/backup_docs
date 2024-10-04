SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.cli.print.header=true;

--Assumption: Segments should only match one account key

-- Same Data (sd)
------ Account Key (sd_ak)

--this should return 0 results, because all segments should match only one account key

SELECT
        c.segment_id
        ,COUNT(distinct encrypted_account_key_256) as should_return_5_records
  FROM `${hiveconf:new_call_table}` c
  WHERE call_end_date_utc>='${hiveconf:test_date}'
 GROUP BY c.segment_id
 LIMIT 5
;

SELECT
        c.segment_id
        ,COUNT(distinct encrypted_account_key_256) as should_return_0_records
  FROM `${hiveconf:new_call_table}` c
  WHERE call_end_date_utc>='${hiveconf:test_date}'
 GROUP BY c.segment_id
HAVING should_return_0_records>1
 LIMIT 10
;
