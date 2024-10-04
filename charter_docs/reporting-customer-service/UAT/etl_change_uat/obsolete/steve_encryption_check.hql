SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.cli.print.header=true;

SELECT
old.segment_id
, old.segment_end_timestamp_utc
--, old.call_end_date_utc
, old.call_start_time_utc
, new.segment_id
, new.segment_end_timestamp_utc
--, new.call_end_date_utc
, new.call_end_time_utc
FROM
  prod.cs_call_care_data old
--  prod.cs_call_data old
-- ${hiveconf:old_table} old
INNER JOIN
--  test.cs_call_care_data_amo new
-- ${hiveconf:new_table} new
--  prod.cs_call_care_data new
test.cs_call_care_data_256 new
  on old.segment_id = new.segment_id
WHERE
  old.call_end_date_utc>='2019-01-23'
  AND new.call_end_date_utc>='2019-01-23'  
AND 
--old.segment_end_timestamp_utc<>new.segment_end_timestamp_utc
old.call_start_time_utc <> new.call_start_time_utc

limit 10
 ;

