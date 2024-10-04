--make a query to find some sample segments and how far off the end-times are

--actually order by how far off they are.  Or get an average and range or something

-- set old_call_table=stg_dasp.cs_call_care_data_wed;
-- set new_call_table=stg.atom_cs_call_care_data_3;
-- set test_date=2021-04-28;
-- set start_date=2021-05-01;
-- set end_date=2021-05-08;

SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.cli.print.header=true;

SELECT "3_sd_ts_average_endtime_discrepancy";

SELECT
    old.segment_id, old.segment_end_timestamp_utc, new.segment_end_timestamp_utc, old.call_end_date_utc, abs(old.segment_end_timestamp_utc-new.segment_end_timestamp_utc) as time_diff
--      avg(abs(old.segment_end_timestamp_utc - new.segment_end_timestamp_utc))
 FROM
      `${hiveconf:old_call_table}` old --old call table
INNER JOIN
      `${hiveconf:new_call_table}` new --new call table
   on old.segment_id = new.segment_id
WHERE old.call_end_date_utc>='${hiveconf:start_date}'
  AND new.call_end_date_utc>='${hiveconf:start_date}'
  AND new.call_end_date_utc<='${hiveconf:end_date}'
  AND old.segment_end_timestamp_utc<>new.segment_end_timestamp_utc
ORDER BY time_diff desc
limit 10;
