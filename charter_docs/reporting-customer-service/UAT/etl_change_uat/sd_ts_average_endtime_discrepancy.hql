--make a query to find some sample segments and how farr off the end-times are

--actually order by how far off they are.  Or get an average and range or something

SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.cli.print.header=true;

SELECT
--old.segment_id
avg(abs(old.segment_end_timestamp_utc - new.segment_end_timestamp_utc))
FROM
  prod.cs_call_care_data old
INNER JOIN
  test.cs_call_care_data_amo new
  on old.segment_id = new.segment_id
WHERE
  old.call_end_date_utc>='2019-01-23'
  AND new.call_end_date_utc>='2019-01-23'
AND
old.segment_end_timestamp_utc<>new.segment_end_timestamp_utc
--ORDER BY time_diff desc
limit 10;
