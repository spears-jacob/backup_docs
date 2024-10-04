SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.cli.print.header=true;

--Assumption: For each segment in calls_with_prior_visits, the dispsition data should be the same as in call_care_data (as_sdd)

--All Segments Included (as)
------ Segment Disposition Data (as_sdd)

--Should return a single line, whose count is 0

SELECT
count(old.segment_id) as count
, "should be 0" as note
FROM
  ${hiveconf:old_call_table} old
--  prod.cs_call_care_data old
INNER JOIN
  ${hiveconf:new_call_table} new
--  test.cs_call_care_data_amo new
  on old.segment_id = new.segment_id
WHERE
  old.call_end_date_utc>='2019-01-23'
  AND new.call_end_date_utc>='2019-01-23'
AND
(trim(old.cause_description)<>trim(new.cause_description)
  OR trim(old.issue_description)<>trim(new.issue_description)
  OR trim(old.resolution_description)<>trim(new.resolution_description))

limit 10
 ;
