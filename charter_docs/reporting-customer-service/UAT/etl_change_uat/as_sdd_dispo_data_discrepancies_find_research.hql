SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.cli.print.header=true;

--query for figuring out what's going on if we fail as_sdd check.  It will pull disposition data for segments
---where we have a discrepancy, so you can see what's going on.

SELECT
old.segment_id
, old.cause_description
, new.cause_description
, old.issue_description
, new.issue_description
, old.resolution_description
, new.resolution_description
FROM
  prod.cs_call_care_data old
INNER JOIN
  test.cs_call_care_data_amo new
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
