SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.cli.print.header=true;

--query for figuring out what's going on if we fail as_sdd check.  It will pull disposition data for segments
---where we have a discrepancy, so you can see what's going on.
set old_call_table=${DASP_db}.atom_cs_call_care_data_3_prod_copy;
set new_call_table=387455165365/prod.atom_cs_call_care_data_3;
set TEST_DATE=2021-03-10;

SELECT
      old.segment_id
      , old.cause_description
      , new.cause_description
      , old.issue_description
      , new.issue_description
      , old.resolution_description
      , new.resolution_description
 FROM
      `${hiveconf:old_call_table}` old --old call table
INNER JOIN
      `${hiveconf:new_call_table}` new --new call table
   on old.segment_id = new.segment_id
WHERE
      old.call_end_date_utc>='${hiveconf:TEST_DATE}'
  AND new.call_end_date_utc>='${hiveconf:TEST_DATE}'
  AND
      (trim(old.cause_description)<>trim(new.cause_description)
        OR trim(old.issue_description)<>trim(new.issue_description)
        OR trim(old.resolution_description)<>trim(new.resolution_description))
limit 10
;
