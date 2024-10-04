-- set old_call_table=387455165365/prod.atom_cs_call_care_data_3;
--  SET new_call_table=stg.atom_cs_call_care_data_3_red_400;
-- set test_date=2021-01-10;
-- set TEST_DATE_END=2021-01-17;

SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.cli.print.header=true;

--Assumption: counts of calls by mso in call_care_data should be reasonably
--- close to those in the old data
--MSO Counts (mc)

--This will print two tables; they should resemble each other pretty closely

SELECT
      agent_mso
      , count(segment_id) as segment_count
      , "${hiveconf:old_call_table} should match query below" as note
 FROM `${hiveconf:old_call_table}` old
WHERE
      old.call_end_date_utc ='${hiveconf:test_date}'
GROUP BY agent_mso

 UNION

SELECT
  agent_mso
  , count(segment_id) as segment_count
  , "${hiveconf:new_call_table} should match query above" as note
FROM
 ${hiveconf:new_call_table} new
WHERE
  new.call_end_date_utc ='${hiveconf:test_date}'
GROUP BY agent_mso
ORDER BY agent_mso
;
