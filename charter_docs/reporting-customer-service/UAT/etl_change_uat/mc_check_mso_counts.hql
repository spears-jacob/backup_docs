SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.cli.print.header=true;

--Assumption: counts of calls by mso in call_care_data should be reasonably
--- close to those in the old data
--MSO Counts (mc)

--This will print two tables; they should resemble each other pretty closely

SELECT
--agent_mso
account_agent_mso
, count(segment_id) as segment_count
, "prod.cs_call_care_data should match query below" as note
FROM
 ${hiveconf:old_table} old
WHERE
  old.call_end_date_utc >="2019-07-03"
 AND old.call_end_date_utc<"2019-07-11"
GROUP BY agent_mso
 ;


SELECT
--agent_mso
account_agent_mso
, count(segment_id) as segment_count
, "test.cs_call_care_data_amo should match query above" as note
FROM
  ${hiveconf:new_table} new
WHERE
  new.call_end_date_utc >="2019-07-03"
 AND new.call_end_date_utc<"2019-07-11"
GROUP BY agent_mso
 ;
