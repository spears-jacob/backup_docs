SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.cli.print.header=true;

--Assumption: Segments should only match one account key

-- Same Data (sd)
------ Account Key (sd_ak)

--this should return 0 results, because all segments should match only one account key

SELECT
c.segment_id
,COUNT(distinct account_key) as count_should_be_empty
FROM
 ${hiveconf:new_call_table} c
GROUP BY c.segment_id
HAVING count_should_be_empty>1
