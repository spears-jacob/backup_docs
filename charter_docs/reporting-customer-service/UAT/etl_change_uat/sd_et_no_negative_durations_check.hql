SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.cli.print.header=true;

--Assumption: Segments should never have a negative duration

-- Same Data (sd)
------ End Times (sd_et)

--this should return 0 results, because there should be no segments with a
--- negative duration

SELECT
c.segment_id
,segment_duration_seconds
,"this shouldn't happen" as should_be_empty
FROM
 ${hiveconf:new_call_table} c
WHERE segment_duration_seconds < 0
