SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.cli.print.header=true;

SELECT * 
FROM prod.cs_v_disposition_research_with_visit_data 
WHERE call_date>='2019-05-26'
AND product='Escalations'
;

