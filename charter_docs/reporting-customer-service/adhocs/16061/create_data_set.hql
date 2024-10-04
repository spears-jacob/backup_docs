SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;

set hive.cli.print.header=true; 

SELECT 
customer_category
,call_flag
,count(distinct visit_id)
FROM dev_tmp.cs_16061_ready_to_agg
GROUP BY customer_category, call_flag
