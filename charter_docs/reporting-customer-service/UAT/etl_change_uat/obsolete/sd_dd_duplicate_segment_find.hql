SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;

set hive.cli.print.header=true; 

SELECT
segment_id
,customer_type
,count(segment_id) as count
FROM 
test.cs_call_care_data_amo_june
--prod.cs_call_care_data
GROUP BY 
segment_id, customer_type
HAVING count>1
limit 50
;
