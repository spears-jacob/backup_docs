SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.cli.print.header=true; 

SELECT 
old.segment_id
,old.call_end_date_utc
,new.call_end_date_utc
, if(old.call_end_date_utc=new.call_end_date_utc,1,0) as flag
FROM 
prod.cs_call_data old
INNER JOIN
test.cs_call_care_data_amo new
on old.segment_id = new.segment_id
WHERE 
--old.resolution_description is NOT NULL
--  AND old.cause_description IS NOT NULL
--  AND old.issue_description IS NOT NULL
--  AND 
old.call_end_date_utc>='2019-01-23'
 AND new.call_end_date_utc>='2019-01-23'
 AND old.call_end_date_utc<>new.call_end_date_utc
limit 10;
