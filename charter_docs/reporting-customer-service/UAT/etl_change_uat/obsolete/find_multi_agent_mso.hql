SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.cli.print.header=true;

-- query to check whether any calls have multiple agent_msos

SELECT  
call_inbound_key
, count(distinct agent_mso) as count
FROM
test.cs_call_care_data_1075_2_amo
WHERE call_end_date_utc >='2018-01-01'
GROUP BY call_inbound_key
having count>1
limit 10
;
