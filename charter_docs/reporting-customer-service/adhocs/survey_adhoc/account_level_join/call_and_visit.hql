SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;

set hive.cli.print.header=true; 
SELECT DISTINCT
account_number
, customer_type
, prod.epoch_converter(visitStart*1000,'America/Denver') as visit_date
FROM prod.cs_calls_with_prior_visit
WHERE call_end_date_utc BETWEEN '2019-04-01' AND '2019-04-30'
AND visit_type in ('smb','specnet')
limit 10
;
