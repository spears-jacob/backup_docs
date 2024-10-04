SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;

set hive.cli.print.header=true; 

SELECT *
FROM prod.cs_sitesection_call_in_rate
WHERE
partition_date_utc='2019-03-21'
AND upper(application_name)='SPECNET'
AND upper(current_app_section)='VOM'
