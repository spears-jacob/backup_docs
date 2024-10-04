SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;

set hive.cli.print.header=true; 

SELECT * 
FROM
prod.cs_visits_total_sitesection
WHERE
partition_date_utc='2019-03-21'
AND upper(application_name)='SPECNET'
AND upper(CURRENT_APP_SECTION)='VOM'
