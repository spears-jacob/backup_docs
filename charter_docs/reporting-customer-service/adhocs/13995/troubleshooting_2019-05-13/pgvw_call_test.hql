SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;

set hive.cli.print.header=true; 

SELECT 
count(1) count_of_sectionviews_with_calls
, count(distinct visit_unique_id) count_of_distinct_visits_with_calls
FROM 
prod.cs_pageviews_and_calls
WHERE
  partition_date_utc ='2019-03-21'
AND upper(APPLICATION_NAME)='SPECNET'
AND upper(CURRENT_APP_SECTION)='VOM'
