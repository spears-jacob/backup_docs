USE ${env:DASP_db};
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions.pernode=800;
SET hive.exec.max.dynamic.partitions=8000;
SET hive.merge.size.per.task=1024000000;
SET hive.merge.smallfiles.avgsize=1024000000;
SET hive.merge.tezfiles=true;
SET hive.vectorized.execution.enabled=false;
SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;
SET orc.force.positional.evolution=true;

INSERT OVERWRITE TABLE ${env:DASP_db}.cs_daily_pageview_selectaction_aggregate PARTITION (partition_date_utc)
SELECT application_name
,current_page
,element_name
,day_of_week_s
,COUNT(1) count_of_buttonclicks
,COUNT(DISTINCT visit_device_uuid) count_of_unique_visitors--count_of_unique_visitors
,COUNT(visit_device_uuid) count_of_visitors --count_of_visitors
,COUNT(DISTINCT unique_visit_id) count_of_distinct_visits
,PARTITION_DATE_UTC
FROM ${env:DASP_db}.cs_selectaction_aggregate sa
  INNER JOIN ${env:DASP_db}.cs_dates dates
    ON sa.partition_date_utc = dates.calendar_date
GROUP BY application_name
,current_page
,element_name
,day_of_week_s
,PARTITION_DATE_UTC
;
