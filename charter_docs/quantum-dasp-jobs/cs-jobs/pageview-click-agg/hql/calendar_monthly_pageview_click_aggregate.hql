USE ${env:DASP_db};
set hive.exec.dynamic.partition.mode=nonstrict;
set orc.force.positional.evolution=true;
set hive.merge.tezfiles=true;
set hive.merge.mapredfiles=true;

INSERT OVERWRITE TABLE ${env:DASP_db}.cs_calendar_monthly_pageview_selectaction_aggregate PARTITION (calendar_month)
SELECT application_name
,current_page
,element_name
,year calendar_year
,COUNT(1) count_of_buttonclicks
,COUNT(DISTINCT visit_device_uuid) count_of_unique_visitors--count_of_unique_visitors
,COUNT(visit_device_uuid) count_of_visitors --count_of_visitors
,COUNT(DISTINCT unique_visit_id) count_of_distinct_visits
,concat(year,'-',lpad(month,2,'0')) calendar_month
FROM ${env:DASP_db}.cs_selectaction_aggregate sa
  INNER JOIN ${env:DASP_db}.cs_dates dates
    ON sa.partition_date_utc = dates.calendar_date
GROUP BY application_name
,current_page
,element_name
,concat(year,'-',lpad(month,2,'0'))
,year
;
