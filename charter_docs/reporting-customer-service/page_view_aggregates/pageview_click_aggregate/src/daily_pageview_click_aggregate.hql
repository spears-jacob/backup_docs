INSERT OVERWRITE TABLE ${env:ENVIRONMENT}.cs_daily_pageview_selectaction_aggregate PARTITION (partition_date_utc)
SELECT application_name
,current_page
,element_name
,day_of_week_s
,COUNT(1) count_of_buttonclicks
,COUNT(DISTINCT visit_device_uuid) count_of_unique_visitors--count_of_unique_visitors
,COUNT(visit_device_uuid) count_of_visitors --count_of_visitors
,COUNT(DISTINCT unique_visit_id) count_of_distinct_visits
,PARTITION_DATE_UTC
FROM ${env:ENVIRONMENT}.cs_selectaction_aggregate sa
  INNER JOIN ${env:LKP_db}.cs_dates dates
    ON sa.partition_date_utc = dates.calendar_date
GROUP BY application_name
,current_page
,element_name
,day_of_week_s
,PARTITION_DATE_UTC
;
