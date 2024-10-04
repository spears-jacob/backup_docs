INSERT OVERWRITE TABLE ${env:ENVIRONMENT}.cs_weekly_pageview_selectaction_aggregate PARTITION (week_starting)
SELECT application_name
,current_page
,element_name
,week_of_year
,calendar_year
,COUNT(1) count_of_buttonclicks
,COUNT(DISTINCT visit_device_uuid) count_of_unique_visitors--count_of_unique_visitors
,COUNT(visit_device_uuid) count_of_visitors --count_of_visitors
,COUNT(DISTINCT unique_visit_id) count_of_distinct_visits
,week_starting
FROM ${env:ENVIRONMENT}.cs_selectaction_aggregate sa
  INNER JOIN
    (
      SELECT calendar_date, week_of_year, year calendar_year, min(calendar_date) over (partition by year, week_of_year) week_starting
      FROM ${env:LKP_db}.cs_dates
    ) dates
      ON sa.partition_date_utc = dates.calendar_date
GROUP BY application_name
,current_page
,element_name
,week_of_year
,calendar_year
,week_starting
;
