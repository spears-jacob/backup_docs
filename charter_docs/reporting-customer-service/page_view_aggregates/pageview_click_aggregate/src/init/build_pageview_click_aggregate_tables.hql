--DROP TABLE IF EXISTS ${env:ENVIRONMENT}.cs_selectaction_aggregate PURGE;
CREATE TABLE IF NOT EXISTS ${env:ENVIRONMENT}.cs_selectaction_aggregate
(
  application_name string
  ,current_page string
  ,element_name string
  ,visit_device_uuid string
  ,unique_visit_id string
)
PARTITIONED BY
(
  partition_date_utc string
)
TBLPROPERTIES (
  'retention_policy'='Event Level - With PII (1 Year)'
)
;


--DROP TABLE IF EXISTS dev.cs_daily_pageview_selectaction_aggregate;
CREATE TABLE IF NOT EXISTS ${env:ENVIRONMENT}.cs_daily_pageview_selectaction_aggregate
(
  application_name string
  ,current_page string
  ,element_name string
  ,day_of_week string
  ,count_of_buttonclicks int
  ,count_of_unique_visitors int
  ,count_of_visitors int
  ,count_of_distinct_visits int
)
PARTITIONED BY
(
    partition_date_utc string
)
TBLPROPERTIES (
  'retention_policy'='Aggregate - Without PII (3 Years)'
)
;


--DROP TABLE IF EXISTS ${env:ENVIRONMENT}.cs_weekly_pageview_selectaction_aggregate;
CREATE TABLE IF NOT EXISTS ${env:ENVIRONMENT}.cs_weekly_pageview_selectaction_aggregate
(
  application_name string
  ,current_page string
  ,element_name string
  ,week_of_year int
  ,calendar_year int
  ,count_of_buttonclicks int
  ,count_of_unique_visitors int
  ,count_of_visitors int
  ,count_of_distinct_visits int
)
PARTITIONED BY
(
    week_starting string
)
TBLPROPERTIES (
  'retention_policy'='Aggregate - Without PII (3 Years)'
)
;


--DROP TABLE IF EXISTS dev.cs_monthly_pageview_selectaction_aggregate;
CREATE TABLE IF NOT EXISTS ${env:ENVIRONMENT}.cs_monthly_pageview_selectaction_aggregate
(
  application_name string
  ,current_page string
  ,element_name string
  ,calendar_year int
  ,count_of_buttonclicks int
  ,count_of_unique_visitors int
  ,count_of_visitors int
  ,count_of_distinct_visits int
)
PARTITIONED BY
(
    fiscal_month string
)
TBLPROPERTIES (
  'retention_policy'='Aggregate - Without PII (3 Years)'
)
;


--DROP TABLE IF EXISTS dev.cs_calendar_monthly_pageview_selectaction_aggregate;
CREATE TABLE IF NOT EXISTS ${env:ENVIRONMENT}.cs_calendar_monthly_pageview_selectaction_aggregate
(
  application_name string
  ,current_page string
  ,element_name string
  ,calendar_year int
  ,count_of_buttonclicks int
  ,count_of_unique_visitors int
  ,count_of_visitors int
  ,count_of_distinct_visits int
)
PARTITIONED BY
(
    calendar_month string
)
TBLPROPERTIES (
  'retention_policy'='Aggregate - Without PII (3 Years)'
)
;
