--DROP TABLE IF EXISTS ${env:ENVIRONMENT}.cs_quantum_cid_pageviews PURGE;
CREATE TABLE IF NOT EXISTS ${env:ENVIRONMENT}.cs_quantum_cid_pageviews
(
  application_name string
  ,application_api_host string
  ,URL_new string
  ,CID string
  ,CMP string
  ,message_name string
  ,visit_device_uuid string
  ,unique_visit_id string
)
PARTITIONED BY
(
  partition_date_utc date
)
TBLPROPERTIES (
  'retention_policy'='Event Level - With PII (1 Year)'
)
;


--DROP TABLE IF EXISTS ${env:ENVIRONMENT}.cs_daily_quantum_pageview_cid_aggregate PURGE;
CREATE TABLE IF NOT EXISTS ${env:ENVIRONMENT}.cs_daily_quantum_pageview_cid_aggregate
(
    application_name STRING
    ,application_api_host string
    ,URL string
    ,CID string
    ,message_name STRING
    ,count_of_events int
    ,count_of_unique_users int
    ,count_of_users int
    ,count_of_visits int
)
PARTITIONED BY
(
    partition_date_utc date
)
TBLPROPERTIES (
  'retention_policy'='Aggregate - Without PII (3 Years)'
)
;


--DROP TABLE IF EXISTS ${env:ENVIRONMENT}.cs_weekly_quantum_pageview_cid_aggregate PURGE;
CREATE TABLE IF NOT EXISTS ${env:ENVIRONMENT}.cs_weekly_quantum_pageview_cid_aggregate
(
    application_name STRING
    ,application_api_host string
    ,URL string
    ,CID string
    ,message_name STRING
    ,count_of_pageviews int
    ,count_of_unique_users int
    ,count_of_users int
    ,count_of_visits int
    ,week_of_year string
    ,calendar_year string
)
PARTITIONED BY
(
    week_starting string
)
TBLPROPERTIES (
  'retention_policy'='Aggregate - Without PII (3 Years)'
)
;


--DROP TABLE IF EXISTS ${env:ENVIRONMENT}.cs_monthly_quantum_pageview_cid_aggregate PURGE;
CREATE TABLE IF NOT EXISTS ${env:ENVIRONMENT}.cs_monthly_quantum_pageview_cid_aggregate
(
    application_name STRING
    ,application_api_host string
    ,URL string
    ,CID string
    ,message_name STRING
    ,count_of_pageviews int
    ,count_of_unique_users int
    ,count_of_users int
    ,count_of_visits int
)
PARTITIONED BY
(
    fiscal_month string
)
TBLPROPERTIES (
  'retention_policy'='Aggregate - Without PII (3 Years)'
)
;


--DROP TABLE IF EXISTS ${env:ENVIRONMENT}.cs_calendar_monthly_quantum_pageview_cid_aggregate PURGE;
CREATE TABLE IF NOT EXISTS ${env:ENVIRONMENT}.cs_calendar_monthly_quantum_pageview_cid_aggregate
(
    application_name STRING
    ,application_api_host string
    ,URL string
    ,CID string
    ,message_name STRING
    ,count_of_pageviews int
    ,count_of_unique_users int
    ,count_of_users int
    ,count_of_visits int
)
PARTITIONED BY
(
    calendar_month string
)
TBLPROPERTIES (
  'retention_policy'='Aggregate - Without PII (3 Years)'
)
;

--DROP TABLE IF EXISTS ${env:ENVIRONMENT}.cs_daily_quantum_pageview_cmp_aggregate PURGE;
CREATE TABLE IF NOT EXISTS ${env:ENVIRONMENT}.cs_daily_quantum_pageview_cmp_aggregate
(
    application_name STRING
    ,application_api_host string
    ,URL string
    ,CMP string
    ,message_name STRING
    ,count_of_events int
    ,count_of_unique_users int
    ,count_of_users int
    ,count_of_visits int
)
PARTITIONED BY
(
    partition_date_utc date
)
TBLPROPERTIES (
  'retention_policy'='Aggregate - Without PII (3 Years)'
)
;

--DROP TABLE IF EXISTS ${env:ENVIRONMENT}.cs_weekly_quantum_pageview_cmp_aggregate PURGE; */
CREATE TABLE IF NOT EXISTS ${env:ENVIRONMENT}.cs_weekly_quantum_pageview_cmp_aggregate
(
    application_name STRING
    ,application_api_host string
    ,URL string
    ,CMP string
    ,message_name STRING
    ,count_of_pageviews int
    ,count_of_unique_users int
    ,count_of_users int
    ,count_of_visits int
    ,week_of_year string
    ,calendar_year string
)
PARTITIONED BY
(
    week_starting string
)
TBLPROPERTIES (
  'retention_policy'='Aggregate - Without PII (3 Years)'
)
;

--DROP TABLE IF EXISTS ${env:ENVIRONMENT}.cs_monthly_quantum_pageview_cmp_aggregate PURGE;
CREATE TABLE IF NOT EXISTS ${env:ENVIRONMENT}.cs_monthly_quantum_pageview_cmp_aggregate
(
    application_name STRING
    ,application_api_host string
    ,URL string
    ,CMP string
    ,message_name STRING
    ,count_of_pageviews int
    ,count_of_unique_users int
    ,count_of_users int
    ,count_of_visits int
)
PARTITIONED BY
(
    fiscal_month string
)
TBLPROPERTIES (
  'retention_policy'='Aggregate - Without PII (3 Years)'
)
;

--DROP TABLE IF EXISTS ${env:ENVIRONMENT}.cs_calendar_monthly_quantum_pageview_cmp_aggregate PURGE;
CREATE TABLE IF NOT EXISTS ${env:ENVIRONMENT}.cs_calendar_monthly_quantum_pageview_cmp_aggregate
(
    application_name STRING
    ,application_api_host string
    ,URL string
    ,CMP string
    ,message_name STRING
    ,count_of_pageviews int
    ,count_of_unique_users int
    ,count_of_users int
    ,count_of_visits int
)
PARTITIONED BY
(
    calendar_month string
)
TBLPROPERTIES (
  'retention_policy'='Aggregate - Without PII (3 Years)'
)
;
