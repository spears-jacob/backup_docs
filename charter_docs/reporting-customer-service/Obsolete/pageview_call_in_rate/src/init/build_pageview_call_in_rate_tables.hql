--DROP TABLE IF EXISTS ${env:ENVIRONMENT}.cs_venona_events;
CREATE TABLE IF NOT EXISTS ${env:ENVIRONMENT}.cs_venona_events
(
  visit_unique_id string
  ,account_number string
  ,company string
  ,visit_id string
  ,current_page_received_timestamp bigint
  ,current_page_datetime string
  ,current_page_name string
  ,current_app_section string
  ,current_modal_name string
  ,visit_start_timestamp bigint
  ,visit_start_datetime string
  ,message_name string
)
PARTITIONED BY
(
  source_system string
  ,application_name string
  ,partition_date_utc date
);

--DROP TABLE IF EXISTS ${env:ENVIRONMENT}.cs_chr_venona_events;
CREATE TABLE IF NOT EXISTS ${env:ENVIRONMENT}.cs_chr_venona_events
(
  visit_unique_id string
  ,account_number string
  ,company string
  ,visit_id string
  ,current_page_received_timestamp bigint
  ,current_page_datetime string
  ,current_page_name string
  ,current_app_section string
  ,current_modal_name string
  ,visit_start_timestamp bigint
  ,visit_start_datetime string
  ,message_name string
  ,source_system string
  ,application_name string
)
PARTITIONED BY
(
  partition_date_utc date
);

--drop table if exists ${env:ENVIRONMENT}.cs_pageview_call_in_rate;
CREATE TABLE IF NOT EXISTS ${env:ENVIRONMENT}.cs_pageview_call_in_rate
(
  application_name string
  ,current_page_name string
  ,current_app_section string
  ,count_of_pageviews_with_calls int
  ,total_pageviews int
  ,count_of_distinct_visits_with_calls int
  ,total_distinct_visits int
  ,call_in_rate decimal(12,4)
  ,mso string

)
PARTITIONED BY
(
  source_system string
  ,partition_date_utc string
)
;