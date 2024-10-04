USE ${env:TMP_db};

SELECT '***** Creating ReprocessDateTable ******';

CREATE TABLE IF NOT EXISTS ${env:ReprocessDateTable} (
  run_date string)
TBLPROPERTIES ('retention_policy'='Short term (90 days)')
;

INSERT OVERWRITE TABLE ${env:ReprocessDateTable}
  VALUES('${env:RUN_DATE}');

SELECT '***** Creating persistent Tables ******';

USE ${env:ENVIRONMENT};

CREATE TABLE IF NOT EXISTS asp_support_quantum_events (
  application_name string,
  visit_id string,
  device_id string,
  page_name string,
  visit_unique_id string,
  account_number string,
  received__timestamp bigint,
  message__name STRING,
  operation__operation_type STRING,
  search_id STRING,
  search_text STRING,
  seq_num INT,
  element_url STRING,
  helpful_yes INT,
  helpful_no INT,
  wasnt_what_i_searched INT,
  incorrect_info INT,
  confusing INT
)
PARTITIONED BY (denver_date STRING)
TBLPROPERTIES ('retention_policy'='Aggregate - With PII (3 Years)')
;

CREATE TABLE IF NOT EXISTS asp_support_content_agg (
  application_name string,
  page_name string,
  pageviews_with_calls INT,
  visitors INT,
  authenticated_visitors INT,
  households INT,
  visits INT,
  pageviews INT,
  helpful_yes INT,
  helpful_no INT,
  wasnt_what_i_searched INT,
  incorrect_info INT,
  confusing INT,
  distinct_visits_with_calls INT,
  total_distinct_visits INT,
  call_in_rate decimal(12,4),
  search_text string,
  search_instances INT
)
PARTITIONED BY (denver_date string)
TBLPROPERTIES ('retention_policy'='Aggregate - Without PII (3 Years)')
;

SELECT '***** Creating Views ******';

CREATE VIEW IF NOT EXISTS asp_v_support_content_agg AS
  SELECT *
    FROM prod.asp_support_content_agg
   WHERE denver_date                   >= DATE_SUB(CURRENT_DATE,90);

CREATE VIEW If NOT EXISTS cs_v_call_care_data as
  SELECT *
    FROM prod.cs_call_care_data;

SELECT '***** View & Table creation complete ******';
