USE ${env:TMP_db};

CREATE TABLE IF NOt EXISTS ${env:ReprocessDateTable} (run_date string);
INSERT OVERWRITE TABLE ${env:ReprocessDateTable} VALUES('${env:RUN_DATE}');

USE ${env:ENVIRONMENT};

create table IF NOT EXISTS asp_product_monthly_time (
  application_name string,
  num_total int,
  num_less_than2 int,
  num_between2_and4 int,
  num_between4_and6 iNT,
  num_larger_than6 INT,
  pct_less_than2 double,
  pct_between2_and4 double,
  pct_between4_and6 DOUBLE,
  pct_larger_than6 DOUBLE,
  run_date DATE
)
PARTITIONED BY (label_date_denver string);

create table IF NOT EXISTS asp_product_monthly_metrics (
  application_name string,
  metric_name string,
  metric_value decimal(15,3),
  run_date Date
)
PARTITIONED BY (label_date_denver string);

create table IF NOT EXISTS asp_product_monthly_metrics_archive (
  metric_name string,
  application_name string,
  metric_order string,
  application_order string,
  report_metric_name string,
  metric_value decimal(15,3),
  run_date date,
  archive_date date
)
PARTITIONED BY (label_date_denver string);

create table IF NOT EXISTS asp_product_monthly_time_archive (
  metric_name string,
  application_name string,
  application_order string,
  pct_less_than2 double,
  pct_between2_and4 double,
  pct_between4_and6 DOUBLE,
  pct_larger_than6 DOUBLE,
  run_date date,
  archive_date date
)
PARTITIONED BY (label_date_denver string);

CREATE TABLE IF NOT EXISTS asp_product_monthly_last_6months (
  metric_name string,
  application_name string,
  metric_order string,
  application_order string,
  report_metric_name string,
  3m_filter string,
  label_date_denver string,
  metric_value decimal(15,3),
  run_date date
);

CREATE TABLE IF NOT EXISTS asp_product_monthly_last_month (
  metric_name string,
  application_name string,
  application_order string,
  pct_less_than2 double,
  pct_between2_and4 double,
  pct_between4_and6 double,
  pct_larger_than6 double,
  label_date_denver string,
  run_date date
);

create VIEW IF NOT EXISTS asp_v_product_monthly_time AS
SELECT *
    FROM asp_product_monthly_time;

create VIEW IF NOT EXISTS asp_v_product_monthly_metrics AS
SELECT *
    FROM asp_product_monthly_metrics;

create VIEW IF NOT EXISTS asp_v_product_monthly_last_6months AS
SELECT
       metric_name,
       application_name,
       CASE WHEN metric_order = '1_unique_households' THEN '1.1.1_unique_households'
            WHEN metric_order = '2_unique_visitors' THEN '1.1.2_unique_visitors'
            WHEN metric_order = '3_support_page_views' THEN '1.1.3_support_page_views'
            WHEN metric_order = '4_one_time_payments' THEN '1.2.1_one_time_payments'
            WHEN metric_order = '5_autopay_setups' THEN '1.2.2_autopay_setups'
            WHEN metric_order = '6_equipment_reset' THEN '1.2.3_equipment_reset'
            WHEN metric_order = '7_scp_device' THEN '1.2.4_scp_device'
            WHEN metric_order = '8_identity' THEN '1.2.5_identity'
            WHEN metric_order = '9_appointments' THEN '1.2.6_appointments'
            WHEN metric_order = '91_digital_first_contact_rate' THEN '3.1.1_digital_first_contact_rate'
            WHEN metric_order = '92_call_in_rate' THEN '3.2.1_call_in_rate'
            WHEN metric_order = '93_error_message_rate' THEN '5.1.1_error_message_rate'
            ELSE metric_order
       END AS metric_order,
       application_order,
       report_metric_name,
       3m_filter,
       label_date_denver,
       metric_value,
       run_date
  FROM  asp_product_monthly_last_6months;
