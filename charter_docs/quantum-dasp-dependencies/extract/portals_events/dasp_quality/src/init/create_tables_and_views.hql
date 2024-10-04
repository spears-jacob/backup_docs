USE ${env:TMP_db};

CREATE TABLE IF NOt EXISTS ${env:ReprocessDateTable} (run_date string);
INSERT OVERWRITE TABLE ${env:ReprocessDateTable} VALUES('${env:RUN_DATE}');


USE ${env:ENVIRONMENT};

CREATE TABLE IF NOT EXISTS asp_quality_kpi (
  timeframe  STRING,
  application_name STRING,
  metric_name  STRING,
  metric_value DOUBLE,
  duration_bucket DOUBLE,
  page_load_time_bucket DOUBLE,
  login_failure_bucket DOUBLE)
PARTITIONED BY (denver_date STRING)
TBLPROPERTIES ('retention_policy'='Aggregate - Without PII (3 Years)')
;

DROP VIEW IF EXISTS asp_v_quality_kpi;
CREATE VIEW asp_v_quality_kpi AS
SELECT  denver_date,
        timeframe,
        application_name,
        MAX(IF(metric_name='portals_login_attempts_devices',metric_value, NULL)) devices_login,
        MAX(IF(metric_name='portals_login_failures_devices',metric_value, NULL)) devices_login_failures,
        MAX(IF(metric_name='portals_site_unique_auth_devices',metric_value, NULL)) portals_site_unique_auth_devices,
        MAX(IF(metric_name='portals_all_equipment_reset_flow_failures_hh',metric_value, NULL)) equipment_reset_failures,
        MAX(IF(metric_name='portals_all_equipment_reset_flow_successes_hh',metric_value, NULL)) equipment_reset_successes,
        MAX(IF(metric_name='portals_one_time_payment_failures_hh',metric_value, NULL)) hhs_billpay_stop_error,
        MAX(IF(metric_name='portals_one_time_payment_successes_hh',metric_value, NULL)) hhs_billpay_stop_success,
        MAX(IF(metric_name='portals_site_unique_hh',metric_value, NULL)) active_hhs
 FROM   asp_quality_kpi
 WHERE (denver_date > DATE_SUB(CURRENT_DATE, 90))
 GROUP BY denver_date,
          timeframe,
          application_name
 ;
