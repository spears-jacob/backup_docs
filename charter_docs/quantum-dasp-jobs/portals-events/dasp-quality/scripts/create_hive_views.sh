hive -v -e "DROP VIEW IF EXISTS ${ENVIRONMENT}.asp_v_quality_kpi;"
hive -v -e "CREATE VIEW ${ENVIRONMENT}.asp_v_quality_kpi AS
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
 FROM   ${ENVIRONMENT}.asp_quality_kpi
 WHERE (denver_date > DATE_SUB(CURRENT_DATE, 90))
 GROUP BY denver_date,
          timeframe,
          application_name
 ;"

 hive -v -e "DROP VIEW IF EXISTS ${ENVIRONMENT}.asp_v_quality_kpi_components;"
 hive -v -e "CREATE VIEW ${ENVIRONMENT}.asp_v_quality_kpi_components AS
 SELECT  timeframe,
         grouping_id,
         application_name,
         application_version,
         metric_name,
         metric_value,
         0 as duration_bucket_filtered_ratio,
         0 as page_load_time_bucket_filtered_ratio,
         0 as login_failure_bucket_filtered_ratio,
         0 as duration_bucket,
         0 as page_load_time_bucket,
         0 as login_failure_bucket,
         0 as login_success_derived,
         0 as login_success,
         0 as otp_both_derived,
         0 as otp_both,
         0 as otp_success,
         0 as otp_failure_not,
         0 as autopay_all_derived,
         0 as autopay_all,
         0 as autopay_success,
         0 as autopay_failure_not,
         0 as number_uniq_visit,
         denver_date
  FROM   ${ENVIRONMENT}.asp_quality_kpi_core
  UNION ALL
  SELECT *
  FROM   ${ENVIRONMENT}.asp_quality_kpi_mos
  ;"

  hive -v -e "DROP VIEW IF EXISTS ${ENVIRONMENT}.asp_v_quality_kpi_components_label;"
  hive -v -e "CREATE VIEW ${ENVIRONMENT}.asp_v_quality_kpi_components_label AS
  SELECT  timeframe, grouping_id, application_name, application_version, denver_date,
          metric_name as metric_type,
          'duration_bucket' as metric_name,
          duration_bucket as metric_value,
          0.0 as application_entry_overall,
          0.0 as otp_overall,
          0.0 as autopay_all,
          duration_bucket,
          0.0 as page_load_time_bucket,
          0.0 as login_failure_bucket,
          0.0 as login_success,
          0.0 as otp_success,
          0.0 as otp_failure_not,
          0.0 as autopay_success,
          0.0 as autopay_failure_not
   FROM   ${ENVIRONMENT}.asp_quality_kpi_mos
   WHERE  metric_name in ('portals_application_entry_mos','portals_autopay_mos','portals_one_time_payment_mos')
   UNION ALL
  SELECT timeframe, grouping_id, application_name, application_version, denver_date,
         metric_name as metric_type,
         'page_load_time_bucket' as metric_name,
         page_load_time_bucket as metric_value,
         0.0 as application_entry_overall,
         0.0 as otp_overall,
         0.0 as autopay_all,
         0.0 as duration_bucket,
         page_load_time_bucket,
         0.0 as login_failure_bucket,
         0.0 as login_success,
         0.0 as otp_success,
         0.0 as otp_failure_not,
         0.0 as autopay_success,
         0.0 as autopay_failure_not
    FROM ${ENVIRONMENT}.asp_quality_kpi_mos
   where metric_name in ('portals_application_entry_mos','portals_autopay_mos','portals_one_time_payment_mos')
   UNION ALL
  SELECT timeframe, grouping_id, application_name, application_version, denver_date,
         metric_name as metric_type,
         'login_failure_bucket' as metric_name,
         login_failure_bucket as metric_value,
         0.0 as application_entry_overall,
         0.0 as otp_overall,
         0.0 as autopay_all,
         0.0 as duration_bucket,
         0.0 as page_load_time_bucket,
         login_failure_bucket,
         0.0 as login_success,
         0.0 as otp_success,
         0.0 as otp_failure_not,
         0.0 as autopay_success,
         0.0 as autopay_failure_not
    FROM ${ENVIRONMENT}.asp_quality_kpi_mos
   where metric_name in ('portals_application_entry_mos')
   UNION ALL
  SELECT timeframe, grouping_id, application_name, application_version, denver_date,
         metric_name as metric_type,
        'login_success' as metric_name,
         login_success as metric_value,
         0.0 as application_entry_overall,
         0.0 as otp_overall,
         0.0 as autopay_all,
         0.0 as duration_bucket,
         0.0 as page_load_time_bucket,
         0.0 as login_failure_bucket,
         login_success,
         0.0 as otp_success,
         0.0 as otp_failure_not,
         0.0 as autopay_success,
         0.0 as autopay_failure_not
    FROM ${ENVIRONMENT}.asp_quality_kpi_mos
   where metric_name in ('portals_application_entry_mos')
   UNION ALL
  SELECT timeframe, grouping_id, application_name, application_version, denver_date,
         metric_name as metric_type,
         'otp_success' as metric_name,
         otp_success as metric_value,
         0.0 as application_entry_overall,
         0.0 as otp_overall,
         0.0 as autopay_all,
         0.0 as duration_bucket,
         0.0 as page_load_time_bucket,
         0.0 as login_failure_bucket,
         0.0 as login_success,
         otp_success,
         0.0 as otp_failure_not,
         0.0 as autopay_success,
         0.0 as autopay_failure_not
    FROM ${ENVIRONMENT}.asp_quality_kpi_mos
   where metric_name in ('portals_one_time_payment_mos')
   UNION ALL
  SELECT timeframe, grouping_id, application_name, application_version, denver_date,
         metric_name as metric_type,
         'otp_failure_not' as metric_name,
         otp_failure_not as metric_value,
         0.0 as application_entry_overall,
         0.0 as otp_overall,
         0.0 as autopay_all,
         0.0 as duration_bucket,
         0.0 as page_load_time_bucket,
         0.0 as login_failure_bucket,
         0.0 as login_success,
         0.0 as otp_success,
         otp_failure_not,
         0.0 as autopay_success,
         0.0 as autopay_failure_not
   FROM  ${ENVIRONMENT}.asp_quality_kpi_mos
   where metric_name in ('portals_one_time_payment_mos')
   UNION ALL
  SELECT timeframe, grouping_id, application_name, application_version, denver_date,
         metric_name as metric_type,
         'autopay_success' as metric_name,
         autopay_success as metric_value,
         0.0 as application_entry_overall,
         0.0 as otp_overall,
         0.0 as autopay_all,
         0.0 as duration_bucket,
         0.0 as page_load_time_bucket,
         0.0 as login_failure_bucket,
         0.0 as login_success,
         0.0 as otp_success,
         0.0 as otp_failure_not,
         autopay_success,
         0.0 as autopay_failure_not
    FROM ${ENVIRONMENT}.asp_quality_kpi_mos
   where metric_name in ('portals_autopay_mos')
   UNION ALL
  SELECT timeframe, grouping_id, application_name, application_version, denver_date,
         metric_name as metric_type,
        'autopay_failure_not' as metric_name,
         autopay_failure_not as metric_value,
         0.0 as application_entry_overall,
         0.0 as otp_overall,
         0.0 as autopay_all,
         0.0 as duration_bucket,
         0.0 as page_load_time_bucket,
         0.0 as login_failure_bucket,
         0.0 as login_success,
         0.0 as otp_success,
         0.0 as otp_failure_not,
         0.0 as autopay_success,
         autopay_failure_not
    FROM ${ENVIRONMENT}.asp_quality_kpi_mos
   where metric_name in ('portals_autopay_mos')
   UNION ALL
  SELECT timeframe, grouping_id, application_name, application_version, denver_date,
         metric_name as metric_type,
        'application_entry_overall' as metric_name,
         metric_value,
         metric_value as application_entry_overall,
         0.0 as otp_overall,
         0.0 as autopay_all,
         0.0 as duration_bucket,
         0.0 as page_load_time_bucket,
         0.0 as login_failure_bucket,
         0.0 as login_success,
         0.0 as otp_success,
         0.0 as otp_failure_not,
         0.0 as autopay_success,
         0.0 as autopay_failure_not
    FROM ${ENVIRONMENT}.asp_quality_kpi_mos
   where metric_name='portals_application_entry_mos'
   UNION ALL
  SELECT timeframe, grouping_id, application_name, application_version, denver_date,
         metric_name as metric_type,
        'otp_overall' as metric_name,
         metric_value,
         0.0 as application_entry_overall,
         metric_value as otp_overall,
         0.0 as autopay_all,
         0.0 as duration_bucket,
         0.0 as page_load_time_bucket,
         0.0 as login_failure_bucket,
         0.0 as login_success,
         0.0 as otp_success,
         0.0 as otp_failure_not,
         0.0 as autopay_success,
         0.0 as autopay_failure_not
    FROM ${ENVIRONMENT}.asp_quality_kpi_mos
   where metric_name='portals_one_time_payment_mos'
   UNION ALL
  SELECT timeframe, grouping_id, application_name, application_version, denver_date,
         metric_name as metric_type,
        'autopay_overall' as metric_name,
         metric_value,
         0.0 as application_entry_overall,
         0.0 as otp_overall,
         metric_value as autopay_all,
         0.0 as duration_bucket,
         0.0 as page_load_time_bucket,
         0.0 as login_failure_bucket,
         0.0 as login_success,
         0.0 as otp_success,
         0.0 as otp_failure_not,
         0.0 as autopay_success,
         0.0 as autopay_failure_not
    FROM ${ENVIRONMENT}.asp_quality_kpi_mos
   where metric_name='portals_autopay_mos'
;"
