#!/bin/bash

hive -v -e "CREATE VIEW IF NOT EXISTS ${DASP_db}.asp_v_product_monthly_time AS SELECT * FROM ${DASP_db}.asp_product_monthly_time;"

hive -v -e "CREATE VIEW IF NOT EXISTS ${DASP_db}.asp_v_product_monthly_metrics AS SELECT * FROM ${DASP_db}.asp_product_monthly_metrics;"

hive -v -e "CREATE VIEW IF NOT EXISTS ${DASP_db}.asp_v_product_monthly_last_6months AS
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
  FROM  ${DASP_db}.asp_product_monthly_last_6months;"