#!/bin/bash

hive -v -e "CREATE OR REPLACE VIEW ${DASP_db}.asp_v_idm_api_agg AS
SELECT grouping_id,
     hour_denver,
     minute_group,
     application_name,
     mso,
     cust_type,
     api_category,
     api_name,
     stream_subtype,
     current_page_name,
     metric_name,
     metric_value,
     week_end,
     month_start,
     technology_type,
     denver_date
  FROM ${DASP_db}.asp_api_agg
 WHERE denver_date              >= DATE_SUB(CURRENT_DATE,30)
   AND application_name         = 'IDManagement'
;"


hive -v -e "CREATE OR REPLACE VIEW ${DASP_db}.asp_v_msa_api_agg AS
SELECT
     grouping_id,
     hour_denver,
     minute_group,
     application_name,
     mso,
     cust_type,
     api_category,
     api_name,
     stream_subtype,
     current_page_name,
     metric_name,
     metric_value,
     week_end,
     month_start,
     technology_type,
     denver_date
  FROM ${DASP_db}.asp_api_agg
 WHERE denver_date              >= DATE_SUB(CURRENT_DATE,30)
   AND application_name         = 'MySpectrum'
;"

hive -v -e "CREATE OR REPLACE VIEW ${DASP_db}.asp_v_smb_api_agg AS
SELECT
     grouping_id,
     hour_denver,
     minute_group,
     application_name,
     mso,
     cust_type,
     api_category,
     api_name,
     stream_subtype,
     current_page_name,
     metric_name,
     metric_value,
     week_end,
     month_start,
     technology_type,
     denver_date
  FROM ${DASP_db}.asp_api_agg
 WHERE denver_date              >= DATE_SUB(CURRENT_DATE,30)
   AND application_name         = 'SMB'
;"

hive -v -e "CREATE OR REPLACE VIEW ${DASP_db}.asp_v_spec_api_agg AS
    SELECT
     grouping_id,
     hour_denver,
     minute_group,
     'SpecNet'                  AS application_name,
     mso,
     cust_type,
     api_category,
     api_name,
     stream_subtype,
     current_page_name,
     metric_name,
     metric_value,
     week_end,
     month_start,
     technology_type,
     denver_date
  FROM ${DASP_db}.asp_api_agg
 WHERE denver_date              >= DATE_SUB(CURRENT_DATE,30)
   AND application_name         IN ('specnet', 'SpecNet')
;"