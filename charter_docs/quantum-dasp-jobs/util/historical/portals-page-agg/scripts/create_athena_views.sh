#!/bin/bash

# this has to be referenced in order to run drop_hive_view_if_exists and execute_script_in_athena
. ./scripts/util-${SCRIPT_VERSION}.sh

# TODO Need to remove after first job's execution in specific environment (dev, prod, stg)
drop_hive_view_if_exists "${DASP_db}.asp_vat_page_set_counts_agg;"
drop_hive_view_if_exists "${DASP_db}.asp_vat_idm_page_set_pathing_agg;"
drop_hive_view_if_exists "${DASP_db}.asp_vat_msa_page_set_pathing_agg;"
drop_hive_view_if_exists "${DASP_db}.asp_vat_smb_page_set_pathing_agg;"
drop_hive_view_if_exists "${DASP_db}.asp_vat_spec_page_set_pathing_agg;"


execute_script_in_athena "CREATE OR REPLACE VIEW ${DASP_db}.asp_vat_page_set_counts_agg AS
SELECT
     denver_date,
     unit_type,
     application_name,
     current_page_name,
     current_article_name,
     standardized_name,
     modal_name,
     modal_view_count,
     page_view_count,
     select_action_count,
     spinner_success_count,
     spinner_failure_count,
     toggle_flips_count,
     grouping_id,
     CAST(denver_date as date)   AS denver_date_dt
  FROM ${DASP_db}.asp_page_set_counts_agg
 WHERE (CAST(denver_date as date) >= date_add('DAY', -90, current_date)
        AND CAST(denver_date as date) <= date_add('DAY', -1, current_date))
;"

execute_script_in_athena "CREATE OR REPLACE VIEW ${DASP_db}.asp_vat_idm_page_set_pathing_agg AS
SELECT
     current_page_name,
     previous_page_name,
     mso,
     application_name,
     select_action_types_value,
     grouping_id,
     metric_name,
     metric_value,
     denver_date,
     CAST(denver_date as date)   AS denver_date_dt
  FROM ${DASP_db}.asp_page_set_pathing_agg
 WHERE (CAST(denver_date as date) >= date_add('DAY', -90, current_date)
        AND CAST(denver_date as date) <= date_add('DAY', -1, current_date))
   and application_name         =  'IDManagement'
;"

execute_script_in_athena "CREATE OR REPLACE VIEW ${DASP_db}.asp_vat_msa_page_set_pathing_agg AS
SELECT
     current_page_name,
     previous_page_name,
     mso,
     application_name,
     select_action_types_value,
     grouping_id,
     metric_name,
     metric_value,
     denver_date,
     CAST(denver_date as date)   AS denver_date_dt
  FROM ${DASP_db}.asp_page_set_pathing_agg
 WHERE (CAST(denver_date as date) >= date_add('DAY', -90, current_date)
        AND CAST(denver_date as date) <= date_add('DAY', -1, current_date))
   and application_name         = 'MySpectrum'
;"

execute_script_in_athena "CREATE OR REPLACE VIEW ${DASP_db}.asp_vat_smb_page_set_pathing_agg AS
SELECT
     current_page_name,
     previous_page_name,
     mso,
     application_name,
     select_action_types_value,
     grouping_id,
     metric_name,
     metric_value,
     denver_date,
     CAST(denver_date as date)   AS denver_date_dt
  FROM ${DASP_db}.asp_page_set_pathing_agg
 WHERE (CAST(denver_date as date) >= date_add('DAY', -90, current_date)
        AND CAST(denver_date as date) <= date_add('DAY', -1, current_date))
   and application_name         = 'SMB'
;"

execute_script_in_athena "CREATE OR REPLACE VIEW ${DASP_db}.asp_vat_spec_page_set_pathing_agg AS
SELECT
     current_page_name,
     previous_page_name,
     mso,
     'SpecNet'                  AS application_name,
     select_action_types_value,
     grouping_id,
     metric_name,
     metric_value,
     denver_date,
     CAST(denver_date as date)   AS denver_date_dt
  FROM ${DASP_db}.asp_page_set_pathing_agg
 WHERE (CAST(denver_date as date) >= date_add('DAY', -90, current_date)
        AND CAST(denver_date as date) <= date_add('DAY', -1, current_date))
   and application_name         = 'SpecNet'
;"
