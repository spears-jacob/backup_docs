USE ${env:ENVIRONMENT};

--------------------------------------------------------------------------------
----------------------------- ***** Resi Agg ***** -----------------------------
--------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS asp_metric_pivot_resi_quantum_wk
(
  metric_value DECIMAL(15,5),
  metric STRING
)
PARTITIONED BY
(
  platform STRING,
  domain STRING,
  company STRING,
  data_source STRING,
  week_end_dt STRING
)
;

CREATE TABLE IF NOT EXISTS asp_metric_pivot_resi_adobe_wk
(
  metric_value DECIMAL(15,5),
  metric STRING
)
PARTITIONED BY
(
  platform STRING,
  domain STRING,
  company STRING,
  data_source STRING,
  week_end_dt STRING
)
;

--------------------------------------------------------------------------------
----------------------------- ***** SMB Agg ***** ------------------------------
--------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS asp_metric_pivot_smb_quantum_wk
(
  metric_value DECIMAL(15,5),
  metric STRING
)
PARTITIONED BY
(
  platform STRING,
  domain STRING,
  company STRING,
  data_source STRING,
  week_end_dt STRING
)
;

CREATE TABLE IF NOT EXISTS asp_metric_pivot_smb_adobe_wk
(
  metric_value DECIMAL(15,5),
  metric STRING
)
PARTITIONED BY
(
  platform STRING,
  domain STRING,
  company STRING,
  data_source STRING,
  week_end_dt STRING
)
;

--------------------------------------------------------------------------------
----------------------------- ***** App Agg ***** ------------------------------
--------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS asp_metric_pivot_app_quantum_wk
(
  metric_value DECIMAL(15,5),
  metric STRING
)
PARTITIONED BY
(
  platform STRING,
  domain STRING,
  company STRING,
  data_source STRING,
  week_end_dt STRING
)
;

CREATE TABLE IF NOT EXISTS asp_metric_pivot_app_adobe_wk
(
  metric_value DECIMAL(15,5),
  metric STRING
)
PARTITIONED BY
(
  platform STRING,
  domain STRING,
  company STRING,
  data_source STRING,
  week_end_dt STRING
)
;

--------------------------------------------------------------------------------
---------------------- ***** Resi Combined Metrics ***** -----------------------
--------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS asp_prod_weekly_resi_adobe_stage
(
  metric_family STRING,
  metric_value DECIMAL(15,5),
  metric STRING,
  report_suite STRING,
  lkp_company STRING,
  portal STRING,
  hive_metric STRING,
  tableau_name STRING,
  quantum_start_fm STRING,
  associated_attempt_metric STRING
)
PARTITIONED BY
(
  platform STRING,
  domain STRING,
  company STRING,
  data_source STRING,
  week_end_dt STRING
)
;

CREATE TABLE IF NOT EXISTS asp_prod_weekly_resi_quantum_stage
(
  metric_family STRING,
  metric_value DECIMAL(15,5),
  metric STRING,
  report_suite STRING,
  lkp_company STRING,
  portal STRING,
  hive_metric STRING,
  tableau_name STRING,
  quantum_start_fm STRING,
  associated_attempt_metric STRING
)
PARTITIONED BY
(
  platform STRING,
  domain STRING,
  company STRING,
  data_source STRING,
  week_end_dt STRING
)
;

CREATE TABLE IF NOT EXISTS asp_prod_weekly_resi_stage
(
  metric_family STRING,
  metric_value DECIMAL(15,5),
  metric STRING,
  report_suite STRING,
  lkp_company STRING,
  portal STRING,
  hive_metric STRING,
  tableau_name STRING,
  quantum_start_fm STRING,
  associated_attempt_metric STRING
)
PARTITIONED BY
(
  platform STRING,
  domain STRING,
  company STRING,
  data_source STRING,
  week_end_dt STRING
)
;

CREATE TABLE IF NOT EXISTS asp_prod_weekly_resi
(
  metric_family STRING,
  portal STRING,
  hive_metric STRING,
  tableau_name STRING,
  name_w_total STRING,
  metric_value DECIMAL(15,5),
  metric_value_string STRING,
  fm_metric_total DECIMAL(15,5),
  fm_metric_total_string STRING,
  quantum_start_fm STRING,
  associated_attempt_metric STRING
)
PARTITIONED BY
(
  platform STRING,
  domain STRING,
  company STRING,
  data_source STRING,
  week_end_dt STRING
)
;

--------------------------------------------------------------------------------
----------------------- ***** SMB Combined Metrics ***** -----------------------
--------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS asp_prod_weekly_smb_adobe_stage
(
  metric_family STRING,
  metric_value DECIMAL(15,5),
  metric STRING,
  report_suite STRING,
  lkp_company STRING,
  portal STRING,
  hive_metric STRING,
  tableau_name STRING,
  quantum_start_fm STRING,
  associated_attempt_metric STRING
)
PARTITIONED BY
(
  platform STRING,
  domain STRING,
  company STRING,
  data_source STRING,
  week_end_dt STRING
)
;

CREATE TABLE IF NOT EXISTS asp_prod_weekly_smb_quantum_stage
(
  metric_family STRING,
  metric_value DECIMAL(15,5),
  metric STRING,
  report_suite STRING,
  lkp_company STRING,
  portal STRING,
  hive_metric STRING,
  tableau_name STRING,
  quantum_start_fm STRING,
  associated_attempt_metric STRING
)
PARTITIONED BY
(
  platform STRING,
  domain STRING,
  company STRING,
  data_source STRING,
  week_end_dt STRING
)
;

CREATE TABLE IF NOT EXISTS asp_prod_weekly_smb_stage
(
  metric_family STRING,
  metric_value DECIMAL(15,5),
  metric STRING,
  report_suite STRING,
  lkp_company STRING,
  portal STRING,
  hive_metric STRING,
  tableau_name STRING,
  quantum_start_fm STRING,
  associated_attempt_metric STRING
)
PARTITIONED BY
(
  platform STRING,
  domain STRING,
  company STRING,
  data_source STRING,
  week_end_dt STRING
)
;

CREATE TABLE IF NOT EXISTS asp_prod_weekly_smb
(
  metric_family STRING,
  portal STRING,
  hive_metric STRING,
  tableau_name STRING,
  name_w_total STRING,
  metric_value DECIMAL(15,5),
  metric_value_string STRING,
  fm_metric_total DECIMAL(15,5),
  fm_metric_total_string STRING,
  quantum_start_fm STRING,
  associated_attempt_metric STRING
)
PARTITIONED BY
(
  platform STRING,
  domain STRING,
  company STRING,
  data_source STRING,
  week_end_dt STRING
)
;

--------------------------------------------------------------------------------
----------------------- ***** App Combined Metrics ***** -----------------------
--------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS asp_prod_weekly_app_adobe_stage
(
  metric_family STRING,
  metric_value DECIMAL(15,5),
  metric STRING,
  report_suite STRING,
  lkp_company STRING,
  portal STRING,
  hive_metric STRING,
  tableau_name STRING,
  quantum_start_fm STRING,
  associated_attempt_metric STRING
)
PARTITIONED BY
(
  platform STRING,
  domain STRING,
  company STRING,
  data_source STRING,
  week_end_dt STRING
)
;

CREATE TABLE IF NOT EXISTS asp_prod_weekly_app_quantum_stage
(
  metric_family STRING,
  metric_value DECIMAL(15,5),
  metric STRING,
  report_suite STRING,
  lkp_company STRING,
  portal STRING,
  hive_metric STRING,
  tableau_name STRING,
  quantum_start_fm STRING,
  associated_attempt_metric STRING
)
PARTITIONED BY
(
  platform STRING,
  domain STRING,
  company STRING,
  data_source STRING,
  week_end_dt STRING
)
;

CREATE TABLE IF NOT EXISTS asp_prod_weekly_app_stage
(
  metric_family STRING,
  metric_value DECIMAL(15,5),
  metric STRING,
  report_suite STRING,
  lkp_company STRING,
  portal STRING,
  hive_metric STRING,
  tableau_name STRING,
  quantum_start_fm STRING,
  associated_attempt_metric STRING
)
PARTITIONED BY
(
  platform STRING,
  domain STRING,
  company STRING,
  data_source STRING,
  week_end_dt STRING
)
;

CREATE TABLE IF NOT EXISTS asp_prod_weekly_app
(
  metric_family STRING,
  portal STRING,
  hive_metric STRING,
  tableau_name STRING,
  name_w_total STRING,
  metric_value DECIMAL(15,5),
  metric_value_string STRING,
  fm_metric_total DECIMAL(15,5),
  fm_metric_total_string STRING,
  quantum_start_fm STRING,
  associated_attempt_metric STRING
)
PARTITIONED BY
(
  platform STRING,
  domain STRING,
  company STRING,
  data_source STRING,
  week_end_dt STRING
)
;

--------------------------------------------------------------------------------
------------------------- ***** Weekly Combined ***** --------------------------
--------------------------------------------------------------------------------


DROP VIEW IF EXISTS asp_v_prod_weekly_all_tab;
CREATE VIEW IF NOT EXISTS asp_v_prod_weekly_all_tab
AS
SELECT * FROM asp_v_prod_weekly_resi_tab
  UNION ALL
    SELECT * FROM asp_v_prod_weekly_smb_tab
      UNION ALL
        SELECT * FROM asp_v_prod_weekly_app_tab
;
