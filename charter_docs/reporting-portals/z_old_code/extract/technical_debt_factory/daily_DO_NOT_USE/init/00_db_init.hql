USE ${env:ENVIRONMENT};

--------------------------------------------------------------------------------
----------------------------- ***** Resi Agg ***** -----------------------------
--------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS asp_metric_pivot_resi_quantum_daily
(
  metric_value DECIMAL(15,5),
  metric STRING,
  review_comment STRING,
  additional_comment STRING
)
PARTITIONED BY
(
  platform STRING,
  domain STRING,
  company STRING,
  data_source STRING,
  denver_date STRING
)
;

CREATE TABLE IF NOT EXISTS asp_metric_pivot_resi_adobe_daily
(
  metric_value DECIMAL(15,5),
  metric STRING,
  review_comment STRING,
  additional_comment STRING
)
PARTITIONED BY
(
  platform STRING,
  domain STRING,
  company STRING,
  data_source STRING,
  denver_date STRING
)
;

--------------------------------------------------------------------------------
----------------------------- ***** SMB Agg ***** ------------------------------
--------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS asp_metric_pivot_smb_quantum_daily
(
  metric_value DECIMAL(15,5),
  metric STRING,
  review_comment STRING,
  additional_comment STRING
)
PARTITIONED BY
(
  platform STRING,
  domain STRING,
  company STRING,
  data_source STRING,
  denver_date STRING
)
;

CREATE TABLE IF NOT EXISTS asp_metric_pivot_smb_adobe_daily
(
  metric_value DECIMAL(15,5),
  metric STRING,
  review_comment STRING,
  additional_comment STRING
)
PARTITIONED BY
(
  platform STRING,
  domain STRING,
  company STRING,
  data_source STRING,
  denver_date STRING
)
;

--------------------------------------------------------------------------------
----------------------------- ***** App Agg ***** ------------------------------
--------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS asp_metric_pivot_app_quantum_daily
(
  metric_value DECIMAL(15,5),
  metric STRING,
  review_comment STRING,
  additional_comment STRING
)
PARTITIONED BY
(
  platform STRING,
  domain STRING,
  company STRING,
  data_source STRING,
  denver_date STRING
)
;

CREATE TABLE IF NOT EXISTS asp_metric_pivot_app_adobe_daily
(
  metric_value DECIMAL(15,5),
  metric STRING,
  review_comment STRING,
  additional_comment STRING
)
PARTITIONED BY
(
  platform STRING,
  domain STRING,
  company STRING,
  data_source STRING,
  denver_date STRING
)
;

--------------------------------------------------------------------------------
---------------------- ***** Resi Combined Metrics ***** -----------------------
--------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS asp_ops_daily_resi_adobe_stage
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
  denver_date STRING
)
;

CREATE TABLE IF NOT EXISTS asp_ops_daily_resi_quantum_stage
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
  denver_date STRING
)
;

CREATE TABLE IF NOT EXISTS asp_ops_daily_resi_stage
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
  denver_date STRING
)
;

CREATE TABLE IF NOT EXISTS asp_ops_daily_resi
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
  denver_date STRING
)
;

--------------------------------------------------------------------------------
----------------------- ***** SMB Combined Metrics ***** -----------------------
--------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS asp_ops_daily_smb_adobe_stage
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
  denver_date STRING
)
;

CREATE TABLE IF NOT EXISTS asp_ops_daily_smb_quantum_stage
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
  denver_date STRING
)
;

CREATE TABLE IF NOT EXISTS asp_ops_daily_smb_stage
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
  denver_date STRING
)
;

CREATE TABLE IF NOT EXISTS asp_ops_daily_smb
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
  denver_date STRING
)
;

--------------------------------------------------------------------------------
----------------------- ***** App Combined Metrics ***** -----------------------
--------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS asp_ops_daily_app_adobe_stage
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
  denver_date STRING
)
;

CREATE TABLE IF NOT EXISTS asp_ops_daily_app_quantum_stage
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
  denver_date STRING
)
;

CREATE TABLE IF NOT EXISTS asp_ops_daily_app_stage
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
  denver_date STRING
)
;

CREATE TABLE IF NOT EXISTS asp_ops_daily_app
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
  denver_date STRING
)
;

--------------------------------------------------------------------------------
------------------------- ***** Ops Daily Combined ***** --------------------------
--------------------------------------------------------------------------------


--DROP VIEW IF EXISTS asp_v_ops_daily_all_tab;
--CREATE VIEW IF NOT EXISTS asp_v_ops_daily_all_tab
--AS
--SELECT * FROM asp_v_ops_daily_resi_tab
--  UNION ALL
--    SELECT * FROM asp_v_ops_daily_smb_tab
--      UNION ALL
--        SELECT * FROM asp_v_ops_daily_app_tab
--;
