USE ${env:TMP_db};

CREATE TABLE IF NOT EXISTS ${env:ReprocessDateTable} (run_date string);
INSERT OVERWRITE TABLE ${env:ReprocessDateTable} VALUES('${env:RUN_DATE}');

USE ${env:ENVIRONMENT};
--DROP TABLE asp_page_agg PURGE;

-- PAGE AGG
CREATE TABLE IF NOT EXISTS asp_page_agg (
  mso STRING,
  application_name STRING,
  visit_id STRING,
  account_number STRING,
  device_id STRING,
  current_page_name STRING,
  current_article_name STRING,
  previous_page_name STRING,
  standardized_name STRING,
  modal_name STRING,
  previous_page_viewed_time_ms INT,
  modal_view_instances BIGINT,
  page_view_instances BIGINT,
  select_action_instances BIGINT,
  spinner_success_instances BIGINT,
  spinner_failure_instances BIGINT,
  toggle_flip_instances BIGINT,
  modal_view_devices BIGINT,
  page_view_devices BIGINT,
  select_action_devices BIGINT,
  spinner_success_devices BIGINT,
  spinner_failure_devices BIGINT,
  toggle_flip_devices BIGINT,
  modal_view_households BIGINT,
  page_view_households BIGINT,
  select_action_households BIGINT,
  spinner_success_households BIGINT,
  spinner_failure_households BIGINT,
  toggle_flip_households BIGINT,
  modal_view_visits BIGINT,
  page_view_visits BIGINT,
  select_action_visits BIGINT,
  spinner_success_visits BIGINT,
  spinner_failure_visits BIGINT,
  toggle_flip_visits BIGINT
  )
PARTITIONED BY (denver_date STRING)
TBLPROPERTIES ('retention_policy'='Aggregate - With PII (3 Years)')
;

--DROP TABLE asp_page_agg_counts PURGE;

-- PAGE AGG COUNTS
CREATE TABLE IF NOT EXISTS asp_page_agg_counts (
  application_name STRING,
  current_page_name STRING,
  current_article_name STRING,
  standardized_name STRING,
  modal_name STRING,
  modal_view_count BIGINT,
  page_view_count BIGINT,
  select_action_count BIGINT,
  spinner_success_count BIGINT,
  spinner_failure_count BIGINT,
  toggle_flips_count BIGINT
  )
PARTITIONED BY (denver_date STRING, unit_type STRING)
TBLPROPERTIES ('retention_policy'='Aggregate - Without PII (3 Years)')
;

-- PAGE SET PATHING AGG
CREATE TABLE IF NOT EXISTS asp_page_set_pathing_agg (
  current_page_name string,
  previous_page_name string,
  mso string,
  application_name string,
  select_action_types_value string,
  grouping_id int,
  metric_name string,
  metric_value double
  )
PARTITIONED BY (denver_date string)
TBLPROPERTIES ('retention_policy'='Aggregate - Without PII (3 Years)')
;

--TABLEAU VIEWS
DROP VIEW asp_v_page_pathing_counts;
DROP VIEW asp_v_idm_page_set_pathing_agg;
DROP VIEW asp_v_msa_page_set_pathing_agg;
DROP VIEW asp_v_smb_page_set_pathing_agg;
DROP VIEW asp_v_spec_page_set_pathing_agg;

CREATE VIEW if not exists asp_v_page_pathing_counts AS
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
     toggle_flips_count
  FROM asp_page_agg_counts
 WHERE denver_date            >= DATE_SUB(CURRENT_DATE,90)
;

CREATE VIEW if not exists asp_v_idm_page_set_pathing_agg AS
SELECT
     current_page_name,
     previous_page_name,
     mso,
     application_name,
     select_action_types_value,
     grouping_id,
     metric_name,
     metric_value,
     denver_date
  FROM asp_page_set_pathing_agg
 WHERE denver_date              >= DATE_SUB(CURRENT_DATE,90)
   and application_name         =  'IDManagement'
;

CREATE VIEW if not exists asp_v_msa_page_set_pathing_agg AS
SELECT
     current_page_name,
     previous_page_name,
     mso,
     application_name,
     select_action_types_value,
     grouping_id,
     metric_name,
     metric_value,
     denver_date
  FROM asp_page_set_pathing_agg
 WHERE denver_date              >= DATE_SUB(CURRENT_DATE,90)
   and application_name         = 'MySpectrum'
;

CREATE VIEW if not exists asp_v_smb_page_set_pathing_agg AS
SELECT
     current_page_name,
     previous_page_name,
     mso,
     application_name,
     select_action_types_value,
     grouping_id,
     metric_name,
     metric_value,
     denver_date
  FROM asp_page_set_pathing_agg
 WHERE denver_date              >= DATE_SUB(CURRENT_DATE,90)
   and application_name         = 'SMB'
;

CREATE VIEW if not exists asp_v_spec_page_set_pathing_agg AS
SELECT
     current_page_name,
     previous_page_name,
     mso,
     'SpecNet'                  AS application_name,
     select_action_types_value,
     grouping_id,
     metric_name,
     metric_value,
     denver_date
  FROM asp_page_set_pathing_agg
 WHERE denver_date              >= DATE_SUB(CURRENT_DATE,90)
   and application_name         IN ('specnet', 'SpecNet')
;
