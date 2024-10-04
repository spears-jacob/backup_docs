USE ${env:TMP_db};

CREATE TABLE IF NOt EXISTS ${env:ReprocessDateTable} (run_date string);
INSERT OVERWRITE TABLE ${env:ReprocessDateTable} VALUES('${env:RUN_DATE}');

USE ${env:ENVIRONMENT};

-- API AGG
CREATE TABLE IF NOT EXISTS asp_api_agg (
  grouping_id BIGINT,
  hour_denver STRING,
  minute_group STRING,
  application_name STRING,
  mso STRING,
  cust_type STRING,
  api_category STRING,
  api_name STRING,
  stream_subtype String,
  current_page_name STRING,
  metric_name STRING,
  metric_value DOUBLE,
  week_end STRING,
  month_start STRING,
  technology_type STRING)
PARTITIONED BY (denver_date STRING)
;

--TABLEAU VIEWS
DROP VIEW asp_v_idm_api_agg;
DROP VIEW asp_v_msa_api_agg;
DROP VIEW asp_v_smb_api_agg;
DROP VIEW asp_v_spec_api_agg;

CREATE VIEW if not exists asp_v_idm_api_agg AS
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
  FROM asp_api_agg
 WHERE denver_date              >= DATE_SUB(CURRENT_DATE,30)
   AND application_name         = 'IDManagement'
;

CREATE VIEW if not exists asp_v_msa_api_agg AS
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
  FROM asp_api_agg
 WHERE denver_date              >= DATE_SUB(CURRENT_DATE,30)
   AND application_name         = 'MySpectrum'
;

CREATE VIEW if not exists asp_v_smb_api_agg AS
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
  FROM asp_api_agg
 WHERE denver_date              >= DATE_SUB(CURRENT_DATE,30)
   AND application_name         = 'SMB'
;

CREATE VIEW if not exists asp_v_spec_api_agg AS
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
  FROM asp_api_agg
 WHERE denver_date              >= DATE_SUB(CURRENT_DATE,30)
   AND application_name         IN ('specnet', 'SpecNet')
;
