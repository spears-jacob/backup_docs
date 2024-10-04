USE ${env:TMP_db};

CREATE TABLE IF NOT EXISTS ${env:ReprocessDateTable_daily} (run_date string);
INSERT OVERWRITE TABLE ${env:ReprocessDateTable_daily} VALUES('${env:RUN_DATE}');

-----------------------------------------

USE ${env:ENVIRONMENT};

CREATE TABLE IF NOT EXISTS asp_idm_set_agg
(
  page_name                   STRING,
  app_section                 STRING,
  user_role                   STRING,
  device_id                   STRING,
  visit_id                    STRING,
  application_type            STRING,
  device_type                 STRING,
  app_version                 STRING,
  logged_in                   STRING,
  application_name            STRING,
  os_name                     STRING,
  operating_system            STRING,
  browser_name                STRING,
  browser_version             STRING,
  browser_size_breakpoint     STRING,
  form_factor                 STRING,
  referrer_link               STRING,
  grouping_id                 INT,
  metric_name                 STRING,
  metric_value                DOUBLE,
  process_date_time_denver    STRING,
  process_identity            STRING,
  unit_type                   STRING
)
PARTITIONED BY (grain STRING, label_date_denver STRING)
TBLPROPERTIES ('retention_policy'='Aggregate - Without PII (3 Years)')
;


--------------------------------------------------------------------------------
-- asp_idm_set_agg_v VIEW Creation
--------------------------------------------------------------------------------
DROP VIEW IF EXISTS asp_idm_set_agg_v;

CREATE VIEW asp_idm_set_agg_v AS
select *
from prod.asp_idm_set_agg;
