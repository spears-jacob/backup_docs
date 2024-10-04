USE ${env:TMP_db};

CREATE TABLE IF NOt EXISTS ${env:ReprocessDateTable_daily} (run_date STRING);
INSERT OVERWRITE TABLE ${env:ReprocessDateTable_daily} VALUES('${env:RUN_DATE}');

-----------------------------------------

USE ${env:ENVIRONMENT};

CREATE TABLE IF NOT EXISTS asp_idm_metric_agg
(
  page_name                 STRING,
  app_section               STRING,
  user_role                 STRING,
  device_id                 STRING,
  visit_id                  STRING,
  application_type          STRING,
  device_type               STRING,
  app_version               STRING,
  logged_in                 STRING,
  application_name          STRING,
  os_name                   STRING,
  operating_system          STRING,
  browser_name              STRING,
  browser_version           STRING,
  browser_size_breakpoint   STRING,
  form_factor               STRING,
  referrer_link             STRING,
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
