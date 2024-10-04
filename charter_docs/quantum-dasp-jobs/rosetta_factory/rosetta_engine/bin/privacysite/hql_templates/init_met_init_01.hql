USE ${env:TMP_db};

CREATE TABLE IF NOt EXISTS ${env:ReprocessDateTable_daily} (run_date STRING);
INSERT OVERWRITE TABLE ${env:ReprocessDateTable_daily} VALUES('${env:RUN_DATE}');



USE ${env:ENVIRONMENT};

CREATE TABLE IF NOT EXISTS asp_privacysite_metric_agg
(
  app_section     STRING,
  user_role       STRING,
  message_context STRING,
  device_id       STRING,
  visit_id        STRING,
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
