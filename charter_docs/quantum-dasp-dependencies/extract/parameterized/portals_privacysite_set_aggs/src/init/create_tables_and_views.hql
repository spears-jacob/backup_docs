USE ${env:TMP_db};

CREATE TABLE IF NOT EXISTS ${env:ReprocessDateTable_daily} (run_date string);
INSERT OVERWRITE TABLE ${env:ReprocessDateTable_daily} VALUES('${env:RUN_DATE}');

-----------------------------------------

USE ${env:ENVIRONMENT};

CREATE TABLE IF NOT EXISTS asp_privacysite_set_agg
(
  app_section  STRING,
  user_role    STRING,
  unit_type    STRING,
  metric_name  STRING,
  metric_value DOUBLE
)
PARTITIONED BY (label_date_denver STRING, grain STRING)
TBLPROPERTIES ('retention_policy'='Aggregate - Without PII (3 Years)')
;


--------------------------------------------------------------------------------
-- asp_privacysite_set_agg_v VIEW Creation
--------------------------------------------------------------------------------
DROP VIEW IF EXISTS asp_privacysite_set_agg_v;

CREATE VIEW asp_privacysite_set_agg_v AS
select *
from prod.asp_privacysite_set_agg;
