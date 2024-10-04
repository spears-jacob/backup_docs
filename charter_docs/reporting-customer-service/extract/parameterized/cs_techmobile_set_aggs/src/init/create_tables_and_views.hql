USE ${env:TMP_db};

CREATE TABLE IF NOT EXISTS ${env:ReprocessDateTable_daily} (run_date string);
INSERT OVERWRITE TABLE ${env:ReprocessDateTable_daily} VALUES('${env:RUN_DATE}');

-----------------------------------------

USE ${env:ENVIRONMENT};

CREATE TABLE IF NOT EXISTS cs_techmobile_set_agg
(
  raw_order_number                     STRING,
  visit_applicationdetails_appversion  STRING,
  visit_location_region                STRING,
  visit_location_regionname            STRING,
  visit_technician_techid              STRING,
  visit_technician_quadid              STRING,
  visit_device_devicetype              STRING,
  visit_device_model                   STRING,
  jobName                              STRING,
  message_timestamp                    STRING,
  receivedDate                         STRING,
  unit_type                            STRING,
  grouping_id                          INT,
  metric_name                          STRING,
  metric_value                         STRING
)
PARTITIONED BY (grain STRING, partition_date_utc STRING)
TBLPROPERTIES ('retention_policy'='Aggregate - Without PII (3 Years)')
;


--------------------------------------------------------------------------------
-- cs_techmobile_set_agg_v VIEW Creation
--------------------------------------------------------------------------------
DROP VIEW IF EXISTS cs_techmobile_set_agg_v;

CREATE VIEW cs_techmobile_set_agg_v AS
select *
from prod.cs_techmobile_set_agg;
