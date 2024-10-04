USE ${env:TMP_db};

SELECT '***** Creating ReprocessDateTable ******'
;

CREATE TABLE IF NOT EXISTS ${env:ReprocessDateTable} (
  run_date string)
;

INSERT OVERWRITE TABLE ${env:ReprocessDateTable} VALUES('${env:RUN_DATE}');

USE ${env:ENVIRONMENT};

SELECT '***** Creating asp_extract_asapp_visitid_data_daily table ******'
;

CREATE TABLE IF NOT EXISTS asp_extract_asapp_visitid_data_daily (
  external_session_id string,
  application_name string,
  acct_enc string,
  acct string,
  account_number string,
  biller_type string,
  division string,
  division_id string,
  sys string,
  prn string,
  agn string,
  acct_site_id string,
  acct_company string,
  acct_franchise string
)
  PARTITIONED BY (
  date_denver string
)
TBLPROPERTIES ('retention_policy'='Event Level - With PII (1 Year)')
;

SELECT '***** Creating asp_extract_asapp_visitid_data_daily_pvt table ******'
;

CREATE TABLE IF NOT EXISTS asp_extract_asapp_visitid_data_daily_pvt (
  external_session_id string,
  application_name string,
  account_number string,
  biller_type string,
  division string,
  division_id string,
  sys string,
  prn string,
  agn string,
  acct_site_id string,
  acct_company string,
  acct_franchise string
)
  PARTITIONED BY (
  date_denver string
)
TBLPROPERTIES ('retention_policy'='Event Level - With PII (1 Year)')
;

SELECT '***** Table creation complete ******'
;
