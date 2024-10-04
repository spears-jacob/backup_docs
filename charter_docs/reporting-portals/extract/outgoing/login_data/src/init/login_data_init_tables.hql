USE ${env:TMP_db};

SELECT '***** Creating ReprocessDateTable ******'
;

CREATE TABLE IF NOT EXISTS ${env:ReprocessDateTable} (
  run_date string)
;

INSERT OVERWRITE TABLE ${env:ReprocessDateTable} VALUES('${env:RUN_DATE}');

USE ${env:ENVIRONMENT};

SELECT '***** Creating asp_extract_login_data_daily table ******'
;

CREATE TABLE IF NOT EXISTS asp_extract_login_data_daily (
  account_number string,
  application_name string,
  message__name string,
  operation__success string,
  division_id string,
  site_sys string,
  prn string,
  agn string
)
  PARTITIONED BY (
  date_denver string
)
;

SELECT '***** Creating asp_extract_login_data_daily_pvt table ******'
;

CREATE TABLE IF NOT EXISTS asp_extract_login_data_daily_pvt (
  account_number string,
  myspectrum string,
  specnet string,
  spectrumbusiness string,
  division_id string,
  site_sys string,
  prn string,
  agn string
)
  PARTITIONED BY (
  date_denver string
)
;

SELECT '***** Table creation complete ******'
;
