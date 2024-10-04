USE ${env:TMP_db};

SELECT '***** Creating ReprocessDateTable ******'
;

CREATE TABLE IF NOt EXISTS ${env:ReprocessDateTable} (
  run_date string)
;

INSERT OVERWRITE TABLE ${env:ReprocessDateTable} VALUES('${env:RUN_DATE}');

USE ${env:ENVIRONMENT};

SELECT '***** Creating venona_screen_resolutions_visits table ******'
;


CREATE TABLE IF NOT EXISTS venona_screen_resolutions_visits (
  screen_resolution string,
  application_name string,
  application_type string,
  operating_system string,
  device_type string,
  width string,
  height string,
  visits int
)
  PARTITIONED BY (
  date_denver string
)
TBLPROPERTIES ('retention_policy'='Aggregate - Without PII (3 Years)')
;
