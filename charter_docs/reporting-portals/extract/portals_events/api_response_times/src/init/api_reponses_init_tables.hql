USE ${env:TMP_db};

SELECT '***** Creating ReprocessDateTable ******'
;

CREATE TABLE IF NOt EXISTS ${env:ReprocessDateTable} (
  run_date string)
;

INSERT OVERWRITE TABLE ${env:ReprocessDateTable} VALUES('${env:RUN_DATE}');

SELECT '***** Creating asp_api_responses_raw table ******'
;

USE ${env:ENVIRONMENT};

CREATE TABLE IF NOT EXISTS asp_api_responses_raw (
  application_name string,
  app_version string,
  mso string,
  api_category string,
  api_name string,
  api_code string,
  page_name string,
  response_ms int,
  response_s_tenths double,
  service_result string
)
  PARTITIONED BY (
  date_denver string
)
;

SELECT '***** Creating asp_v_api_responses view ******'
;

CREATE VIEW IF NOT EXISTS asp_v_api_responses
AS
SELECT
  app_version,
  mso,
  api_category,
  api_name,
  page_name,
  response_s_tenths,
  -- api success count
  SUM(IF(api_code BETWEEN 200 AND 299 OR service_result = 'success', 1,0)) AS success_count,
  -- number of api calls total
  COUNT(1) AS event_count,
  -- total, min, max response time
  SUM(response_ms) AS total_response_time_ms,
  MIN(response_ms) AS min_response_time_ms,
  MAX(response_ms) AS max_response_time_ms,
  date_denver,
  application_name
FROM asp_api_responses_raw
WHERE (date_denver > DATE_SUB(CURRENT_DATE, 90))
GROUP BY
application_name,
app_version,
mso,
api_category,
api_name,
page_name,
response_s_tenths,
date_denver
;

SELECT '***** Creating asp_v_api_responses_specnet view ******'
;

CREATE VIEW IF NOT EXISTS asp_v_api_responses_specnet AS
  select * from asp_v_api_responses
  WHERE application_name = 'specnet';

SELECT '***** Creating asp_v_api_responses_smb view ******'
;

CREATE VIEW IF NOT EXISTS asp_v_api_responses_smb AS
  select * from asp_v_api_responses
  WHERE application_name = 'smb';

SELECT '***** Creating asp_v_api_responses_myspectrum view ******'
;

CREATE VIEW IF NOT EXISTS asp_v_api_responses_myspectrum AS
  select * from asp_v_api_responses
  WHERE application_name = 'myspectrum';

SELECT '***** View & Table creation complete ******'
;
