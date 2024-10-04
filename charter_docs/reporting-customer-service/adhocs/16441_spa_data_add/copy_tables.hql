--CREATE TABLE test.asp_extract_login_data_daily_20191112
--AS SELECT * FROM prod.asp_extract_login_data_daily
--;

--SELECT count(account_number) FROM prod.asp_extract_login_data_daily;
--SELECT count(account_number) FROM test.asp_extract_login_data_daily_20191112;

--CREATE TABLE test.asp_extract_login_data_daily_pvt_20191112
--AS SELECT * FROM prod.asp_extract_login_data_daily_pvt
--;

--SELECT count(account_number) FROM prod.asp_extract_login_data_daily_pvt;
--SELECT count(account_number) FROM test.asp_extract_login_data_daily_pvt_20191112;


DROP TABLE IF EXISTS prod.asp_extract_login_data_daily PURGE;

CREATE TABLE IF NOT EXISTS prod.asp_extract_login_data_daily (
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

INSERT INTO prod.asp_extract_login_data_daily partition(date_denver)
SELECT 
account_number
, application_name
, message__name
, operation__success
, cast(null as string) as division_id
, cast(null as string) as site_sys
, cast(null as string) as prn
, cast(null as string) as agn
, date_denver
FROM test.asp_extract_login_data_daily_20191112
;

DROP TABLE IF EXISTS prod.asp_extract_login_data_daily_pvt PURGE;

CREATE TABLE IF NOT EXISTS prod.asp_extract_login_data_daily_pvt (
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

INSERT INTO prod.asp_extract_login_data_daily_pvt partition (date_denver)
SELECT 
account_number
, myspectrum
, specnet
, spectrumbusiness
, cast(null as string) as division_id
, cast(null as string) as site_sys
, cast(null as string) as prn
, cast(null as string) as agn
, date_denver
FROM test.asp_extract_login_data_daily_pvt_20191112
;
