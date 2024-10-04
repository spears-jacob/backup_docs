DROP TABLE dev.cs_historical_snapshot_logins_18541 PURGE;

--CREATE TABLE dev.cs_historical_snapshot_logins_18541 AS
SELECT
prod.aes_decrypt256(account_number) as account_number
, max(myspectrum) as myspectrum
, max(specnet) as specnet
, max(spectrumbusiness)	as spectrumbusiness
, max(prod.aes_decrypt256(division_id)) as division_id
, max(prod.aes_decrypt256(site_sys))	as site_sys
, max(prod.aes_decrypt256(prn)) as prn
, max(prod.aes_decrypt256(agn)) as agn
FROM
prod.asp_extract_login_data_daily_pvt
WHERE date_denver>='2018-09-01'
GROUP BY account_number
--;

--SELECT * FROM dev.cs_historical_snapshot_logins_18541
limit 10
;
