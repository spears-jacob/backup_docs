set hive.resultset.use.unique.column.names=false;
--Once this table has been backfilled, this will be the only one necessary
SELECT
       PROD.AES_DECRYPT256(account_number) as account_number,
       myspectrum,
       specnet,
       spectrumbusiness,
       date_denver,
       PROD.AES_DECRYPT256(division_id) as division_id,
       PROD.AES_DECRYPT256(site_sys) as site_sys,
       prod.aes_decrypt256(prn) as prn,
       prod.aes_decrypt256(agn) as agn
FROM prod.asp_extract_login_data_daily_pvt
WHERE date_denver >="${hiveconf:min_date}"
;
