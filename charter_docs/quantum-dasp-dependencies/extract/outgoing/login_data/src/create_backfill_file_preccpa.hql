set hive.resultset.use.unique.column.names=false;
-- Once this table has been backfilled, this will become obsolete
-- But for now, it's needed to correctly handle the encryption on account_number
----- before 2019-12-03
----- and the lack of encryption on SPA data before the same date
SELECT
       PROD.AES_DECRYPT(account_number) as account_number,
       myspectrum,
       specnet,
       spectrumbusiness,
       date_denver,
       division_id,
       site_sys,
       prn,
       agn
FROM prod.asp_extract_login_data_daily_pvt
WHERE date_denver >='2019-11-11'
AND date_denver < '2019-12-03'
;
