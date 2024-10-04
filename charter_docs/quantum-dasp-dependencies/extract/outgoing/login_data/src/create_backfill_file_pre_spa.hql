set hive.resultset.use.unique.column.names=false;
set hive.cli.print.header=true;
-- Once this table has been backfilled, this will be obsolete
-- But for right now, it correctly handles the encryption on account numbers
-----that existed before 2019-12-03
-----(There were some encrypted division_ids before 11-11, but they aren't reliable)
SELECT
       PROD.AES_DECRYPT(account_number) as account_number,
       myspectrum,
       specnet,
       spectrumbusiness,
       date_denver,
       NULL as division_id,
       NULL as site_sys,
       NULL as prn,
       NULL as agn
FROM prod.asp_extract_login_data_daily_pvt
WHERE date_denver >='2018-09-30'
AND date_denver < '2019-11-11'
;
