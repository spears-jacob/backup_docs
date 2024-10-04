SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.resultset.use.unique.column.names=false;
set hive.cli.print.header=true;

DROP TABLE dev.cs_asp_extract_login_data_daily_pvt_normalized PURGE;

CREATE TABLE dev.cs_asp_extract_login_data_daily_pvt_normalized AS
SELECT
      CASE WHEN PROD.AES_DECRYPT256(account_number) IS NOT NULL
        THEN PROD.AES_DECRYPT256(account_number)
        ELSE cast(account_number as string) END
        as account_number,
       myspectrum,
       specnet,
       spectrumbusiness,
       date_denver,
       CASE WHEN PROD.AES_DECRYPT256(account_number) IS NOT NULL
         THEN prod.aes_decrypt256(division_id)
         ELSE cast(division_id as string) END
         as division_id,
       CASE WHEN PROD.AES_DECRYPT256(account_number) IS NOT NULL
           THEN prod.aes_decrypt256(site_sys)
           ELSE cast(site_sys as string) END
           as site_sys,
       CASE WHEN PROD.AES_DECRYPT256(account_number) IS NOT NULL
           THEN prod.aes_decrypt256(prn)
           ELSE cast(prn as string) END
           as prn,
       CASE WHEN PROD.AES_DECRYPT256(account_number) IS NOT NULL
             THEN prod.aes_decrypt256(agn)
             ELSE cast(agn as string) END
             as agn
FROM prod.asp_extract_login_data_daily_pvt
--WHERE date_denver>='2018-09-01'
WHERE date_denver>='2019-09-01'
;

SELECT * FROM dev.cs_asp_extract_login_data_daily_pvt_normalized
limit 10
;

