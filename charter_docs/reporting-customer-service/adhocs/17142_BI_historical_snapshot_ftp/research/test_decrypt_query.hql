SELECT
--       PROD.AES_DECRYPT256(account_number) as account_number,
 --      myspectrum,
 --      specnet,
 --      spectrumbusiness,
 --      date_denver,
 --      PROD.AES_DECRYPT256(division_id) as division_id,
 --      PROD.AES_DECRYPT256(site_sys) as site_sys,
 --      prod.aes_decrypt256(prn) as prn,
 --      prod.aes_decrypt256(agn) as agn
DISTINCT prn
FROM prod.asp_extract_login_data_daily_pvt
WHERE date_denver = '2019-10-31'
limit 10
;


--SELECT *
--FROM prod.asp_extract_login_data_daily_pvt
--WHERE date_denver='2019-11-17'
--LIMIT 10
--;

--SELECT DISTINCT date_denver
--FROM prod.asp_extract_login_data_daily_pvt
--ORDER BY date_denver
--;
