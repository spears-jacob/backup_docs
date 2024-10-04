SELECT
t.account_number
,p.myspectrum
,t.myspectrum
,p.specnet
,t.specnet
,p.spectrumbusiness
,t.spectrumbusiness
FROM prod.asp_extract_login_data_daily_pvt p
JOIN test.asp_extract_login_data_daily_pvt t
ON p.account_number=t.account_number
	and p.date_denver=t.date_denver

WHERE p.account_number IS NULL
OR p.myspectrum!=t.myspectrum
OR p.specnet!=t.specnet
OR p.spectrumbusiness!=t.spectrumbusiness
limit 10
;

