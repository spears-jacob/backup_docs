SELECT DISTINCT
prod.aes_decrypt(account_number)
FROM prod.asp_extract_login_data_daily_pvt
WHERE date_denver='2019-12-02'
limit 10
;
