SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.cli.print.header=true; 

SELECT 
  lower(visit__application_details__application_name) as portal
  ,partition_date_utc
  ,count(prod.aes_decrypt(visit__account__account_billing_id)) as count_billing_id
  ,count(prod.aes_decrypt256(visit__account__account_number_aes_256)) as count_acct_num_256
  ,count(prod.aes_decrypt(visit__account__account_number)) as count_account_number

FROM prod.venona_events_portals 
WHERE partition_date_utc>='2019-04-01' 
  AND visit__application_details__application_name in ('MySpectrum','SpecNet','specnet') 
--  AND prod.aes_decrypt(visit__account__account_billing_id) IS NOT NULL
GROUP BY lower(visit__application_details__application_name), partition_date_utc
ORDER BY portal, partition_date_utc
--limit 30
;

