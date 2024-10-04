SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.cli.print.header=true; 


SELECT
 prod.aes_decrypt(visit__account__account_billing_id) as billing_id
 , prod.aes_decrypt(visit__account__account_number) as account_number
FROM prod.venona_events_portals
WHERE partition_date_utc='2019-04-25'
  AND lower(visit__application_details__application_name)='myspectrum'
limit 10
;
