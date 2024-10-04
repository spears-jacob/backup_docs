SELECT DISTINCT
prod.aes_decrypt(old.account_number) as old_decrypted
, prod.aes_decrypt256(new.account_number) as new_decrypted
, old.segment_id

FROM
 ${hiveconf:old_table} old
INNER JOIN
 ${hiveconf:new_table} new
  on old.segment_id = new.segment_id
WHERE
  old.call_end_date_utc>='2019-01-23'
  AND new.call_end_date_utc>='2019-01-23'  
AND 
--old.account_number <> new.account_number
prod.aes_decrypt(old.account_number) <> prod.aes_decrypt256(new.account_number)
limit 10
 ;

