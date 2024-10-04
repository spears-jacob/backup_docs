SELECT
"account number decrypted"
,count(old.segment_id)
, "This should be 0"
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
 ;

