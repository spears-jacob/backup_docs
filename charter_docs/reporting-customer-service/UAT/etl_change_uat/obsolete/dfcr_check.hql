SELECT count( DISTINCT call_inbound_key) as  
FROM test.cs_call_care_data_amo_julyv2 
WHERE agent_mso='CHR' AND call_end_date_utc='2019-07-04' AND customer_type='COMMERCIAL'
AND enhanced_account_number=0
AND segment_handled_flag=true
AND prod.aes_decrypt256(account_number)!='unknown'

;
