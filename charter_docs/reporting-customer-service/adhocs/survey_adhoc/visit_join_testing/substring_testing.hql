SELECT 
split(prod.aes_decrypt(visit__account__account_billing_id),'/')[1]

FROM prod.venona_events_portals
WHERE
 (prod.aes_decrypt(visit__account__account_billing_id)='NTX.8260/8260170011727475'
 OR prod.aes_decrypt(visit__account__account_billing_id)='8203110010440001')
 AND partition_date_utc>='2019-04-01'
