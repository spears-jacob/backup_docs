SELECT DISTINCT
 ev.visit__application_details__application_name
FROM 
 prod.venona_events_portals ev
 INNER JOIN
 prod.quantum_atom_snapshot_accounts_v ac
  on prod.aes_decrypt(ev.visit__account__account_billing_id) = prod.aes_decrypt256(ac.encrypted_legacy_account_number_256)
WHERE
partition_date_utc='2019-04-25'
AND partition_date_denver='2019-04-25'
;
