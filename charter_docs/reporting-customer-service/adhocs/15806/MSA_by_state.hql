
use prod;
set mapreduce.input.fileinputformat.split.maxsize=5368709120;
set mapreduce.input.fileinputformat.split.minsize=5368709120;


SELECT 
	region
	,acct.division as division
       ,count(distinct ev.visit__account__account_number)  as customer_count
  FROM prod.asp_v_venona_events_portals_msa            ev
  JOIN (select prod.aes_decrypt256(encrypted_legacy_account_number_256) as account_number
		,region
		, location_state as division
          from quantum_atom_snapshot_accounts_v
         where partition_date_denver   >= '2018-08-01'
           and lower(account_type)     = 'subscriber'
           and lower(customer_type)    = 'residential'
           and account_status          = 'CONNECTED')  acct
    ON prod.aes_decrypt(ev.visit__account__account_number) = acct.account_number
 WHERE ev.partition_date_hour_utc        >= '2019-01-01'
GROUP BY region, division
ORDER BY region
;

