use prod;
set mapreduce.input.fileinputformat.split.maxsize=5368709120;
set mapreduce.input.fileinputformat.split.minsize=5368709120;


SELECT
	date_yearmonth(epoch_converter(cast(ev.received__timestamp as bigint),'America/Denver'))
                                                          as year_month_denver
	,region
	,acct.division as division
       ,count(distinct ev.visit__account__account_number)  as customer_count
  FROM prod.asp_v_venona_events_portals_msa            ev
  JOIN (select DISTINCT prod.aes_decrypt256(encrypted_legacy_account_number_256) as account_number
		,region
		, location_state as division
          from quantum_atom_snapshot_accounts_v
         where partition_date_denver   >= "${hiveconf:start_date}"
				   and partition_date_denver <"${hiveconf:end_date}"
           and lower(account_type)     = 'subscriber'
           and lower(customer_type)    = 'residential'
           and account_status          = 'CONNECTED')  acct
    ON prod.aes_decrypt(ev.visit__account__account_number) = acct.account_number
 WHERE ev.partition_date_hour_utc        >= "${hiveconf:start_date}"
	 AND ev.partition_date_hour_utc <"${hiveconf:end_date}"
GROUP BY region, division, date_yearmonth(epoch_converter(cast(ev.received__timestamp as bigint),'America/Denver'))
ORDER BY year_month_denver,division,region
;
