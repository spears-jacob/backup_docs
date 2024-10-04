SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.resultset.use.unique.column.names=false;
set hive.cli.print.header=true;

  	 Select
        	max(partition_date_utc) as last_login_date_denver,
	        prod.aes_decrypt(visit__account__account_number) as account_number,
	        visit__visit_id,
	        visit__application_details__application_name as application_name	
	    from prod.asp_v_venona_events_portals
	    where partition_date_hour_utc >= '2017-03-01' 
		AND partition_date_hour_utc <='2019-03-01'
	      and prod.aes_decrypt(visit__account__account_number) in ('8245116813904093')
--('8245116813904093','8336300020417681','8246100616548587','8260140212340311','8260140212477311','8260140330283781')


	      and (
	           (visit__application_details__application_name in ('MySpectrum','SpecNet')
	            AND operation__success = TRUE
	            AND message__name = 'loginStop'	
	            )
	         OR(visit__application_details__application_name = 'SMB'
	            AND message__name = 'userConfigSet')
	          )
	    group by 
	             visit__account__account_number,
	             visit__visit_id,	
	             visit__application_details__application_name
limit 10
;
