SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.resultset.use.unique.column.names=false;
set hive.cli.print.header=true;

DROP TABLE dev.cs_historical_snapshot_logins_17142;

CREATE TABLE dev.cs_historical_snapshot_logins_17142 AS
Select  account_number
	       ,max(case WHEN application_name='MySpectrum' then date_denver END) as myspectrum
	       ,max(case WHEN application_name='SpecNet' then date_denver END) as specnet	       
	       ,max(case WHEN application_name='SMB' then date_denver END) as spectrumbusiness
	from
  	( Select
        	partition_date_utc as date_denver,
	        prod.aes_decrypt(visit__account__account_number) as account_number,
	        visit__visit_id,
	        visit__application_details__application_name as application_name	
	    from prod.asp_v_venona_events_portals
	    where partition_date_hour_utc >= "${hiveconf:start_date}" AND
	        partition_date_hour_utc <"${hiveconf:end_date}" AND
	      visit__account__account_number is not null
	      and visit__account__account_number NOT IN('7FbKtybuOWU4/Q0SRInbHA==','GSWNkZXIfDPD6x25Na3i8g==') -- exclude blank or 'pending login'
	      and (
	           (visit__application_details__application_name in ('MySpectrum','SpecNet')
	            AND operation__success = TRUE
	            AND message__name = 'loginStop'	
	            )
	         OR(visit__application_details__application_name = 'SMB'
	            AND message__name = 'userConfigSet')
	          )
	    group by partition_date_utc,
	             visit__account__account_number,
	             visit__visit_id,	
	             visit__application_details__application_name
	) subquery
	group by account_number
;

SELECT * FROM dev.cs_historical_snapshot_logins_17142
limit 10
;
