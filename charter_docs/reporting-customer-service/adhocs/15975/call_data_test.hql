SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;

set hive.cli.print.header=true;

	SELECT 
	  account_number
	  ,visit_id
	  ,visit_time
	  ,btm_flag
	  , passwd_flag
	  , cpni_flag
	  , call_id
	  , call_start_time
	  , if(call_start_time - visit_time BETWEEN 0 and 18600,1,0) as call_flag 
	FROM (
		SELECT 
		 visits.account_number
		, visit_id
		, CAST(max(received__timestamp)/1000 as BIGINT) as visit_time
		, if(avg(btm)<'.01',"No",if(avg(btm)>'9.99',"Yes","Both")) as btm_flag
		, max(passwd) as passwd_flag
		, max(cpni) as cpni_flag
		, call_inbound_key as call_id
		, CAST(call_start_timestamp_utc/1000 as BIGINT) as call_start_time
		FROM
       			(SELECT
                         a.visit__account__account_number as account_number 
                        , a.visit__visit_id AS visit_id 
                        , a.received__timestamp 
                        , if(visit__connection__network_status='onNet',10,if(visit__connection__network_status='offNet',0,null)) as btm 
                        , if(state__view__current_page__elements__standardized_name='forgot-username-or-password',1,0) as passwd
                        , if(state__view__current_page__page_name='verifyYourAccount',1,0) as cpni

                        FROM prod.asp_v_venona_events_portals_specnet a
                        WHERE
                        partition_date_utc>='2019-06-07'
                        AND visit__account__account_number is not null
                        GROUP BY visit__visit_id
                                , visit__account__account_number
				,state__view__current_page__elements__standardized_name
				, state__view__current_page__page_name
				, received__timestamp
				,visit__connection__network_status) visits

			LEFT JOIN 
				(SELECT * FROM prod.cs_call_data WHERE call_end_date_utc>='2019-06-06') calls
				on prod.aes_decrypt(visits.account_number) = prod.aes_decrypt256(calls.account_number)
		GROUP BY 
		  visits.account_number
		 , visit_id
		 , call_inbound_key
		 , call_start_timestamp_utc) cv

limit 10
