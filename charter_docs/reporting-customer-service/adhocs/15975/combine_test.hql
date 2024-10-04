SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;

set hive.cli.print.header=true;

	SELECT * FROM
		(SELECT 
		account_number
		, visit_id
		, CAST(max(received__timestamp)/1000 as BIGINT) as visit_time
		, partition_date_utc
		, if(avg(btm)<'.01',"No",if(avg(btm)>'9.99',"Yes","Both")) as btm_flag
		, max(passwd) as passwd_flag
		, max(cpni) as cpni_flag
		FROM
       			(SELECT
                         a.visit__account__account_number as account_number     -- Customer account number encrypted with 128-bit key length
                        , a.visit__visit_id AS visit_id                                          -- Visit ID is the unique identifier for each customer visit and carries through page views
                        , a.received__timestamp 
                        , a.partition_date_utc 
                        , if(visit__connection__network_status='onNet',10,if(visit__connection__network_status='offNet',0,null)) as btm 
                        , if(state__view__current_page__elements__standardized_name='forgot-username-or-password',1,0) as passwd
                        , if(state__view__current_page__page_name='verifyYourAccount',1,0) as cpni

                        FROM prod.asp_v_venona_events_portals_specnet a
                        WHERE
                        partition_date_utc>='2019-06-07'
                        AND visit__account__account_number is not null
                        GROUP BY visit__visit_id
                                , visit__account__account_number
                                ,partition_date_utc
				,state__view__current_page__elements__standardized_name
				, state__view__current_page__page_name
				, received__timestamp
				,visit__connection__network_status) visits

		GROUP BY 
		  account_number
		 , visit_id
		 , partition_date_utc ) flags
	WHERE btm_flag='Yes'
		limit 100;

