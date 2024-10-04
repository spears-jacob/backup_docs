SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;

set hive.cli.print.header=true; 


			SELECT 
--		         max(a.visit__account__account_number) as account_number     -- Customer account number encrypted with 128-bit key length
		         a.visit__account__account_number as account_number     -- Customer account number encrypted with 128-bit key length
		        , a.visit__visit_id AS visit_id                                          -- Visit ID is the unique identifier for each customer visit and carries through page views
 --			, avg(if(visit__connection__network_status='onNet',10,if(visit__connection__network_status='offNet',0,null))) as btm
 			, if(visit__connection__network_status='onNet',10,if(visit__connection__network_status='offNet',0,null)) as btm

			FROM prod.asp_v_venona_events_portals_specnet a
			WHERE
			partition_date_utc>='2019-06-07'
			AND visit__visit_id='12188658-3912-47d4-a31b-5f235ebff145'
--			GROUP BY --visit__account__account_number,
--			visit__visit_id
--limit 10;
