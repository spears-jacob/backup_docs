SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;

set hive.cli.print.header=true; 

SELECT 
	account_number
	, visit_id
	, received__timestamp as visit_time
	, partition_date_utc
	, if(btm<'.01',"No",if(btm>'9.99',"Yes","Both")) as btm_status
FROM (
SELECT 
        a.visit__account__account_number as account_number,     -- Customer account number encrypted with 128-bit key length
        a.visit__visit_id AS visit_id,                                          -- Visit ID is the unique identifier for each customer visit and carries through page views
        CAST(max(a.received__timestamp)/1000 as BIGINT) as received__timestamp,
        a.partition_date_utc,
 	avg(if(visit__connection__network_status='onNet',10,if(visit__connection__network_status='offNet',0,null))) as btm
FROM prod.asp_v_venona_events_portals a
WHERE
partition_date_utc='2019-05-31'
AND lower(visit__application_details__application_name)='specnet'
GROUP BY visit__visit_id
	, visit__account__account_number
	,partition_date_utc

) b


limit 100;
