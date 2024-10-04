SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;

set hive.cli.print.header=true;

SELECT 
visit__account__account_number
, received__timestamp
, visit__connection__network_status
,state__view__current_page__elements__standardized_name
, state__view__current_page__page_name
FROM
prod.asp_v_venona_events_portals_specnet
WHERE
partition_date_utc>='2019-06-01'
AND visit__visit_id='4d019435-a598-489e-a4aa-0b1a7569ecf5'
ORDER BY received__timestamp
;
