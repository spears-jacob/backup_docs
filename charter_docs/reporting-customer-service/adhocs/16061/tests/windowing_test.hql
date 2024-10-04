SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;

set hive.cli.print.header=true;

DROP VIEW if exists dev_tmp.cs_msa_actions_20190610;
CREATE VIEW dev_tmp.cs_msa_actions_20190610 AS
SELECT
	state__view__current_page__page_name
        , message__name
        , state__view__current_page__elements__standardized_name
        , received__timestamp
        ,visit__visit_id --, partition_date_hour_utc
--	, ROW_NUMBER
FROM prod.asp_v_venona_events_portals_msa
WHERE
--      AND state__view__current_page__elements__standardized_name='troubleshoot'
--      AND state__view__current_page__page_name='internetTab' AND
        partition_date_hour_utc>='2019-06-07'
        AND message__name in ('selectAction','pageView')
;

SELECT * FROM 
(SELECT 
 visit__visit_id,
 message__name,
 state__view__current_page__page_name,
 state__view__current_page__elements__standardized_name,
 LEAD(state__view__current_page__page_name,1) OVER w AS next_page,
 LEAD(state__view__current_page__elements__standardized_name,1) OVER w AS next_click
FROM 
	prod.asp_v_venona_events_portals_msa
	--dev_tmp.cs_msa_actions_20190610
WHERE partition_date_utc>='2019-06-07'  --added
	AND message__name in ('selectAction') --added
WINDOW w as (PARTITION BY visit__visit_id ORDER BY received__timestamp)) sub
WHERE
 state__view__current_page__page_name='internetTab'
 AND state__view__current_page__elements__standardized_name='troubleshoot'
limit 10
;



