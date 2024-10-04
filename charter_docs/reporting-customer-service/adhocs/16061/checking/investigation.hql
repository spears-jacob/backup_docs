
--SELECT * FROM 
--dev_tmp.cs_msa_actions_20190610
--WHERE visit__visit_id='fe944a73-89ff-45cf-ad43-cfaf7798cc15'
--ORDER BY received__timestamp

SELECT
      	state__view__current_page__page_name
        , message__name
        , state__view__current_page__elements__standardized_name
        , received__timestamp
        ,visit__visit_id --, partition_date_hour_utc
FROM prod.asp_v_venona_events_portals_msa
WHERE
        partition_date_hour_utc>='2019-06-07'
	AND message__name in ('selectAction', 'pageView')
	AND visit__visit_id='1018afb1-d8b1-4bf3-bb16-d18a148fde61'
