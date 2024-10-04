set hive.exec.orc.split.strategy=BI;

INSERT OVERWRITE TABLE ${env:ENVIRONMENT}.cs_selectaction_aggregate PARTITION (partition_date_utc)
SELECT visit__application_details__application_name application_name
,CASE WHEN state__view__current_page__page_name = 'supportArticle'
        THEN regexp_extract(state__view__current_page__page_url,"(\/[A-Za-z\-\/0-9]+)")
        ELSE state__view__current_page__page_name
        END current_page
,CASE WHEN LOWER(state__view__current_page__elements__standardized_name) = "selectcontentcard"
          THEN state__view__current_Page__elements__ui_Name
          ELSE state__view__current_page__elements__standardized_name END element_name
--,visit__device__uuid visit_device_uuid
,visit__device__enc_uuid visit_device_uuid
,CONCAT(visit__visit_id,'-',
  coalesce(visit__account__enc_account_number,"UNKNOWN")
) unique_visit_id
,partition_date_utc
--FROM ${env:ENVIRONMENT}.ASP_V_VENONA_EVENTS_PORTALS
FROM ${env:ENVIRONMENT}.core_quantum_events_portals_v
WHERE 1=1
AND partition_date_utc >= '${env:START_DATE}'
AND message__name = 'selectAction'
;

-- troubleshooting code
-- SELECT * FROM ${env:ENVIRONMENT}.cs_selectaction_aggregate
-- WHERE partition_date_utc='2019-11-25'
-- LIMIT 10
-- ;
