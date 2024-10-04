set hive.exec.orc.split.strategy=BI;

INSERT OVERWRITE TABLE ${env:ENVIRONMENT}.cs_quantum_cid_pageviews PARTITION (partition_date_utc)
SELECT visit__application_details__application_name application_name
,application__api__host application_api_host
,CASE WHEN lower(regexp_replace(state__view__current_page__page_url,"https\:\/\/www.spectrum.net",'')) = "/"
                OR (lower(state__view__current_page__page_url) like "https://www.spectrum.net/%" AND lower(COALESCE(regexp_extract(regexp_replace(state__view__current_page__page_url,"https\:\/\/www.spectrum.net",''),'(\/(?:[A-Za-z\-0-9\=\&\_\!\%\#\/])+)',1),'')) = '')
        THEN 'home'
        ELSE lower(regexp_extract(regexp_replace(state__view__current_page__page_url,"https\:\/\/www.spectrum.net",''),'(\/(?:[A-Za-z\-0-9\=\&\_\!\%\#\/])+)',1))
        END URL_new --new logic
,lower(regexp_extract(state__view__current_page__page_url,'(?:[A-Za-z\-0-9\=\&\_\!\%\#\/\?])cid=([A-Za-z\-0-9]+)',1)) CID
,lower(regexp_extract(state__view__current_page__page_url,'(?:[A-Za-z\-0-9\=\&\_\!\%\#\/\?])cmp=([\_A-Za-z\-0-9]+)',1)) CMP --separate out CMP details
,message__name message_name
--,visit__device__uuid visit_device_uuid
,visit__device__enc_uuid visit_device_uuid
,CONCAT(visit__visit_id,'-',coalesce(visit__account__enc_account_number,"UNKNOWN")) unique_visit_id
,partition_date_utc
--FROM ${env:ENVIRONMENT}.ASP_V_VENONA_EVENTS_PORTALS
FROM ${env:ENVIRONMENT}.core_quantum_events_portals_v
WHERE 1=1
AND partition_date_utc >= '${env:START_DATE}'
AND message__name = 'pageView'
;

--  Troubleshooting code
-- SELECT * FROM ${env:ENVIRONMENT}.cs_quantum_cid_pageviews
-- WHERE partition_date_utc='2019-11-25'
-- limit 10
-- ;
