USE ${env:LKP_db};

INSERT OVERWRITE TABLE sbnet_twc_event
SELECT event_id, detail
FROM nifi_sbnet_twc_event
WHERE partition_date='${env:RUN_DATE}'
;

INSERT OVERWRITE TABLE sbnet_twc_browser
SELECT browser_id, detail
FROM nifi_sbnet_twc_browser
WHERE partition_date='${env:RUN_DATE}'
;

INSERT OVERWRITE TABLE sbnet_twc_country
SELECT country_id, detail
FROM nifi_sbnet_twc_country
WHERE partition_date='${env:RUN_DATE}'
;

INSERT OVERWRITE TABLE sbnet_twc_connection_type
SELECT connection_type_id, detail
FROM nifi_sbnet_twc_connection_type
WHERE partition_date='${env:RUN_DATE}'
;

INSERT OVERWRITE TABLE sbnet_twc_javascript
SELECT javascript_id, detail
FROM nifi_sbnet_twc_javascript
WHERE partition_date='${env:RUN_DATE}'
;

INSERT OVERWRITE TABLE sbnet_twc_language
SELECT language_id, detail
FROM nifi_sbnet_twc_language
WHERE partition_date='${env:RUN_DATE}'
;

INSERT OVERWRITE TABLE sbnet_twc_os
SELECT os_id, detail
FROM nifi_sbnet_twc_os
WHERE partition_date='${env:RUN_DATE}'
;

INSERT OVERWRITE TABLE sbnet_twc_resolution
SELECT resolution_id, detail
FROM nifi_sbnet_twc_resolution
WHERE partition_date='${env:RUN_DATE}'
;

INSERT OVERWRITE TABLE sbnet_twc_search_engine
SELECT search_engine_id, detail
FROM nifi_sbnet_twc_search_engine
WHERE partition_date='${env:RUN_DATE}'
;
