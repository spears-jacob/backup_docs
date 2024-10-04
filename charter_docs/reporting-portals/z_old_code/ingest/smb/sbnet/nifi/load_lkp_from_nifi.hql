USE ${env:LKP_db};

INSERT OVERWRITE TABLE sbnet_event
SELECT event_id, detail
FROM nifi_sbnet_event 
WHERE partition_date = '${env:RUN_DATE}';

INSERT OVERWRITE TABLE sbnet_browser
SELECT browser_id, detail
FROM nifi_sbnet_browser 
WHERE partition_date = '${env:RUN_DATE}';

INSERT OVERWRITE TABLE sbnet_country
SELECT country_id, detail
FROM nifi_sbnet_country 
WHERE partition_date = '${env:RUN_DATE}';

INSERT OVERWRITE TABLE sbnet_connection_type
SELECT connection_type_id, detail
FROM nifi_sbnet_connection_type 
WHERE partition_date = '${env:RUN_DATE}';

INSERT OVERWRITE TABLE sbnet_javascript
SELECT javascript_id, detail
FROM nifi_sbnet_javascript 
WHERE partition_date = '${env:RUN_DATE}';

INSERT OVERWRITE TABLE sbnet_language
SELECT language_id, detail
FROM nifi_sbnet_language 
WHERE partition_date = '${env:RUN_DATE}';

INSERT OVERWRITE TABLE sbnet_os
SELECT os_id, detail
FROM nifi_sbnet_os 
WHERE partition_date = '${env:RUN_DATE}';

INSERT OVERWRITE TABLE sbnet_resolution
SELECT resolution_id, detail
FROM nifi_sbnet_resolution 
WHERE partition_date = '${env:RUN_DATE}';

INSERT OVERWRITE TABLE sbnet_search_engine
SELECT search_engine_id, detail
FROM nifi_sbnet_search_engine 
WHERE partition_date = '${env:RUN_DATE}';
