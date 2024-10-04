USE ${env:LKP_db};

INSERT OVERWRITE TABLE sbnet_dev_event
SELECT event_id, detail
FROM nifi_sbnet_dev_event 
WHERE partition_date = '${env:RUN_DATE}';

INSERT OVERWRITE TABLE sbnet_dev_browser
SELECT browser_id, detail
FROM nifi_sbnet_dev_browser 
WHERE partition_date = '${env:RUN_DATE}';

INSERT OVERWRITE TABLE sbnet_dev_country
SELECT country_id, detail
FROM nifi_sbnet_dev_country 
WHERE partition_date = '${env:RUN_DATE}';

INSERT OVERWRITE TABLE sbnet_dev_connection_type
SELECT connection_type_id, detail
FROM nifi_sbnet_dev_connection_type 
WHERE partition_date = '${env:RUN_DATE}';

INSERT OVERWRITE TABLE sbnet_dev_javascript
SELECT javascript_id, detail
FROM nifi_sbnet_dev_javascript 
WHERE partition_date = '${env:RUN_DATE}';

INSERT OVERWRITE TABLE sbnet_dev_language
SELECT language_id, detail
FROM nifi_sbnet_dev_language 
WHERE partition_date = '${env:RUN_DATE}';

INSERT OVERWRITE TABLE sbnet_dev_os
SELECT os_id, detail
FROM nifi_sbnet_dev_os 
WHERE partition_date = '${env:RUN_DATE}';

INSERT OVERWRITE TABLE sbnet_dev_resolution
SELECT resolution_id, detail
FROM nifi_sbnet_dev_resolution 
WHERE partition_date = '${env:RUN_DATE}';

INSERT OVERWRITE TABLE sbnet_dev_search_engine
SELECT search_engine_id, detail
FROM nifi_sbnet_dev_search_engine 
WHERE partition_date = '${env:RUN_DATE}';
