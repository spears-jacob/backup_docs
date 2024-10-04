USE ${env:LKP_db};

SELECT "

  ⚒ ⚒     Now loading ingest lookups with the latest data,
  ⚒ ⚒     which was found to be:  ${env:MX_PD}

";

INSERT OVERWRITE TABLE sbnet_twc_global_event
SELECT event_id, detail
FROM nifi.asp_sbnet_twc_global_event
WHERE partition_date="${env:MX_PD}";

INSERT OVERWRITE TABLE sbnet_twc_global_browser
SELECT browser_id, detail
FROM nifi.asp_sbnet_twc_global_browser
WHERE partition_date="${env:MX_PD}";

INSERT OVERWRITE TABLE sbnet_twc_global_country
SELECT country_id, detail
FROM nifi.asp_sbnet_twc_global_country
WHERE partition_date="${env:MX_PD}";

INSERT OVERWRITE TABLE sbnet_twc_global_connection_type
SELECT connection_type_id, detail
FROM nifi.asp_sbnet_twc_global_connection_type
WHERE partition_date="${env:MX_PD}";

INSERT OVERWRITE TABLE sbnet_twc_global_javascript
SELECT javascript_id, detail
FROM nifi.asp_sbnet_twc_global_javascript
WHERE partition_date="${env:MX_PD}";

INSERT OVERWRITE TABLE sbnet_twc_global_language
SELECT language_id, detail
FROM nifi.asp_sbnet_twc_global_language
WHERE partition_date="${env:MX_PD}";

INSERT OVERWRITE TABLE sbnet_twc_global_os
SELECT os_id, detail
FROM nifi.asp_sbnet_twc_global_os
WHERE partition_date="${env:MX_PD}";

INSERT OVERWRITE TABLE sbnet_twc_global_resolution
SELECT resolution_id, detail
FROM nifi.asp_sbnet_twc_global_resolution
WHERE partition_date="${env:MX_PD}";

INSERT OVERWRITE TABLE sbnet_twc_global_search_engine
SELECT search_engine_id, detail
FROM nifi.asp_sbnet_twc_global_search_engine
WHERE partition_date="${env:MX_PD}";
