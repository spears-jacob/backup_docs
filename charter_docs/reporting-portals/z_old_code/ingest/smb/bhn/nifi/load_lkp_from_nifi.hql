USE ${env:LKP_db};

SELECT "

  ⚒ ⚒     Now loading ingest lookups with the latest data,
  ⚒ ⚒     which was found to be:  ${env:MX_PD}

";

INSERT OVERWRITE TABLE sbnet_bhn_event
SELECT event_id, detail
FROM nifi.asp_sbnet_bhn_event
WHERE partition_date="${env:MX_PD}";

INSERT OVERWRITE TABLE sbnet_bhn_browser
SELECT browser_id, detail
FROM nifi.asp_sbnet_bhn_browser
WHERE partition_date="${env:MX_PD}";

INSERT OVERWRITE TABLE sbnet_bhn_country
SELECT country_id, detail
FROM nifi.asp_sbnet_bhn_country
WHERE partition_date="${env:MX_PD}";

INSERT OVERWRITE TABLE sbnet_bhn_connection_type
SELECT connection_type_id, detail
FROM nifi.asp_sbnet_bhn_connection_type
WHERE partition_date="${env:MX_PD}";

INSERT OVERWRITE TABLE sbnet_bhn_javascript
SELECT javascript_id, detail
FROM nifi.asp_sbnet_bhn_javascript
WHERE partition_date="${env:MX_PD}";

INSERT OVERWRITE TABLE sbnet_bhn_language
SELECT language_id, detail
FROM nifi.asp_sbnet_bhn_language
WHERE partition_date="${env:MX_PD}";

INSERT OVERWRITE TABLE sbnet_bhn_os
SELECT os_id, detail
FROM nifi.asp_sbnet_bhn_os
WHERE partition_date="${env:MX_PD}";

INSERT OVERWRITE TABLE sbnet_bhn_resolution
SELECT resolution_id, detail
FROM nifi.asp_sbnet_bhn_resolution
WHERE partition_date="${env:MX_PD}";

INSERT OVERWRITE TABLE sbnet_bhn_search_engine
SELECT search_engine_id, detail
FROM nifi.asp_sbnet_bhn_search_engine
WHERE partition_date="${env:MX_PD}";
