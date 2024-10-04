USE ${env:LKP_db};

SELECT "

  ⚒ ⚒     Now loading ingest lookups with the latest data,
  ⚒ ⚒     which was found to be:  ${env:MX_PD}

";

INSERT OVERWRITE TABLE bhn_residential_event
SELECT event_id, detail
FROM nifi.asp_bhn_residential_event
WHERE partition_date="${env:MX_PD}";

INSERT OVERWRITE TABLE bhn_residential_browser
SELECT browser_id, detail
FROM nifi.asp_bhn_residential_browser
WHERE partition_date="${env:MX_PD}";

INSERT OVERWRITE TABLE bhn_residential_country
SELECT country_id, detail
FROM nifi.asp_bhn_residential_country
WHERE partition_date="${env:MX_PD}";

INSERT OVERWRITE TABLE bhn_residential_connection_type
SELECT connection_type_id, detail
FROM nifi.asp_bhn_residential_connection_type
WHERE partition_date="${env:MX_PD}";

INSERT OVERWRITE TABLE bhn_residential_javascript
SELECT javascript_id, detail
FROM nifi.asp_bhn_residential_javascript
WHERE partition_date="${env:MX_PD}";

INSERT OVERWRITE TABLE bhn_residential_language
SELECT language_id, detail
FROM nifi.asp_bhn_residential_language
WHERE partition_date="${env:MX_PD}";

INSERT OVERWRITE TABLE bhn_residential_os
SELECT os_id, detail
FROM nifi.asp_bhn_residential_os
WHERE partition_date="${env:MX_PD}";

INSERT OVERWRITE TABLE bhn_residential_resolution
SELECT resolution_id, detail
FROM nifi.asp_bhn_residential_resolution
WHERE partition_date="${env:MX_PD}";

INSERT OVERWRITE TABLE bhn_residential_search_engine
SELECT search_engine_id, detail
FROM nifi.asp_bhn_residential_search_engine
WHERE partition_date="${env:MX_PD}";
