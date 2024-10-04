USE ${env:DASP_db};

set hive.exec.max.dynamic.partitions = 8000;
set hive.optimize.sort.dynamic.partition = false;
set hive.exec.dynamic.partition.mode=nonstrict;
set orc.force.positional.evolution=true;
-- default logic is ignored and Tez tries to group splits into the specified number of groups. Change that parameter carefully.
set tez.grouping.split-count=1200;
SET hive.merge.tezfiles=true;
set hive.merge.smallfiles.avgsize=2048000000;
set hive.merge.size.per.task=2048000000;

ADD JAR ${env:JARS_S3_LOCATION}/epoch-1.0-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION epoch_converter AS 'Epoch_To_Denver';

ADD JAR ${env:JARS_S3_LOCATION}/jdatehour-1.0-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION epoch_datehour AS 'Epoch_Datehour';


SELECT "\n\nFor Cold Page Interactive Time\n\n";
-- FOR COLD PAGE Interactive TIME
-- Drop and rebuild asp_hourly_page_load_tenths_quantum temp table --
DROP TABLE IF EXISTS ${env:TMP_db}.asp_hourly_page_load_tenths_quantum_${env:CLUSTER} PURGE;

-- For troubleshooting queries, remove 'TEMPORARY'
CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.asp_hourly_page_load_tenths_quantum_${env:CLUSTER} AS
SELECT  date_denver,
        date_hour_denver,
        company,
        device_os,
        browser_name,
        page_name,
        pg_load_sec_tenths,
        COUNT(DISTINCT visit__visit_id)   AS unique_visits,
        COUNT(1)       AS instances,
        domain,
        url
FROM  (SELECT epoch_datehour(received__timestamp)  AS date_hour_denver,
              epoch_converter(received__timestamp) AS date_denver,
              COALESCE (visit__account__details__mso,'Unknown') AS company,
              split(visit__device__operating_system,"\\.")[0]   AS device_os,
              visit__device__device_type           AS device_type,
              visit__device__browser__name         AS browser_name,
              state__view__current_page__page_name AS page_name,
              ROUND((state__view__current_page__performance_timing__dom_content_loaded_event_end -
                     state__view__current_page__performance_timing__navigation_start)/1000,1) AS pg_load_sec_tenths,
              visit__visit_id,
              CASE WHEN visit__application_details__application_name = 'SpecNet' THEN 'Spectrum.net'
                   WHEN visit__application_details__application_name = 'SMB'     THEN 'smb'
                   WHEN visit__application_details__application_name = 'IDManagement' THEN 'idm'
              END      AS domain,
              CASE WHEN state__view__current_page__page_url in (
                      'https://www.spectrumbusiness.net/support/forms/smb_contract_buyout',
                      'https://www.spectrumbusiness.net/support/forms/smb_account_takeover',
                      'https://www.spectrumbusiness.net/support/forms/smb_name_change_confirmation',
                      'https://www.spectrum.net/support/forms/contract_buyout',
                      'https://www.spectrum.net/support/forms/account_takeover',
                      'https://www.spectrum.net/support/forms/name_change',
                      'https://www.spectrum.net/support/forms/name_confirmation'
                   ) THEN state__view__current_page__page_url
              ELSE parse_url(state__view__current_page__page_url,'HOST')
              END AS url
        FROM ${env:ENVIRONMENT}.core_quantum_events_sspp
       WHERE (
         ( partition_date_hour_utc  >= '${hiveconf:START_DATE_TZ}' AND partition_date_hour_utc < '${hiveconf:END_DATE_TZ}')
         AND (visit__application_details__application_name IN ('SpecNet','SMB','MySpectrum','IDManagement'))
              )
         AND message__name = 'pageViewPerformance'
         AND (state__view__current_page__performance_timing__dom_content_loaded_event_end -
              state__view__current_page__performance_timing__navigation_start) > 0
       ) dictionary
GROUP BY date_denver,
         date_hour_denver,
         company,
         device_os,
         browser_name,
         page_name,
         pg_load_sec_tenths,
         domain,
         url
;

-- Drop and rebuild asp_hourly_page_load_tenths_quantum RESULTS temp table --
DROP TABLE IF EXISTS ${env:TMP_db}.asp_hourly_page_load_tenths_quantum_results_${env:CLUSTER};
CREATE TEMPORARY TABLE ${env:TMP_db}.asp_hourly_page_load_tenths_quantum_results_${env:CLUSTER} AS
SELECT 'Cold Interactive'          AS pg_load_type,
       company,
       device_os,
       browser_name,
       page_name,
       pg_load_sec_tenths,
       unique_visits,
       instances,
       url,
       domain,
       date_hour_denver,
       date_denver
FROM ${env:TMP_db}.asp_hourly_page_load_tenths_quantum_${env:CLUSTER};


SELECT "\n\nFor Cold Page Fully Loaded Time\n\n";
-- FOR COLD PAGE Fully Loaded TIME
-- Drop and rebuild asp_hourly_page_load_tenths_quantum temp table --
DROP TABLE IF EXISTS ${env:TMP_db}.asp_hourly_page_load_tenths_quantum_${env:CLUSTER} PURGE;

-- For troubleshooting queries, remove 'TEMPORARY'
CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.asp_hourly_page_load_tenths_quantum_${env:CLUSTER} AS
SELECT  date_denver,
        date_hour_denver,
        company,
        device_os,
        browser_name,
        page_name,
        pg_load_sec_tenths,
        COUNT(DISTINCT visit__visit_id)   AS unique_visits,
        COUNT(1)       AS instances,
        domain,
        url
FROM  (SELECT epoch_datehour(received__timestamp)  AS date_hour_denver,
              epoch_converter(received__timestamp) AS date_denver,
              COALESCE (visit__account__details__mso,'Unknown') AS company,
              split(visit__device__operating_system,"\\.")[0]   AS device_os,
              visit__device__device_type           AS device_type,
              visit__device__browser__name         AS browser_name,
              state__view__current_page__page_name AS page_name,
              ROUND((state__view__current_page__performance_timing__dom_complete -
                     state__view__current_page__performance_timing__navigation_start)/1000,1) AS pg_load_sec_tenths,
              visit__visit_id,
              CASE WHEN visit__application_details__application_name = 'SpecNet' THEN 'Spectrum.net'
                   WHEN visit__application_details__application_name = 'SMB'     THEN 'smb'
                   WHEN visit__application_details__application_name = 'IDManagement' THEN 'idm'
              END      AS domain,
              CASE WHEN state__view__current_page__page_url in (
                      'https://www.spectrumbusiness.net/support/forms/smb_contract_buyout',
                      'https://www.spectrumbusiness.net/support/forms/smb_account_takeover',
                      'https://www.spectrumbusiness.net/support/forms/smb_name_change_confirmation',
                      'https://www.spectrum.net/support/forms/contract_buyout',
                      'https://www.spectrum.net/support/forms/account_takeover',
                      'https://www.spectrum.net/support/forms/name_change',
                      'https://www.spectrum.net/support/forms/name_confirmation'
                   ) THEN state__view__current_page__page_url
              ELSE parse_url(state__view__current_page__page_url,'HOST')
              END AS url
        FROM ${env:ENVIRONMENT}.core_quantum_events_sspp
       WHERE (
              ( partition_date_hour_utc  >= '${hiveconf:START_DATE_TZ}' AND partition_date_hour_utc < '${hiveconf:END_DATE_TZ}')
                AND (visit__application_details__application_name IN ('SpecNet','SMB','MySpectrum','IDManagement'))
              )
         AND message__name = 'pageViewPerformance'
         AND (state__view__current_page__performance_timing__dom_complete -
              state__view__current_page__performance_timing__navigation_start) > 0
       ) dictionary
GROUP BY date_denver,
         date_hour_denver,
         company,
         device_os,
         browser_name,
         page_name,
         pg_load_sec_tenths,
         domain,
         url
;

INSERT INTO TABLE ${env:TMP_db}.asp_hourly_page_load_tenths_quantum_results_${env:CLUSTER}
SELECT 'Cold Fully Loaded'          AS pg_load_type,
       company,
       device_os,
       browser_name,
       page_name,
       pg_load_sec_tenths,
       unique_visits,
       instances,
       url,
       domain,
       date_hour_denver,
       date_denver
FROM ${env:TMP_db}.asp_hourly_page_load_tenths_quantum_${env:CLUSTER};


SELECT "\n\nFor Hot Page Load Time\n\n";
-- FOR HOT PAGE LOAD TIME

-- Drop and rebuild asp_hourly_page_load_tenths_quantum temp table --
DROP TABLE IF EXISTS ${env:TMP_db}.asp_hourly_page_load_tenths_quantum_${env:CLUSTER} PURGE;

-- For troubleshooting queries, remove 'TEMPORARY'
CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.asp_hourly_page_load_tenths_quantum_${env:CLUSTER} AS
SELECT  date_denver,
        date_hour_denver,
        company,
        device_os,
        browser_name,
        page_name,
        pg_load_sec_tenths,
        COUNT(DISTINCT visit__visit_id) AS unique_visits,
        COUNT(1)       AS instances,
        domain,
        url
FROM  (SELECt epoch_datehour(received__timestamp)    AS date_hour_denver,
              epoch_converter(received__timestamp)   AS date_denver,
              COALESCE (visit__account__details__mso,'Unknown') AS company,
              split(visit__device__operating_system,"\\.")[0]   AS device_os,
              visit__device__device_type     AS device_type,
              visit__device__browser__name   AS browser_name,
              state__view__current_page__page_name AS page_name,
              ROUND(state__view__current_page__render_details__fully_rendered_ms/1000,1) AS pg_load_sec_tenths,
              visit__visit_id,
              CASE WHEN visit__application_details__application_name = 'SpecNet' THEN 'Spectrum.net'
                   WHEN visit__application_details__application_name = 'SMB'     THEN 'smb'
                   WHEN visit__application_details__application_name = 'IDManagement' THEN 'idm'
              END      AS domain,
              CASE WHEN state__view__current_page__page_url in (
                      'https://www.spectrumbusiness.net/support/forms/smb_contract_buyout',
                      'https://www.spectrumbusiness.net/support/forms/smb_account_takeover',
                      'https://www.spectrumbusiness.net/support/forms/smb_name_change_confirmation',
                      'https://www.spectrum.net/support/forms/contract_buyout',
                      'https://www.spectrum.net/support/forms/account_takeover',
                      'https://www.spectrum.net/support/forms/name_change',
                      'https://www.spectrum.net/support/forms/name_confirmation'
                   ) THEN state__view__current_page__page_url
              ELSE parse_url(state__view__current_page__page_url,'HOST')
              END AS url
        FROM ${env:ENVIRONMENT}.core_quantum_events_sspp
       WHERE (
              ( partition_date_hour_utc  >= '${hiveconf:START_DATE_TZ}' AND partition_date_hour_utc < '${hiveconf:END_DATE_TZ}')
              and visit__application_details__application_name IN ('SpecNet','SMB','IDManagement'))
         AND message__name = 'pageView'
         AND state__view__current_page__render_details__fully_rendered_ms > 0
       ) dictionary
GROUP BY date_denver,
         date_hour_denver,
         company,
         device_os,
         browser_name,
         page_name,
         pg_load_sec_tenths,
         domain,
         url
;

INSERT INTO TABLE ${env:TMP_db}.asp_hourly_page_load_tenths_quantum_results_${env:CLUSTER}
select 'Hot'           AS pg_load_type,
       company,
       device_os,
       browser_name,
       page_name,
       pg_load_sec_tenths,
       unique_visits,
       instances,
       url,
       domain,
       date_hour_denver,
       date_denver
FROM ${env:TMP_db}.asp_hourly_page_load_tenths_quantum_${env:CLUSTER};

-- Single insert overwrite to results table, keeps it clean by creating up to 2GB single file in each partition
INSERT OVERWRITE TABLE asp_hourly_page_load_tenths_quantum PARTITION(date_denver)
SELECT * FROM ${env:TMP_db}.asp_hourly_page_load_tenths_quantum_results_${env:CLUSTER};

--- Keep GLUE and HDFS clean by deleting temporary tables after use
DROP TABLE IF EXISTS ${env:TMP_db}.asp_hourly_page_load_tenths_quantum_${env:CLUSTER} PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.asp_hourly_page_load_tenths_quantum_results_${env:CLUSTER} PURGE;
