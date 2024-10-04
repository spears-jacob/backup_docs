USE ${env:ENVIRONMENT};

set mapreduce.input.fileinputformat.split.maxsize=5368709120;
set mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.optimize.sort.dynamic.partition = false;
set hive.vectorized.execution.enabled = false;

SELECT "\n\nFor 2: error_message\n\n";

DROP TABLE IF EXISTS ${env:TMP_db}.asp_monthly_error_msg_rate PURGE;

CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.asp_monthly_error_msg_rate AS

select '${env:label_date_denver}' AS date_denver,
       CASE WHEN lower(visit__application_details__application_name) = 'specnet' THEN 'Spectrum.net'
            WHEN lower(visit__application_details__application_name) = 'smb'     THEN 'SpectrumBusiness.net'
            WHEN lower(visit__application_details__application_name) = 'myspectrum' THEN 'My Spectrum App'
       END AS domain,
       SUM(IF(message__name='apiCall', 1, 0)) AS numTotal,
       SUM(IF((message__name='apiCall' and application__api__service_result !='success'), 1, 0)) AS numFailure,
       SUM(IF((message__name='apiCall' and application__api__service_result !='success'), 1, 0))/SUM(IF(message__name='apiCall', 1, 0)) as ratioFailure
       --ROUND(SUM(IF((message__name='apiCall' and application__api__service_result !='success'), 1, 0))*100/SUM(IF(message__name='apiCall', 1, 0)),1) as ratioFailure
  from asp_v_venona_events_portals
 WHERE ( partition_date_utc  >= '${env:START_DATE}' AND partition_date_utc < '${env:END_DATE}')
   and lower(visit__application_details__application_name) in ('specnet', 'smb', 'myspectrum')
 group by
       CASE WHEN lower(visit__application_details__application_name) = 'specnet' THEN 'Spectrum.net'
            WHEN lower(visit__application_details__application_name) = 'smb'     THEN 'SpectrumBusiness.net'
            WHEN lower(visit__application_details__application_name) = 'myspectrum' THEN 'My Spectrum App'
       END;

INSERT INTO ${env:TMP_db}.asp_monthly_error_msg_rate
select '${env:label_date_denver}' AS date_denver,
      CASE WHEN lower(visit__application_details__application_name) = 'specmobile' THEN 'Spectrum Mobile Account App'
      END AS domain,
      SUM(IF(message__name='apiCall', 1, 0)) AS numTotal,
      SUM(IF((message__name='apiCall' and application__api__service_result !='success'), 1, 0)) AS numFailure,
      SUM(IF((message__name='apiCall' and application__api__service_result !='success'), 1, 0))/SUM(IF(message__name='apiCall', 1, 0)) as ratioFailure
      --ROUND(SUM(IF((message__name='apiCall' and application__api__service_result !='success'), 1, 0))*100/SUM(IF(message__name='apiCall', 1, 0)),1) as ratioFailure
 from asp_v_venona_events_specmobile
WHERE ( partition_date_utc  >= '${env:START_DATE}' AND partition_date_utc < '${env:END_DATE}')
  and lower(visit__application_details__application_name) in ('specmobile')
GROUP bY lower(visit__application_details__application_name);

INSERT OVERWRIte TABLE asp_product_monthly_metrics partition(label_date_denver)
SELECT domain,
       'error_message_rate' as metric_name,
       ratioFailure as metric_value,
       current_date as run_date,
       date_denver as label_date_denver
  FROM ${env:TMP_db}.asp_monthly_error_msg_rate;
