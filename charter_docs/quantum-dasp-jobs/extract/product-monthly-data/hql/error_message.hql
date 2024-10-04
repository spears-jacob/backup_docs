USE ${env:DASP_db};

set mapreduce.input.fileinputformat.split.maxsize=512000000;
set mapreduce.input.fileinputformat.split.minsize=512000000;
set hive.optimize.sort.dynamic.partition = false;
set hive.vectorized.execution.enabled = false;
set hive.exec.dynamic.partition.mode=nonstrict;
set orc.force.positional.evolution=true;
SET hive.merge.tezfiles=true;
set hive.merge.mapredfiles=true;

SELECT "\n\nFor 2: error_message\n\n";

DROP TABLE IF EXISTS ${env:TMP_db}.asp_monthly_error_msg_rate PURGE;

CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.asp_monthly_error_msg_rate AS

select '${hiveconf:label_date_denver}' AS date_denver,
       CASE WHEN visit__application_details__application_name = 'SpecNet' THEN 'Spectrum.net'
            WHEN visit__application_details__application_name = 'SMB'     THEN 'SpectrumBusiness.net'
            WHEN visit__application_details__application_name = 'MySpectrum' THEN 'My Spectrum App'
            ELSE visit__application_details__application_name
       END AS domain,
       SUM(IF(message__name='apiCall', 1, 0)) AS numTotal,
       SUM(IF((message__name='apiCall' and application__api__service_result !='success'), 1, 0)) AS numFailure,
       SUM(IF((message__name='apiCall' and application__api__service_result !='success'), 1, 0))/SUM(IF(message__name='apiCall', 1, 0)) as ratioFailure
  from ${env:ENVIRONMENT}.core_quantum_events_sspp
 WHERE ( partition_date_utc  >= '${hiveconf:START_DATE}' AND partition_date_utc < '${hiveconf:END_DATE}')
   AND visit__application_details__application_name IN ('SpecNet','SMB','MySpectrum','IDManagement','PrivacyMicrosite')
 group by
       CASE WHEN visit__application_details__application_name = 'SpecNet' THEN 'Spectrum.net'
            WHEN visit__application_details__application_name = 'SMB'     THEN 'SpectrumBusiness.net'
            WHEN visit__application_details__application_name = 'MySpectrum' THEN 'My Spectrum App'
            ELSE visit__application_details__application_name
       END;

INSERT INTO ${env:TMP_db}.asp_monthly_error_msg_rate
select '${hiveconf:label_date_denver}' AS date_denver,
      CASE WHEN visit__application_details__application_name = 'SpecMobile' THEN 'Spectrum Mobile Account App'
      END AS domain,
      SUM(IF(message__name='apiCall', 1, 0)) AS numTotal,
      SUM(IF((message__name='apiCall' and application__api__service_result !='success'), 1, 0)) AS numFailure,
      SUM(IF((message__name='apiCall' and application__api__service_result !='success'), 1, 0))/SUM(IF(message__name='apiCall', 1, 0)) as ratioFailure
      --ROUND(SUM(IF((message__name='apiCall' and application__api__service_result !='success'), 1, 0))*100/SUM(IF(message__name='apiCall', 1, 0)),1) as ratioFailure
 from ${env:ENVIRONMENT}.core_quantum_events_sspp
WHERE ( partition_date_utc  >= '${hiveconf:START_DATE}' AND partition_date_utc < '${hiveconf:END_DATE}')
  and visit__application_details__application_name in ('SpecMobile')
GROUP bY visit__application_details__application_name;

INSERT OVERWRITE TABLE ${env:DASP_db}.asp_product_monthly_metrics partition(label_date_denver)
SELECT domain,
       'error_message_rate' as metric_name,
       ratioFailure as metric_value,
       current_date as run_date,
       date_denver as label_date_denver
  FROM ${env:TMP_db}.asp_monthly_error_msg_rate;

--- Keep GLUE and HDFS clean by deleting temporary tables after use
DROP TABLE IF EXISTS ${env:TMP_db}.asp_monthly_error_msg_rate PURGE;
