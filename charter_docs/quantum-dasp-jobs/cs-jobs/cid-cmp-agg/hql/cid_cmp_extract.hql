USE ${env:DASP_db};
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions.pernode=800;
SET hive.exec.max.dynamic.partitions=8000;
SET hive.merge.size.per.task=1024000000;
SET hive.merge.smallfiles.avgsize=1024000000;
SET hive.merge.tezfiles=true;
SET hive.vectorized.execution.enabled=false;
SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;
SET orc.force.positional.evolution=true;

ADD JAR s3://pi-global-${env:ENVIRONMENT}-udf-jars/epoch-1.0-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION epoch_converter AS 'Epoch_To_Denver';

INSERT OVERWRITE TABLE ${env:DASP_db}.cs_cid_cmp_extract PARTITION (denver_date)
SELECT visit__application_details__application_name application_name,
       message__name message_name,
       visit__device__enc_uuid visit_device_uuid,
       CONCAT(visit__visit_id,'-',coalesce(visit__account__enc_account_number,'Unknown')) unique_visit_id,
       visit__application_details__campaign__campaign_id campaign_id,
       state__view__current_page__dec_page_title page_title,
       state__view__current_page__page_id page_id,
       state__view__current_page__page_name page_name,
       epoch_converter(received__timestamp, 'America/Denver') AS denver_date
FROM `${env:CQE_SSPP}`
WHERE (partition_date_hour_utc >= '${env:START_DATE_TZ}'
   AND partition_date_hour_utc <  '${env:END_DATE_TZ}')
   AND message__name = 'pageView'
   AND visit__application_details__application_name
      IN ('SpecNet',
          'SMB',
          'MySpectrum',
          'IDManagement',
          'PrivacyMicrosite'
          'SpectrumCommunitySolutions'
         )
;

