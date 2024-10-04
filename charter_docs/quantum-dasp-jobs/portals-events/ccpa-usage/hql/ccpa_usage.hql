USE ${env:DASP_db};

--XG-17663 CCPA | DASP Usage
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager;
set hive.support.concurrency=false;
set hive.auto.convert.join = true;

set mapreduce.input.fileinputformat.split.maxsize=5368709120;
set mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.optimize.sort.dynamic.partition = false;
set hive.exec.dynamic.partition.mode=nonstrict;
set orc.force.positional.evolution=true;

SET hive.merge.tezfiles=true;
set hive.merge.smallfiles.avgsize=1024000000;
set hive.merge.size.per.task=1024000000;

ADD JAR s3://pi-global-${env:ENVIRONMENT}-udf-jars/epoch-1.0-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION epoch_converter AS 'Epoch_To_Denver';

INSERT OVERWRITE TABLE asp_usage_ccpa_daily_table_a PARTITION (denver_date)
SELECT
  visit__account__enc_account_number               AS account,
  visit__account__enc_account_billing_division_id  AS billing_division_id,
  visit__account__enc_account_billing_division     AS billing_division,
  CASE
       WHEN ISNULL(visit__account__details__mso)   THEN 'MSO-MISSING'
       WHEN visit__account__details__mso = ''      THEN 'MSO-MISSING'
       WHEN visit__account__details__mso = 'NONECAPTURED' THEN 'L-TWC'
       WHEN visit__account__details__mso = 'TWC'   THEN 'L-TWC'
       WHEN visit__account__details__mso = 'CHARTER' THEN 'L-CHTR'
       WHEN visit__account__details__mso = 'BH'    THEN 'L-BHN'
       ELSE visit__account__details__mso
  END                                              AS company,
  visit__application_details__application_name     AS app_name,
  visit__application_details__application_type     AS app_type,

  COUNT(distinct visit__visit_id)                  AS session_count,
  (MAX(received__timestamp) - MIN(received__timestamp))/1000  AS session_duration,
  COUNT(distinct visit__device__enc_uuid)          AS device_count,

  epoch_converter(received__timestamp, 'America/Denver') AS denver_date

  FROM ${env:ENVIRONMENT}.core_quantum_events_sspp
 WHERE partition_date_hour_utc             >= '${hiveconf:START_DATE_TZ}'
   AND partition_date_hour_utc             <  '${hiveconf:END_DATE_TZ}'
   AND visit__account__enc_account_number  IS NOT NULL
   AND message__name                       =  'selectAction'
   AND message__category                   IN ('equipment','navigation')
   AND visit__application_details__application_name IN ('SpecNet','SMB','MySpectrum','IDManagement','PrivacyMicrosite')
GROUP BY epoch_converter(received__timestamp, 'America/Denver'),
         visit__account__enc_account_number,
         visit__account__enc_account_billing_division_id,
         visit__account__enc_account_billing_division,
         CASE
         WHEN ISNULL(visit__account__details__mso) THEN 'MSO-MISSING'
         WHEN visit__account__details__mso = ''    THEN 'MSO-MISSING'
         WHEN visit__account__details__mso = 'NONECAPTURED' THEN 'L-TWC'
         WHEN visit__account__details__mso = 'TWC' THEN 'L-TWC'
         WHEN visit__account__details__mso = 'CHARTER' THEN 'L-CHTR'
         WHEN visit__account__details__mso = 'BH'  THEN 'L-BHN'
         ELSE visit__account__details__mso
         END,
         visit__application_details__application_name,
         visit__application_details__application_type
;

INSERT OVERWRITE TABLE asp_usage_ccpa_daily_table_b PARTITION (denver_date)
SELECT
  visit__account__enc_account_number               AS account,
  visit__account__enc_account_billing_division_id  AS billing_division_id,
  visit__account__enc_account_billing_division     AS billing_division,
  CASE
       WHEN ISNULL(visit__account__details__mso)   THEN 'MSO-MISSING'
       WHEN visit__account__details__mso = ''      THEN 'MSO-MISSING'
       WHEN visit__account__details__mso = 'NONECAPTURED' THEN 'L-TWC'
       WHEN visit__account__details__mso = 'TWC'   THEN 'L-TWC'
       WHEN visit__account__details__mso = 'CHARTER' THEN 'L-CHTR'
       WHEN visit__account__details__mso = 'BH'    THEN 'L-BHN'
       ELSE visit__account__details__mso
  END                                              AS company,
  visit__application_details__application_name     AS app_name,
  visit__application_details__application_type     AS app_type,

  message__category                                AS event_type,
  state__view__current_page__page_name             AS event_name,
  SUM(1)                                           AS event_count,

  epoch_converter(received__timestamp, 'America/Denver') AS denver_date

  FROM ${env:ENVIRONMENT}.core_quantum_events_sspp
 WHERE partition_date_hour_utc             >= '${hiveconf:START_DATE_TZ}'
   AND partition_date_hour_utc             <  '${hiveconf:END_DATE_TZ}'
   AND visit__account__enc_account_number  IS NOT NULL
   AND message__name                       =  'selectAction'
   AND message__category                   IN ('equipment','navigation')
   AND visit__application_details__application_name IN ('SpecNet','SMB','MySpectrum','IDManagement','PrivacyMicrosite')
GROUP BY epoch_converter(received__timestamp, 'America/Denver'),
         visit__account__enc_account_number,
         visit__account__enc_account_billing_division_id,
         visit__account__enc_account_billing_division,
         CASE
         WHEN ISNULL(visit__account__details__mso) THEN 'MSO-MISSING'
         WHEN visit__account__details__mso = ''    THEN 'MSO-MISSING'
         WHEN visit__account__details__mso = 'NONECAPTURED' THEN 'L-TWC'
         WHEN visit__account__details__mso = 'TWC' THEN 'L-TWC'
         WHEN visit__account__details__mso = 'CHARTER' THEN 'L-CHTR'
         WHEN visit__account__details__mso = 'BH'  THEN 'L-BHN'
         ELSE visit__account__details__mso
         END,
         visit__application_details__application_name,
         visit__application_details__application_type,
         message__category,
         state__view__current_page__page_name
;

INSERT OVERWRITE TABLE asp_usage_ccpa_daily_table_c PARTITION (denver_date)
SELECT
  visit__account__enc_account_number               AS account,
  visit__account__enc_account_billing_division_id  AS billing_division_id,
  visit__account__enc_account_billing_division     AS billing_division,
  CASE
       WHEN ISNULL(visit__account__details__mso)   THEN 'MSO-MISSING'
       WHEN visit__account__details__mso = ''      THEN 'MSO-MISSING'
       WHEN visit__account__details__mso = 'NONECAPTURED' THEN 'L-TWC'
       WHEN visit__account__details__mso = 'TWC'   THEN 'L-TWC'
       WHEN visit__account__details__mso = 'CHARTER' THEN 'L-CHTR'
       WHEN visit__account__details__mso = 'BH'    THEN 'L-BHN'
       ELSE visit__account__details__mso
  END                                              AS company,
  visit__application_details__application_name     AS app_name,
  visit__application_details__application_type     AS app_type,

  SUM(IF(visit__application_details__application_name = 'MySpectrum' AND message__name = 'selectAction' AND state__view__current_page__elements__standardized_name = 'confirmRemoveDevice' , 1, 0)) AS device_remove_count,
  NULL                                           AS device_add_count,
  SUM(IF(visit__application_details__application_name = 'MySpectrum' AND message__name = 'selectAction' AND state__view__current_page__elements__standardized_name = 'confirmWifiChange' , 1, 0)) AS network_name_change,

  epoch_converter(received__timestamp, 'America/Denver') AS denver_date

  FROM ${env:ENVIRONMENT}.core_quantum_events_sspp
 WHERE partition_date_hour_utc             >= '${hiveconf:START_DATE_TZ}'
   AND partition_date_hour_utc             <  '${hiveconf:END_DATE_TZ}'
   AND visit__account__enc_account_number  IS NOT NULL
   AND message__name                       =  'selectAction'
   AND message__category                   IN ('equipment','navigation')
   AND visit__application_details__application_name = 'MySpectrum'
GROUP BY epoch_converter(received__timestamp, 'America/Denver'),
         visit__account__enc_account_number,
         visit__account__enc_account_billing_division_id,
         visit__account__enc_account_billing_division,
         CASE
         WHEN ISNULL(visit__account__details__mso) THEN 'MSO-MISSING'
         WHEN visit__account__details__mso = ''    THEN 'MSO-MISSING'
         WHEN visit__account__details__mso = 'NONECAPTURED' THEN 'L-TWC'
         WHEN visit__account__details__mso = 'TWC' THEN 'L-TWC'
         WHEN visit__account__details__mso = 'CHARTER' THEN 'L-CHTR'
         WHEN visit__account__details__mso = 'BH'  THEN 'L-BHN'
         ELSE visit__account__details__mso
         END,
         visit__application_details__application_name,
         visit__application_details__application_type
;
