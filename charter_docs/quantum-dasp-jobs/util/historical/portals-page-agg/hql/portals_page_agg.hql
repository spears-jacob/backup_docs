USE ${env:DASP_db};

set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager;
set hive.support.concurrency=false;
set hive.auto.convert.join = true;

set mapreduce.input.fileinputformat.split.maxsize=5368709120;
set mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.optimize.sort.dynamic.partition = false;
SET hive.exec.dynamic.partition.mode=nonstrict;
set orc.force.positional.evolution=true;

SET hive.merge.tezfiles=true;
set hive.merge.smallfiles.avgsize=1024000000;
set hive.merge.size.per.task=1024000000;

ADD JAR s3://pi-global-${env:ENVIRONMENT}-udf-jars/jars/epoch-1.0-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION epoch_converter AS 'Epoch_To_Denver';

INSERT OVERWRITE TABLE ${env:DASP_db}.asp_page_agg partition(denver_date)
SELECT
  CASE
    WHEN  visit__account__details__mso IN ('CHARTER','CHTR','"CHTR"')       THEN 'L-CHTR'
    WHEN  visit__account__details__mso IN ('TWC','"TWC"', 'NONECAPTURED')   THEN 'L-TWC'
    WHEN  visit__account__details__mso IN ('BH','BHN','"BHN"')              THEN 'L-BHN'
    WHEN (visit__account__details__mso IN ('','unknown','"NONE"', 'NONE')
      OR  visit__account__details__mso IS NULL )                            THEN 'MSO-MISSING'
    ELSE visit__account__details__mso  END           AS mso,

  visit__application_details__application_name       AS application_name,
  visit__visit_id                                    AS visit_id,
  visit__account__enc_account_number                 AS account_number,
  visit__device__enc_uuid                            AS device_id,
  state__view__current_page__page_name               AS current_page_name,
  state__view__current_page__page_title              AS current_article_name,

  state__view__previous_page__page_name              AS previous_page_name,
  CASE WHEN message__event_case_id  IN ('SPECTRUM_settings_spinnerSuccess', 'SPECTRUM_settings_cardSpinnerSuccess', 'SPECTRUM_redeem_cardSpinnerSuccess', 'SPECTRUM_billing_storedPaymentDeleteSpinnerSuccess', 'SPECTRUM_billing_storedPaymentDuplicateReplacedSpinnerSuccess', 'SPECTRUM_settings_spinnerFailure', 'SPECTRUM_settings_cardSpinnerFailure', 'SPECTRUM_redeem_cardSpinnerFailure', 'SPECTRUM_billing_storedPaymentDeleteSpinnerFailure', 'SPECTRUM_billing_storedPaymentDuplicateReplacedSpinnerFailure')
       THEN COALESCE(message__feature__feature_name, state__view__current_page__elements__element_string_value, operation__operation_type)
       ELSE state__view__current_page__elements__standardized_name
       END                                               AS standardized_name,
  state__view__modal__name                               AS modal_name,
  max(state__view__previous_page__page_viewed_time_ms)   AS previous_page_viewed_time_ms,
  --------------------------instances----------------------------------------------------
  SUM(IF(message__name = 'modalView', 1, 0))             AS modal_view_instances,
  SUM(IF(message__name = 'pageView', 1, 0))              AS page_view_instances,
  SUM(IF(message__name = 'selectAction', 1, 0))          AS select_action_instances,
  SUM(IF((message__event_case_id  IN ('SPECTRUM_settings_spinnerSuccess', 'SPECTRUM_settings_cardSpinnerSuccess', 'SPECTRUM_redeem_cardSpinnerSuccess', 'SPECTRUM_billing_storedPaymentDeleteSpinnerSuccess', 'SPECTRUM_billing_storedPaymentDuplicateReplacedSpinnerSuccess')), 1, 0))  AS spinner_success_instances,
  SUM(IF((message__event_case_id  IN ('SPECTRUM_settings_spinnerFailure', 'SPECTRUM_settings_cardSpinnerFailure', 'SPECTRUM_redeem_cardSpinnerFailure', 'SPECTRUM_billing_storedPaymentDeleteSpinnerFailure', 'SPECTRUM_billing_storedPaymentDuplicateReplacedSpinnerFailure')), 1, 0))  AS spinner_failure_instances,
  SUM(IF((message__name = 'selectAction' AND ISNOTNULL(operation__toggle_state)
          AND operation__operation_type = 'toggleFlip'), 1, 0))      AS toggle_flip_instances,

  --------------------------devices---------------------------------------------------------
  SIZE(COLLECT_SET(IF(message__name = 'modalView', visit__device__enc_uuid, NULL)))
        AS modal_view_devices,
  SIZE(COLLECT_SET(IF(message__name = 'pageView', visit__device__enc_uuid, NULL)))
        AS page_view_devices,
  SIZE(COLLECT_SET(IF(message__name = 'selectAction', visit__device__enc_uuid, NULL)))
        AS select_action_devices,
  SIZE(COLLECT_SET(IF(message__event_case_id  IN ('SPECTRUM_settings_spinnerSuccess', 'SPECTRUM_settings_cardSpinnerSuccess', 'SPECTRUM_redeem_cardSpinnerSuccess', 'SPECTRUM_billing_storedPaymentDeleteSpinnerSuccess', 'SPECTRUM_billing_storedPaymentDuplicateReplacedSpinnerSuccess'), visit__device__enc_uuid, NULL)))
        AS spinner_success_devices,
  SIZE(COLLECT_SET(IF(message__event_case_id  IN ('SPECTRUM_settings_spinnerFailure', 'SPECTRUM_settings_cardSpinnerFailure', 'SPECTRUM_redeem_cardSpinnerFailure', 'SPECTRUM_billing_storedPaymentDeleteSpinnerFailure', 'SPECTRUM_billing_storedPaymentDuplicateReplacedSpinnerFailure'), visit__device__enc_uuid, NULL)))
        AS spinner_failure_devices,
  SIZE(COLLECT_SET(IF(message__name = 'selectAction' AND ISNOTNULL(operation__toggle_state) AND operation__operation_type = 'toggleFlip', visit__device__enc_uuid, NULL)))
        AS toggle_flip_devices,

  --------------------------households-----------------------------------------------------
  SIZE(COLLECT_SET(IF(message__name = 'modalView', visit__account__enc_account_number, NULL)))
        AS modal_view_households,
  SIZE(COLLECT_SET(IF(message__name = 'pageView', visit__account__enc_account_number, NULL)))
        AS page_view_households,
  SIZE(COLLECT_SET(IF(message__name = 'selectAction', visit__account__enc_account_number, NULL)))
        AS select_action_households,
  SIZE(COLLECT_SET(IF(message__event_case_id  IN ('SPECTRUM_settings_spinnerSuccess', 'SPECTRUM_settings_cardSpinnerSuccess', 'SPECTRUM_redeem_cardSpinnerSuccess', 'SPECTRUM_billing_storedPaymentDeleteSpinnerSuccess', 'SPECTRUM_billing_storedPaymentDuplicateReplacedSpinnerSuccess'), visit__account__enc_account_number, NULL)))
        AS spinner_success_households,
  SIZE(COLLECT_SET(IF(message__event_case_id  IN ('SPECTRUM_settings_spinnerFailure', 'SPECTRUM_settings_cardSpinnerFailure', 'SPECTRUM_redeem_cardSpinnerFailure', 'SPECTRUM_billing_storedPaymentDeleteSpinnerFailure', 'SPECTRUM_billing_storedPaymentDuplicateReplacedSpinnerFailure'), visit__account__enc_account_number, NULL)))
        AS spinner_failure_households,
  SIZE(COLLECT_SET(IF(message__name = 'selectAction' AND ISNOTNULL(operation__toggle_state) AND operation__operation_type = 'toggleFlip', visit__account__enc_account_number, NULL)))
        AS toggle_flip_households,

  --------------------------visits-----------------------------------------------------------
  SIZE(COLLECT_SET(IF(message__name = 'modalView', visit__visit_id, NULL)))
        AS modal_view_visits,
  SIZE(COLLECT_SET(IF(message__name = 'pageView', visit__visit_id, NULL)))
        AS page_view_visits,
  SIZE(COLLECT_SET(IF(message__name = 'selectAction', visit__visit_id, NULL)))
        AS select_action_visits,
  SIZE(COLLECT_SET(IF(message__event_case_id  IN ('SPECTRUM_settings_spinnerSuccess', 'SPECTRUM_settings_cardSpinnerSuccess', 'SPECTRUM_redeem_cardSpinnerSuccess', 'SPECTRUM_billing_storedPaymentDeleteSpinnerSuccess', 'SPECTRUM_billing_storedPaymentDuplicateReplacedSpinnerSuccess'), visit__visit_id, NULL)))
        AS spinner_success_visits,
  SIZE(COLLECT_SET(IF(message__event_case_id  IN ('SPECTRUM_settings_spinnerFailure', 'SPECTRUM_settings_cardSpinnerFailure', 'SPECTRUM_redeem_cardSpinnerFailure', 'SPECTRUM_billing_storedPaymentDeleteSpinnerFailure', 'SPECTRUM_billing_storedPaymentDuplicateReplacedSpinnerFailure'), visit__visit_id, NULL)))
        AS spinner_failure_visits,
  SIZE(COLLECT_SET(IF(message__name = 'selectAction' AND ISNOTNULL(operation__toggle_state) AND operation__operation_type = 'toggleFlip', visit__visit_id, NULL)))
        AS toggle_flip_visits,

  epoch_converter(received__timestamp, 'America/Denver') AS denver_date

  FROM ${env:ENVIRONMENT}.core_quantum_events_sspp
 WHERE partition_date_hour_utc       >= '${hiveconf:START_DATE_TZ}'
   AND partition_date_hour_utc       <  '${hiveconf:END_DATE_TZ}'
   AND visit__application_details__application_name IN ('SpecNet','SMB','MySpectrum','IDManagement','PrivacyMicrosite')

  GROUP BY
  CASE
    WHEN  visit__account__details__mso IN ('CHARTER','CHTR','"CHTR"')       THEN 'L-CHTR'
    WHEN  visit__account__details__mso IN ('TWC','"TWC"', 'NONECAPTURED')   THEN 'L-TWC'
    WHEN  visit__account__details__mso IN ('BH','BHN','"BHN"')              THEN 'L-BHN'
    WHEN (visit__account__details__mso IN ('','unknown','"NONE"', 'NONE')
          OR visit__account__details__mso IS NULL )                         THEN 'MSO-MISSING'
    ELSE visit__account__details__mso END,
  visit__application_details__application_name,
  visit__visit_id,
  visit__account__enc_account_number,
  visit__device__enc_uuid,
  state__view__current_page__page_name,
  state__view__current_page__page_title,
  state__view__previous_page__page_name,
  CASE WHEN message__event_case_id  IN ('SPECTRUM_settings_spinnerSuccess', 'SPECTRUM_settings_cardSpinnerSuccess', 'SPECTRUM_redeem_cardSpinnerSuccess', 'SPECTRUM_billing_storedPaymentDeleteSpinnerSuccess', 'SPECTRUM_billing_storedPaymentDuplicateReplacedSpinnerSuccess', 'SPECTRUM_settings_spinnerFailure', 'SPECTRUM_settings_cardSpinnerFailure', 'SPECTRUM_redeem_cardSpinnerFailure', 'SPECTRUM_billing_storedPaymentDeleteSpinnerFailure', 'SPECTRUM_billing_storedPaymentDuplicateReplacedSpinnerFailure')
       THEN COALESCE(message__feature__feature_name, state__view__current_page__elements__element_string_value, operation__operation_type)
       ELSE state__view__current_page__elements__standardized_name
       END,
  state__view__modal__name,
  epoch_converter(received__timestamp, 'America/Denver')
  ;
