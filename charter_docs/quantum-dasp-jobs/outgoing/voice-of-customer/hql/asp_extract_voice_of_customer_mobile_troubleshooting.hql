USE ${env:DASP_db};

SET hive.vectorized.execution.enabled=false;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET orc.force.positional.evolution=true;
SET hive.merge.tezfiles=true;
SET hive.merge.mapredfiles=true;
SET hive.merge.smallfiles.avgsize=2048000000;
SET hive.merge.size.per.task=2048000000;
SET hive.exec.max.dynamic.partitions=8000;
SET hive.exec.max.dynamic.partitions.pernode=800;


ADD JAR ${env:JARS_S3_LOCATION}/epoch-1.0-SNAPSHOT.jar;
DROP FUNCTION IF EXISTS epoch_converter; CREATE FUNCTION epoch_converter AS 'Epoch_To_Denver';

ADD JAR ${env:JARS_S3_LOCATION}/epochtotimestamp-1.0-SNAPSHOT.jar;
DROP FUNCTION IF EXISTS epoch_timestamp; CREATE FUNCTION epoch_timestamp AS 'Epoch_Timestamp';

SELECT '***** getting voice of customer - mobile troubleshooting ******'
;
INSERT OVERWRITE TABLE asp_extract_voice_of_customer_mobile_troubleshooting PARTITION (denver_date) --new table
SELECT epoch_timestamp(received__timestamp,'America/Denver') AS datetime_denver
      ,visit__account__enc_account_number
      ,visit__account__enc_account_billing_id
      ,visit__visit_id
      ,visit__application_details__application_name
      ,concat_ws(' ',state__view__current_page__user_journey) as user_journey
      ,concat_ws(' ',state__view__current_page__user_sub_journey) as user_sub_journey
      ,visit__account__details__mso
      ,MAX(visit__account__enc_account_billing_division) AS division
      ,MAX(visit__account__enc_account_billing_division_id) AS divisionID
      ,MAX(visit__account__enc_system_sys) AS system
      ,MAX(visit__account__enc_system_prin) AS prin
      ,MAX(visit__account__enc_system_agent) AS agent
      ,epoch_converter(received__timestamp,'America/Denver') AS denver_date
FROM `${env:CQE}`
WHERE ((partition_date_hour_utc >= '${env:START_DATE_TZ}'
AND partition_date_hour_utc <  '${env:END_DATE_TZ}')
AND visit__application_details__application_name IN ('SpecNet','MySpectrum'))
AND visit__account__enc_account_number IS NOT NULL
AND visit__account__enc_account_number NOT IN('7FbKtybuOWU4/Q0SRInbHA==','GSWNkZXIfDPD6x25Na3i8g==')
AND visit__account__enc_account_number IN
    (SELECT charter_ban_aes256
    FROM `${env:MVNO_A}`
    WHERE is_active = 'true')
AND (state__view__current_page__page_name IN ('supportMobile') OR message__event_case_id = 'mySpectrum_selectAction_button_mobileSupport')
GROUP BY epoch_converter(received__timestamp,'America/Denver')
        ,epoch_timestamp(received__timestamp,'America/Denver')
        ,visit__account__enc_account_number
        ,visit__account__enc_account_billing_id
        ,visit__visit_id
        ,visit__application_details__application_name
        ,state__view__current_page__user_journey
        ,state__view__current_page__user_sub_journey
        ,visit__account__details__mso
;
