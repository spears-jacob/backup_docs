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
CREATE TEMPORARY FUNCTION epoch_converter AS 'Epoch_To_Denver';

ADD JAR ${env:JARS_S3_LOCATION}/epochtotimestamp-1.0-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION  epoch_timestamp AS 'Epoch_Timestamp';

SELECT '***** getting voice of customer - troubleshooting ******'
;
INSERT OVERWRITE TABLE asp_extract_voice_of_customer_troubleshooting PARTITION (denver_date) --new table

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
WHERE  ((partition_date_hour_utc >= '${env:START_DATE_TZ}'
     AND partition_date_hour_utc <  '${env:END_DATE_TZ}')
     AND visit__application_details__application_name IN ('SpecNet','MySpectrum'))
AND visit__account__enc_account_number IS NOT NULL
AND visit__account__enc_account_number NOT IN('7FbKtybuOWU4/Q0SRInbHA==','GSWNkZXIfDPD6x25Na3i8g==') -- exclude blank or 'pending login'
AND message__event_case_id IN ('SPECTRUM_selectAction_equipment_deviceDetails','SPECTRUM_selectAction_equipment_troubleshoot','SPECNET_selectAction_equipment_internet_checkInternetSpeed','SPECTRUM_selectAction_equipment_internet_showWifiPassword','SPECTRUM_selectAction_equipment_manageNetwork','SPECTRUM_selectAction_equipment_troubleshoot_resetEquipment_start','SPECTRUM_selectAction_equipment_internet_manualReset_close','SPECTRUM_selectAction_equipment_voice_manageVoice','SPECNET_selectAction_askSpectrum','SPECTRUM_selectAction_equipment_troubleshoot_resetEquipment_cancel','SPECTRUM_selectAction_equipment_troubleshoot_successfullyReset_close','SPECTRUM_selectAction_equipment_troubleshoot_successfullyReset_continue','SPECTRUM_selectAction_equipment_internet_expandWifiCard','SPECTRUM_selectAction_equipment_internet_manualReset_contactUs','SPECTRUM_selectAction_equipment_internet_hideWifiPassword','SPECTRUM_selectAction_equipment_troubleshoot_successfullyReset_issueResolved','SPECNET_selectAction_equipment_internet_activateEquipment','SPECTRUM_selectAction_equipment_troubleshoot_waitToResetOk','SPECTRUM_selectAction_equipment_manualReset_worldBoxOk','SPECTRUM_selectAction_equipment_troubleshoot_delinquentAccountMakePayment','SPECNET_selectAction_equipment_tv_activateEquipment','SPECTRUM_selectAction_equipment_troubleshoot_waitToResetClose','mySpectrum_selectAction_equipment_deviceManagement_deviceSelect','mySpectrum_selectAction_equipment_troubleshoot','mySpectrum_selectAction_equipment_deviceManagement_viewDeviceInfo','mySpectrum_selectAction_equipment_resetEquip','mySpectrum_selectAction_equipment_manualResetContinue','mySpectrum_selectAction_equipment_manualResetClose','mySpectrum_selectAction_equipment_saveEditWifi','mySpectrum_selectAction_equipment_editNetworkInfo','mySpectrum_selectAction_equipment_confirmWifiChange','mySpectrum_selectAction_equipment_manageDevicesBack','mySpectrum_selectAction_equipment_troubleshootBack','mySpectrum_selectAction_equipment_editNetworkBack','mySpectrum_selectAction_equipment_cancelEditWifi','mySpectrum_selectAction_equipment_resetSuccessContinue','mySpectrum_selectAction_equipment_equipDetails_viewUserGuide','mySpectrum_selectAction_equipment_internetConnectedDevices','mySpectrum_selectAction_equipment_cancelIssue','mySpectrum_selectAction_equipment_manualResetResolved','mySpectrum_selectAction_equipment_resetSuccessIssueResolved','mySpectrum_selectAction_equipment_resetFailureContinue','mySpectrum_selectAction_equipment_internetNotConnectedDevices','mySpectrum_selectAction_equipment_resetEquipClose','mySpectrum_selectAction_equipment_tpsi_activateEquipment','mySpectrum_selectAction_equipment_internetShowPassword','mySpectrum_selectAction_equipment_powerLightOff','mySpectrum_selectAction_equipment_resetFailureClose','mySpectrum_selectAction_equipment_cancelWifiChange','mySpectrum_selectAction_equipment_reviewConnectionSteps','mySpectrum_selectAction_equipment_resetSuccessClose','mySpectrum_selectAction_equipment_internetHidePassword')
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
