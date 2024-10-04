set hive.vectorized.execution.enabled = false;

USE prod;

DROP TABLE IF EXISTS ${env:TMP_db}.asp_venona_counts_hourly_counts_instances_columns;
CREATE TEMPORARY TABLE ${env:TMP_db}.asp_venona_counts_hourly_counts_instances_columns AS
    SELECT
        COALESCE (visit__account__details__mso,'Unknown') as company,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('pageView')   AND LOWER(state__view__current_page__page_name) = LOWER('accountHome') , 1, 0)) AS account_view,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(operation__operation_type) = LOWER('singleSelectCheckBox')   AND LOWER(state__view__current_page__elements__standardized_name) IN (LOWER('autoPayEnroll'), LOWER('autoPayToggleEnable')) , 1, 0)) AS set_up_auto_payment_starts,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('pageView')   AND LOWER(state__view__current_page__page_name) = LOWER('autopaysuccess') , 1, 0)) AS set_up_auto_payment_successes,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__elements__standardized_name) IN (LOWER('autoPayToggleEnable'),LOWER('autoPayEnroll')) , 1, 0)) AS autopay_enroll_radio_toggle,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('pageView')   AND LOWER(state__view__current_page__page_name) = LOWER('billingHome') , 1, 0)) AS bill_pay_view,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('callUsButton') , 1, 0)) AS call_support,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('modalView')   AND LOWER(state__view__modal__name) = LOWER('cancelappointment-modal') , 1, 0)) AS cancelled_appointments,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('contactSpecialist') , 1, 0)) AS contact_our_moving_specialist,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('apiCall')   AND LOWER(application__api__path) = LOWER('/care/api/v1/user/register')   AND application__api__response_code RLIKE '2.*'   AND LOWER(application__api__response_text) NOT IN (LOWER('OK'),NULL) , 1, 0)) AS create_username_flow_failure,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_name) = LOWER('login')   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('createusername') , 1, 0)) AS create_username_flow_start,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('apiCall')   AND LOWER(application__api__path) = LOWER('/care/api/v1/user/register')   AND application__api__response_code RLIKE '2.*'   AND LOWER(application__api__response_text) IN (LOWER('OK'),NULL) , 1, 0)) AS create_username_fow_success,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('pageView')   AND LOWER(state__view__current_page__page_name) = LOWER('dashboardHome') , 1, 0)) AS dashboard_view,
        SUM(IF( LOWER(visit__application_details__application_Name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('installWiFiProfile') , 1, 0)) AS download_wifi_profile_button_click,
        SUM(IF( LOWER(visit__application_details__application_Name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('pageView')   AND LOWER(state__view__current_page__page_Name) = LOWER('spectrumWiFiProfileResponse') , 1, 0)) AS download_wifi_profile_success,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('pageView')   AND LOWER(state__view__current_page__page_name) = LOWER('equipDetails') , 1, 0)) AS equipment_detail_views,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('pageView')   AND LOWER(state__view__current_page__page_name) IN (LOWER('internetTab'),LOWER('voiceTab'),LOWER('tvTab')) , 1, 0)) AS equipment_list_view,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('modalView')   AND LOWER(state__view__modal__name) IN (LOWER('resetrouterfail-modal'),LOWER('resetmodemfail-modal')) , 1, 0)) AS all_equipment_reset_flow_failures,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('modalView')   AND LOWER(state__view__modal__name) IN (LOWER('resetrouterfail-modal'),LOWER('resetmodemfail-modal'))   AND LOWER(state__view__previous_page__page_name) IN (LOWER('internetTab'),LOWER('equipmentHome'))   AND LOWER(state__view__current_page__page_name) <> LOWER('voiceTab') , 1, 0)) AS internet_equipment_reset_flow_failures,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('modalView')   AND LOWER(state__view__current_page__page_name) = LOWER('tvTab')   AND LOWER(state__view__modal__name) IN (LOWER('resetrouterfail-modal'),LOWER('resetmodemfail-modal')) , 1, 0)) AS tv_equipment_reset_flow_failures,
        SUM(IF( ((LOWER(visit__application_details__application_name) = LOWER('MySpectrum') AND LOWER(message__name) = LOWER('modalView') AND LOWER(state__view__current_page__page_name) = LOWER('voiceTab') AND LOWER(state__view__modal__name) IN (LOWER('resetrouterfail-modal'),LOWER('resetmodemfail-modal'))) OR (LOWER(visit__application_details__application_name) = LOWER('MySpectrum') AND LOWER(message__name) = LOWER('modalView') AND LOWER(state__view__modal__name) IN (LOWER('resetrouterfail-modal'),LOWER('resetmodemfail-modal')) AND LOWER(state__view__current_page__page_name) = LOWER('voiceTab'))), 1, 0)) AS voice_equipment_reset_flow_failures,
        SUM(IF( ((LOWER(visit__application_details__application_name) = LOWER('MySpectrum') AND LOWER(message__name) = LOWER('selectAction') AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('troubleshoot')) OR (LOWER(visit__application_details__application_name) = LOWER('MySpectrum') AND LOWER(message__name) = LOWER('selectAction') AND LOWER(state__view__current_page__elements__standardized_name) IN (LOWER('internetResetEquip'),LOWER('resetEquip')))), 1, 0)) AS all_equipment_reset_flow_starts,
        SUM(IF( ((LOWER(visit__application_details__application_name) = LOWER('MySpectrum') AND LOWER(message__name) = LOWER('selectAction') AND LOWER(state__view__current_page__page_name) = LOWER('internetTab') AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('troubleshoot')) OR (LOWER(visit__application_details__application_name) = LOWER('MySpectrum') AND LOWER(message__name) = LOWER('selectAction') AND LOWER(state__view__current_page__elements__standardized_name) IN (LOWER('internetResetEquip'),LOWER('resetEquip')) AND LOWER(state__view__current_page__page_name) IN (LOWER('internetTab'),LOWER('equipmentHome')))), 1, 0)) AS internet_equipment_reset_flow_starts,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_name) = LOWER('tvTab')   AND LOWER(state__view__current_page__elements__standardized_name) IN(LOWER('troubleshoot'),LOWER('internetConnectIssues'),LOWER('internetTroubleshoot')) , 1, 0)) AS tv_equipment_reset_flow_starts,
        SUM(IF( ((LOWER(visit__application_details__application_name) = LOWER('MySpectrum') AND LOWER(message__name) = LOWER('selectAction') AND LOWER(state__view__current_page__page_name) = LOWER('voiceTab') AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('troubleshoot')) OR (LOWER(visit__application_details__application_name) = LOWER('MySpectrum') AND LOWER(message__name) = LOWER('selectAction') AND LOWER(state__view__current_page__elements__standardized_name) IN (LOWER('internetResetEquip'),LOWER('resetEquip')) AND LOWER(state__view__current_page__page_name) = LOWER('voiceTab'))), 1, 0)) AS voice_equipment_reset_flow_starts,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('modalView')   AND LOWER(state__view__modal__name) = LOWER('resetsuccess-modal') , 1, 0)) AS all_equipment_reset_flow_successes,
        SUM(IF( ((LOWER(visit__application_details__application_name) = LOWER('MySpectrum') AND LOWER(message__name) = LOWER('modalView') AND LOWER(state__view__modal__name) = LOWER('internetresetsuccess-modal') AND LOWER(state__view__previous_page__page_name) IN (LOWER('internetTab'),LOWER('equipmentHome')) AND LOWER(state__view__current_page__page_name) <> LOWER('voiceTab')) OR (LOWER(visit__application_details__application_name) = LOWER('MySpectrum') AND LOWER(message__name) = LOWER('modalView') AND LOWER(state__view__modal__name) = LOWER('resetsuccess-modal') AND LOWER(state__view__current_page__page_name) <> LOWER('voiceTab')) OR (LOWER(visit__application_details__application_name) = LOWER('MySpectrum') AND LOWER(message__name) = LOWER('modalView') AND LOWER(state__view__modal__name) = LOWER('resetsuccess-modal') AND LOWER(state__view__previous_page__page_name) IN (LOWER('internetTab'),LOWER('equipmentHome')) AND LOWER(state__view__current_page__page_name) <> LOWER('voiceTab'))), 1, 0)) AS internet_equipment_reset_flow_successes,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('modalView')   AND LOWER(state__view__current_page__page_name) = LOWER('tvTab')   AND LOWER(state__view__modal__name) = LOWER('internetresetsuccess-modal') , 1, 0)) AS tv_equipment_reset_flow_successes,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('modalView')   AND LOWER(state__view__modal__name) = LOWER('resetsuccess-modal')   AND LOWER(state__view__current_page__page_name) = LOWER('voiceTab') , 1, 0)) AS voice_equipment_reset_flow_successes,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('apiCall')   AND LOWER(application__api__path) = LOWER('/care/api/v1/forgotpwd3')   AND application__api__response_code RLIKE '2.*'   AND LOWER(application__api__response_text) NOT IN (LOWER('OK'),NULL) , 1, 0)) AS forgot_password_flow_failure,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('selectaction')   AND LOWER(state__view__current_page__page_name) = LOWER('login')   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('forgotpassword') , 1, 0)) AS forgot_password_flow_start,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('modalView')   AND LOWER(state__view__modal__name) = LOWER('newpasswordresponse-modal')   AND LOWER(state__view__current_page__page_name) = LOWER('newPasswordEntry') , 1, 0)) AS forgot_password_flow_success,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('apiCall')   AND LOWER(application__api__path) = LOWER('/care/api/v1/forgotusername')   AND application__api__response_code RLIKE '2.*'   AND LOWER(application__api__response_text) NOT IN (LOWER('OK'),NULL) , 1, 0)) AS forgot_username_flow_failure,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_name) = LOWER('login')   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('forgotUsername') , 1, 0)) AS forgot_username_flow_start,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('loginRecoverySendEmail') , 1, 0)) AS forgot_username_flow_success,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('getMoveInfo') , 1, 0)) AS get_moving_info,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(state__view__current_page__page_name) = LOWER('accountHome')   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('wifiProfile') , 1, 0)) AS install_wifi_profile_start,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(state__view__current_page__page_name) = LOWER('spectrumWiFiProfileResponse') , 1, 0)) AS install_wifi_profile_success,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('selectAction')    AND LOWER(state__view__current_page__elements__standardized_name) IN (LOWER('internetConnectIssues'),LOWER('internetTroubleshoot')) , 1, 0)) AS internet_services_connected_experiencing_issues,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('selectAction')    AND LOWER(state__view__current_page__elements__standardized_name) IN (LOWER('internetConnectIssues'),LOWER('internetTroubleshoot')) , 1, 0)) AS internet_services_connection_issue_troubleshoot,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('loginStop') , 1, 0)) AS login_success,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('loginStop') , 1, 0)) AS login_failure,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_name) = LOWER('manualReset')   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('manualResetContinue') , 1, 0)) AS manual_reset_failures,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('pageView')   AND LOWER(state__view__current_page__page_name) = LOWER('manualReset') , 1, 0)) AS manual_reset_starts,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_name) IN (LOWER('internetTSManualReset'),LOWER('internetManualReset'))   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('internetManualResetResolved') , 1, 0)) AS manual_reset_success,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('modalView')   AND LOWER(state__view__current_page__page_name) = LOWER('internetTab')   AND LOWER(state__view__modal__name) = LOWER('resetmodemfail-modal') , 1, 0)) AS internet_modem_reset_failures,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('modalView')   AND LOWER(state__view__current_page__page_name) = LOWER('tvTab')   AND LOWER(state__view__modal__name) = LOWER('resetmodemfail-modal') , 1, 0)) AS tv_modem_reset_failures,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('modalView')   AND LOWER(state__view__current_page__page_name) = LOWER('voiceTab')   AND LOWER(state__view__modal__name) = LOWER('resetmodemfail-modal') , 1, 0)) AS voice_modem_reset_failures,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('apiCall')   AND LOWER(state__view__current_page__page_name) = LOWER('internetResettingEquip')   AND LOWER(application__api__api_name) = LOWER('equipmentV1ModemReset')   AND LOWER(state__view__previous_page__page_name) IN (LOWER('equipmentHome'),LOWER('internetTab'))   AND (application__api__response_code RLIKE '2.*' OR application__api__response_code = '429') , 1, 0)) AS internet_modem_reset_successes,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('apiCall')   AND LOWER(state__view__current_page__page_name) = LOWER('internetResettingEquip')   AND LOWER(application__api__api_name) = LOWER('equipmentV1ModemReset')   AND LOWER(state__view__previous_page__page_name) IN (LOWER('tvTab'))   AND (application__api__response_code RLIKE '2.*' OR application__api__response_code = '429') , 1, 0)) AS tv_modem_reset_successes,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('modalView')   AND LOWER(state__view__modal__name) = LOWER('internetresetsuccess-modal')   AND LOWER(state__view__current_page__page_name) = LOWER('voiceTab') , 1, 0)) AS voice_modem_reset_successes,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('internetResetEquip')   AND state__view__current_page__page_name IN (LOWER('equipmentHome'),LOWER('internetTab')) , 1, 0)) AS internet_modem_router_reset_starts,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('internetResetEquip')   AND state__view__current_page__page_name IN (LOWER('tvTab')) , 1, 0)) AS tv_modem_router_reset_starts,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('internetResetEquip')   AND LOWER(state__view__current_page__page_name) IN (LOWER('voiceTab')) , 1, 0)) AS voice_modem_router_reset_starts,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('movingButton') , 1, 0)) AS moving,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('pageView')   AND LOWER(state__view__current_page__page_name) IN (LOWER('payUnsuccess'),LOWER('payUnsuccessAutoPay')) , 1, 0)) AS one_time_payment_failure,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('makePaymentButton') , 1, 0)) AS one_time_payment_start,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('pageView')   AND LOWER(state__view__current_page__page_name) IN (LOWER('paySuccess'),LOWER('paySuccessAutoPay')) , 1, 0)) AS one_time_payment_success,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('pageView') , 1, 0)) AS page_views,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('appTutorialButton') , 1, 0)) AS quick_tutorial,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('apiCall')   AND LOWER(application__api__path) = LOWER('/care/api/v1/appointment/reschedule')    AND application__api__response_code RLIKE '2.*' , 1, 0)) AS rescheduled_appointments,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('modalView')   AND LOWER(state__view__modal__name) = LOWER('resetrouterfail-modal')   AND LOWER(state__view__previous_page__page_name) IN (LOWER('internetTab'),LOWER('equipmentHome'))   AND LOWER(state__view__current_page__page_name) <> LOWER('voiceTab') , 1, 0)) AS internet_router_reset_failures,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('modalView')   AND LOWER(state__view__current_page__page_name) = LOWER('tvTab')   AND LOWER(state__view__modal__name) = LOWER('resetrouterfail-modal') , 1, 0)) AS tv_router_reset_failures,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('modalView')   AND LOWER(state__view__current_page__page_name) = LOWER('voiceTab')   AND LOWER(state__view__modal__name) = LOWER('resetrouterfail-modal') , 1, 0)) AS voice_router_reset_failures,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('apiCall')   AND LOWER(state__view__current_page__page_name) = LOWER('internetResettingEquip')   AND LOWER(application__api__api_name) = LOWER('equipmentV1RouterReset')   AND LOWER(state__view__previous_page__page_name) IN (LOWER('equipmentHome'),LOWER('internetTab'))   AND (application__api__response_code RLIKE '2.*' OR application__api__response_code = '429') , 1, 0)) AS internet_router_reset_successes,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('apiCall')   AND LOWER(state__view__current_page__page_name) = LOWER('internetResettingEquip')   AND LOWER(application__api__api_name) = LOWER('equipmentV1RouterReset')   AND LOWER(state__view__previous_page__page_name) IN (LOWER('tvTab'))   AND (application__api__response_code RLIKE '2.*' OR application__api__response_code = '429') , 1, 0)) AS tv_router_reset_successes,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('apiCall')   AND LOWER(state__view__current_page__page_name) = LOWER('internetResettingEquip')   AND LOWER(application__api__api_name) = LOWER('equipmentV1RouterReset')   AND LOWER(state__view__previous_page__page_name) IN (LOWER('voiceTab'))   AND (application__api__response_code RLIKE '2.*' OR application__api__response_code = '429') , 1, 0)) AS voice_router_reset_successes,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('apiCall')   AND LOWER(application__api__response_code) NOT RLIKE '2.*'   AND application__api__response_code <> '429' , 1, 0)) AS service_call_failures,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('pageView')   AND LOWER(state__view__current_page__page_name) IN (LOWER('internetTab'),LOWER('voiceTab'),LOWER('tvTab')) , 1, 0)) AS services_tab_views,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('startSession') , 1, 0)) AS session_start,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum'), 1, 0)) AS site_unique,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('supportHome') , 1, 0)) AS support_button,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('pageView')   AND LOWER(state__view__current_page__app_section) = LOWER('support') , 1, 0)) AS support_page_views,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('pageView')   AND LOWER(state__view__current_page__page_name) = LOWER('supportHome') , 1, 0)) AS support_view,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('pageView')   AND LOWER(state__view__current_page__page_name) = LOWER('statementDetail') , 1, 0)) AS view_statement,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('internetShowPassword') , 1, 0)) AS wifi_password_views,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(visit__device__device_type) RLIKE LOWER('and.*') , 1, 0)) AS os_android,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(visit__device__device_type) RLIKE LOWER('ip.*') , 1, 0)) AS os_ios,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('pageView')   AND LOWER(state__view__current_page__app_section) = LOWER('support') , 1, 0)) AS support_section_page_views,
        'asp' AS platform,
        'app' AS domain,
        prod.epoch_datehour(cast(received__timestamp as bigint),'America/Denver') as date_hour_denver,
        epoch_converter(cast(received__timestamp as bigint),'America/Denver') as date_denver
    FROM asp_v_venona_events_portals_msa
         
    WHERE (partition_date_hour_utc >= '${env:START_DATE_TZ}'
       AND partition_date_hour_utc <  '${env:END_DATE_TZ}')
    GROUP BY
        epoch_converter(cast(received__timestamp as bigint),'America/Denver'),
        prod.epoch_datehour(cast(received__timestamp as bigint),'America/Denver'),
        COALESCE (visit__account__details__mso,'Unknown')
    ;
INSERT OVERWRITE TABLE prod.asp_venona_counts_hourly
PARTITION(unit,platform,domain,company,date_denver,source_table)

    SELECT  value,
            metric,
            date_hour_denver,
            'instances',
            'asp',
            'app',
            company,
            date_denver,
            'asp_v_venona_events_portals_msa'
    FROM (SELECT  company,
                  date_denver,
                  date_hour_denver,
                  MAP(

                      'account_view|Account View|MySpectrum||', account_view,
                      'set_up_auto_payment_starts|Auto Pay Enrollment Starts|MySpectrum||', set_up_auto_payment_starts,
                      'set_up_auto_payment_successes|Auto Pay Enrollment Successes|MySpectrum||', set_up_auto_payment_successes,
                      'autopay_enroll_radio_toggle|AutoPay Enroll Radio Toggle|MySpectrum||', autopay_enroll_radio_toggle,
                      'bill_pay_view|Bill Pay View|MySpectrum||', bill_pay_view,
                      'call_support|Call Support|MySpectrum||', call_support,
                      'cancelled_appointments|Cancelled Appointments|MySpectrum||', cancelled_appointments,
                      'contact_our_moving_specialist|Contact Our Moving Specialist|MySpectrum||', contact_our_moving_specialist,
                      'create_username_flow_failure|Create Username Flow Failure|MySpectrum||', create_username_flow_failure,
                      'create_username_flow_start|Create Username Flow Start|MySpectrum||', create_username_flow_start,
                      'create_username_fow_success|Create Username Flow Success|MySpectrum||', create_username_fow_success,
                      'dashboard_view|Dashboard View|MySpectrum||', dashboard_view,
                      'download_wifi_profile_button_click|Download WiFi Profile Button Click|MySpectrum||', download_wifi_profile_button_click,
                      'download_wifi_profile_success|Download WiFi Profile Success|MySpectrum||', download_wifi_profile_success,
                      'equipment_detail_views|Equipment Details Views|MySpectrum||', equipment_detail_views,
                      'equipment_list_view|Equipment List View|MySpectrum||', equipment_list_view,
                      'all_equipment_reset_flow_failures|Equipment Reset Flow Failures All|MySpectrum||', all_equipment_reset_flow_failures,
                      'internet_equipment_reset_flow_failures|Equipment Reset Flow Failures Internet|MySpectrum||', internet_equipment_reset_flow_failures,
                      'tv_equipment_reset_flow_failures|Equipment Reset Flow Failures TV|MySpectrum||', tv_equipment_reset_flow_failures,
                      'voice_equipment_reset_flow_failures|Equipment Reset Flow Failures Voice|MySpectrum||', voice_equipment_reset_flow_failures,
                      'all_equipment_reset_flow_starts|Equipment Reset Flow Starts All|MySpectrum||', all_equipment_reset_flow_starts,
                      'internet_equipment_reset_flow_starts|Equipment Reset Flow Starts Internet|MySpectrum||', internet_equipment_reset_flow_starts,
                      'tv_equipment_reset_flow_starts|Equipment Reset Flow Starts TV|MySpectrum||', tv_equipment_reset_flow_starts,
                      'voice_equipment_reset_flow_starts|Equipment Reset Flow Starts Voice|MySpectrum||', voice_equipment_reset_flow_starts,
                      'all_equipment_reset_flow_successes|Equipment Reset Flow Successes All|MySpectrum||', all_equipment_reset_flow_successes,
                      'internet_equipment_reset_flow_successes|Equipment Reset Flow Successes Internet|MySpectrum||', internet_equipment_reset_flow_successes,
                      'tv_equipment_reset_flow_successes|Equipment Reset Flow Successes TV|MySpectrum||', tv_equipment_reset_flow_successes,
                      'voice_equipment_reset_flow_successes|Equipment Reset Flow Successes Voice|MySpectrum||', voice_equipment_reset_flow_successes,
                      'forgot_password_flow_failure|Forgot Password Flow Failure|MySpectrum||', forgot_password_flow_failure,
                      'forgot_password_flow_start|Forgot Password Flow Start|MySpectrum||', forgot_password_flow_start,
                      'forgot_password_flow_success|Forgot Password Flow Success|MySpectrum||', forgot_password_flow_success,
                      'forgot_username_flow_failure|Forgot Username Flow Failure|MySpectrum||', forgot_username_flow_failure,
                      'forgot_username_flow_start|Forgot Username Flow Start|MySpectrum||', forgot_username_flow_start,
                      'forgot_username_flow_success|Forgot Username Flow Success|MySpectrum||', forgot_username_flow_success,
                      'get_moving_info|Get Moving Info|MySpectrum||', get_moving_info,
                      'install_wifi_profile_start|Install Wifi Profile Starts|MySpectrum||', install_wifi_profile_start,
                      'install_wifi_profile_success|Install Wifi Profile Successes|MySpectrum||', install_wifi_profile_success,
                      'internet_services_connected_experiencing_issues|Internet Services Connected Experiencing Issues|MySpectrum||', internet_services_connected_experiencing_issues,
                      'internet_services_connection_issue_troubleshoot|Internet Services Connection Issue Troubleshoot|MySpectrum||', internet_services_connection_issue_troubleshoot,
                      'login_success|Login Success|MySpectrum||', login_success,
                      'login_failure|Logion Failure|MySpectrum||', login_failure,
                      'manual_reset_failures|Manual Reset Failures|MySpectrum||', manual_reset_failures,
                      'manual_reset_starts|Manual Reset Starts|MySpectrum||', manual_reset_starts,
                      'manual_reset_success|Manual Reset Success|MySpectrum||', manual_reset_success,
                      'internet_modem_reset_failures|Modem Reset Failures Internet|MySpectrum||', internet_modem_reset_failures,
                      'tv_modem_reset_failures|Modem Reset Failures TV|MySpectrum||', tv_modem_reset_failures,
                      'voice_modem_reset_failures|Modem Reset Failures Voice|MySpectrum||', voice_modem_reset_failures,
                      'internet_modem_reset_successes|Modem Reset Successes Internet|MySpectrum||', internet_modem_reset_successes,
                      'tv_modem_reset_successes|Modem Reset Successes TV|MySpectrum||', tv_modem_reset_successes,
                      'voice_modem_reset_successes|Modem Reset Successes Voice|MySpectrum||', voice_modem_reset_successes,
                      'internet_modem_router_reset_starts|Modem Router Reset Starts Internet|MySpectrum||', internet_modem_router_reset_starts,
                      'tv_modem_router_reset_starts|Modem Router Reset Starts TV|MySpectrum||', tv_modem_router_reset_starts,
                      'voice_modem_router_reset_starts|Modem Router Reset Starts Voice|MySpectrum||', voice_modem_router_reset_starts,
                      'moving|Moving|MySpectrum||', moving,
                      'one_time_payment_failure|One Time Payment Failure|MySpectrum||', one_time_payment_failure,
                      'one_time_payment_start|One Time Payment Start|MySpectrum||', one_time_payment_start,
                      'one_time_payment_success|One Time Payment Success|MySpectrum||', one_time_payment_success,
                      'page_views|Page Views|MySpectrum||', page_views,
                      'quick_tutorial|Quick Tutorial|MySpectrum||', quick_tutorial,
                      'rescheduled_appointments|Rescheduled Appointments|MySpectrum||', rescheduled_appointments,
                      'internet_router_reset_failures|Router Reset Failures Internet|MySpectrum||', internet_router_reset_failures,
                      'tv_router_reset_failures|Router Reset Failures TV|MySpectrum||', tv_router_reset_failures,
                      'voice_router_reset_failures|Router Reset Failures Voice|MySpectrum||', voice_router_reset_failures,
                      'internet_router_reset_successes|Router Reset Successes Internet|MySpectrum||', internet_router_reset_successes,
                      'tv_router_reset_successes|Router Reset Successes TV|MySpectrum||', tv_router_reset_successes,
                      'voice_router_reset_successes|Router Reset Successes Voice|MySpectrum||', voice_router_reset_successes,
                      'service_call_failures|Service Call Failures|MySpectrum||', service_call_failures,
                      'services_tab_views|Services Tab Views|MySpectrum||', services_tab_views,
                      'session_start|Session Starts|MySpectrum||', session_start,
                      'site_unique|Site Unique Values|MySpectrum||', site_unique,
                      'support_button|Support Button|MySpectrum||', support_button,
                      'support_page_views|Support Page Views|MySpectrum||', support_page_views,
                      'support_view|Support View|MySpectrum||', support_view,
                      'view_statement|View Statement|MySpectrum||', view_statement,
                      'wifi_password_views|Wifi Password Views|MySpectrum||', wifi_password_views,
                      'os_android|Operating System - Android|MySpectrum||', os_android,
                      'os_ios|Operating System - iOS|MySpectrum||', os_ios,
                      'support_section_page_views|Support Section Page Views|MySpectrum||', support_section_page_views
                  ) as map_column
          FROM ${env:TMP_db}.asp_venona_counts_hourly_counts_instances_columns
        ) as derived_table
    LATERAL VIEW EXPLODE (map_column) exploded_table AS metric, value
;
