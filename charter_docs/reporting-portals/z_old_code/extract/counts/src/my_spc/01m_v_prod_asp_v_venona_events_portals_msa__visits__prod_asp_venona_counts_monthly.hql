set hive.vectorized.execution.enabled = false;

USE prod;

DROP TABLE IF EXISTS ${env:TMP_db}.asp_venona_counts_monthly_counts_visits_columns;
CREATE TEMPORARY TABLE ${env:TMP_db}.asp_venona_counts_monthly_counts_visits_columns AS
    SELECT
        COALESCE (visit__account__details__mso,'Unknown') as company,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('pageView')   AND LOWER(state__view__current_page__page_name) = LOWER('accountHome') , visit__visit_id, Null))) AS account_view_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(operation__operation_type) = LOWER('singleSelectCheckBox')   AND LOWER(state__view__current_page__elements__standardized_name) IN (LOWER('autoPayEnroll'), LOWER('autoPayToggleEnable')) , visit__visit_id, Null))) AS set_up_auto_payment_starts_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('pageView')   AND LOWER(state__view__current_page__page_name) = LOWER('autopaysuccess') , visit__visit_id, Null))) AS set_up_auto_payment_successes_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__elements__standardized_name) IN (LOWER('autoPayToggleEnable'),LOWER('autoPayEnroll')) , visit__visit_id, Null))) AS autopay_enroll_radio_toggle_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('pageView')   AND LOWER(state__view__current_page__page_name) = LOWER('billingHome') , visit__visit_id, Null))) AS bill_pay_view_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('callUsButton') , visit__visit_id, Null))) AS call_support_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('modalView')   AND LOWER(state__view__modal__name) = LOWER('cancelappointment-modal') , visit__visit_id, Null))) AS cancelled_appointments_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('contactSpecialist') , visit__visit_id, Null))) AS contact_our_moving_specialist_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('apiCall')   AND LOWER(application__api__path) = LOWER('/care/api/v1/user/register')   AND application__api__response_code RLIKE '2.*'   AND LOWER(application__api__response_text) NOT IN (LOWER('OK'),NULL) , visit__visit_id, Null))) AS create_username_flow_failure_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_name) = LOWER('login')   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('createusername') , visit__visit_id, Null))) AS create_username_flow_start_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('apiCall')   AND LOWER(application__api__path) = LOWER('/care/api/v1/user/register')   AND application__api__response_code RLIKE '2.*'   AND LOWER(application__api__response_text) IN (LOWER('OK'),NULL) , visit__visit_id, Null))) AS create_username_fow_success_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('pageView')   AND LOWER(state__view__current_page__page_name) = LOWER('dashboardHome') , visit__visit_id, Null))) AS dashboard_view_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_Name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('installWiFiProfile') , visit__visit_id, Null))) AS download_wifi_profile_button_click_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_Name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('pageView')   AND LOWER(state__view__current_page__page_Name) = LOWER('spectrumWiFiProfileResponse') , visit__visit_id, Null))) AS download_wifi_profile_success_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('pageView')   AND LOWER(state__view__current_page__page_name) = LOWER('equipDetails') , visit__visit_id, Null))) AS equipment_detail_views_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('pageView')   AND LOWER(state__view__current_page__page_name) IN (LOWER('internetTab'),LOWER('voiceTab'),LOWER('tvTab')) , visit__visit_id, Null))) AS equipment_list_view_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('modalView')   AND LOWER(state__view__modal__name) IN (LOWER('resetrouterfail-modal'),LOWER('resetmodemfail-modal')) , visit__visit_id, Null))) AS all_equipment_reset_flow_failures_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('modalView')   AND LOWER(state__view__modal__name) IN (LOWER('resetrouterfail-modal'),LOWER('resetmodemfail-modal'))   AND LOWER(state__view__previous_page__page_name) IN (LOWER('internetTab'),LOWER('equipmentHome'))   AND LOWER(state__view__current_page__page_name) <> LOWER('voiceTab') , visit__visit_id, Null))) AS internet_equipment_reset_flow_failures_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('modalView')   AND LOWER(state__view__current_page__page_name) = LOWER('tvTab')   AND LOWER(state__view__modal__name) IN (LOWER('resetrouterfail-modal'),LOWER('resetmodemfail-modal')) , visit__visit_id, Null))) AS tv_equipment_reset_flow_failures_visits,
        SIZE(COLLECT_SET(IF( ((LOWER(visit__application_details__application_name) = LOWER('MySpectrum') AND LOWER(message__name) = LOWER('modalView') AND LOWER(state__view__current_page__page_name) = LOWER('voiceTab') AND LOWER(state__view__modal__name) IN (LOWER('resetrouterfail-modal'),LOWER('resetmodemfail-modal'))) OR (LOWER(visit__application_details__application_name) = LOWER('MySpectrum') AND LOWER(message__name) = LOWER('modalView') AND LOWER(state__view__modal__name) IN (LOWER('resetrouterfail-modal'),LOWER('resetmodemfail-modal')) AND LOWER(state__view__current_page__page_name) = LOWER('voiceTab'))), visit__visit_id, Null))) AS voice_equipment_reset_flow_failures_visits,
        SIZE(COLLECT_SET(IF( ((LOWER(visit__application_details__application_name) = LOWER('MySpectrum') AND LOWER(message__name) = LOWER('selectAction') AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('troubleshoot')) OR (LOWER(visit__application_details__application_name) = LOWER('MySpectrum') AND LOWER(message__name) = LOWER('selectAction') AND LOWER(state__view__current_page__elements__standardized_name) IN (LOWER('internetResetEquip'),LOWER('resetEquip')))), visit__visit_id, Null))) AS all_equipment_reset_flow_starts_visits,
        SIZE(COLLECT_SET(IF( ((LOWER(visit__application_details__application_name) = LOWER('MySpectrum') AND LOWER(message__name) = LOWER('selectAction') AND LOWER(state__view__current_page__page_name) = LOWER('internetTab') AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('troubleshoot')) OR (LOWER(visit__application_details__application_name) = LOWER('MySpectrum') AND LOWER(message__name) = LOWER('selectAction') AND LOWER(state__view__current_page__elements__standardized_name) IN (LOWER('internetResetEquip'),LOWER('resetEquip')) AND LOWER(state__view__current_page__page_name) IN (LOWER('internetTab'),LOWER('equipmentHome')))), visit__visit_id, Null))) AS internet_equipment_reset_flow_starts_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_name) = LOWER('tvTab')   AND LOWER(state__view__current_page__elements__standardized_name) IN(LOWER('troubleshoot'),LOWER('internetConnectIssues'),LOWER('internetTroubleshoot')) , visit__visit_id, Null))) AS tv_equipment_reset_flow_starts_visits,
        SIZE(COLLECT_SET(IF( ((LOWER(visit__application_details__application_name) = LOWER('MySpectrum') AND LOWER(message__name) = LOWER('selectAction') AND LOWER(state__view__current_page__page_name) = LOWER('voiceTab') AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('troubleshoot')) OR (LOWER(visit__application_details__application_name) = LOWER('MySpectrum') AND LOWER(message__name) = LOWER('selectAction') AND LOWER(state__view__current_page__elements__standardized_name) IN (LOWER('internetResetEquip'),LOWER('resetEquip')) AND LOWER(state__view__current_page__page_name) = LOWER('voiceTab'))), visit__visit_id, Null))) AS voice_equipment_reset_flow_starts_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('modalView')   AND LOWER(state__view__modal__name) = LOWER('resetsuccess-modal') , visit__visit_id, Null))) AS all_equipment_reset_flow_successes_visits,
        SIZE(COLLECT_SET(IF( ((LOWER(visit__application_details__application_name) = LOWER('MySpectrum') AND LOWER(message__name) = LOWER('modalView') AND LOWER(state__view__modal__name) = LOWER('internetresetsuccess-modal') AND LOWER(state__view__previous_page__page_name) IN (LOWER('internetTab'),LOWER('equipmentHome')) AND LOWER(state__view__current_page__page_name) <> LOWER('voiceTab')) OR (LOWER(visit__application_details__application_name) = LOWER('MySpectrum') AND LOWER(message__name) = LOWER('modalView') AND LOWER(state__view__modal__name) = LOWER('resetsuccess-modal') AND LOWER(state__view__current_page__page_name) <> LOWER('voiceTab')) OR (LOWER(visit__application_details__application_name) = LOWER('MySpectrum') AND LOWER(message__name) = LOWER('modalView') AND LOWER(state__view__modal__name) = LOWER('resetsuccess-modal') AND LOWER(state__view__previous_page__page_name) IN (LOWER('internetTab'),LOWER('equipmentHome')) AND LOWER(state__view__current_page__page_name) <> LOWER('voiceTab'))), visit__visit_id, Null))) AS internet_equipment_reset_flow_successes_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('modalView')   AND LOWER(state__view__current_page__page_name) = LOWER('tvTab')   AND LOWER(state__view__modal__name) = LOWER('internetresetsuccess-modal') , visit__visit_id, Null))) AS tv_equipment_reset_flow_successes_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('modalView')   AND LOWER(state__view__modal__name) = LOWER('resetsuccess-modal')   AND LOWER(state__view__current_page__page_name) = LOWER('voiceTab') , visit__visit_id, Null))) AS voice_equipment_reset_flow_successes_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('apiCall')   AND LOWER(application__api__path) = LOWER('/care/api/v1/forgotpwd3')   AND application__api__response_code RLIKE '2.*'   AND LOWER(application__api__response_text) NOT IN (LOWER('OK'),NULL) , visit__visit_id, Null))) AS forgot_password_flow_failure_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('selectaction')   AND LOWER(state__view__current_page__page_name) = LOWER('login')   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('forgotpassword') , visit__visit_id, Null))) AS forgot_password_flow_start_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('modalView')   AND LOWER(state__view__modal__name) = LOWER('newpasswordresponse-modal')   AND LOWER(state__view__current_page__page_name) = LOWER('newPasswordEntry') , visit__visit_id, Null))) AS forgot_password_flow_success_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('apiCall')   AND LOWER(application__api__path) = LOWER('/care/api/v1/forgotusername')   AND application__api__response_code RLIKE '2.*'   AND LOWER(application__api__response_text) NOT IN (LOWER('OK'),NULL) , visit__visit_id, Null))) AS forgot_username_flow_failure_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_name) = LOWER('login')   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('forgotUsername') , visit__visit_id, Null))) AS forgot_username_flow_start_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('loginRecoverySendEmail') , visit__visit_id, Null))) AS forgot_username_flow_success_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('getMoveInfo') , visit__visit_id, Null))) AS get_moving_info_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(state__view__current_page__page_name) = LOWER('accountHome')   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('wifiProfile') , visit__visit_id, Null))) AS install_wifi_profile_start_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(state__view__current_page__page_name) = LOWER('spectrumWiFiProfileResponse') , visit__visit_id, Null))) AS install_wifi_profile_success_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('selectAction')    AND LOWER(state__view__current_page__elements__standardized_name) IN (LOWER('internetConnectIssues'),LOWER('internetTroubleshoot')) , visit__visit_id, Null))) AS internet_services_connected_experiencing_issues_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('selectAction')    AND LOWER(state__view__current_page__elements__standardized_name) IN (LOWER('internetConnectIssues'),LOWER('internetTroubleshoot')) , visit__visit_id, Null))) AS internet_services_connection_issue_troubleshoot_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('loginStop') , visit__visit_id, Null))) AS login_success_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('loginStop') , visit__visit_id, Null))) AS login_failure_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_name) = LOWER('manualReset')   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('manualResetContinue') , visit__visit_id, Null))) AS manual_reset_failures_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('pageView')   AND LOWER(state__view__current_page__page_name) = LOWER('manualReset') , visit__visit_id, Null))) AS manual_reset_starts_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_name) IN (LOWER('internetTSManualReset'),LOWER('internetManualReset'))   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('internetManualResetResolved') , visit__visit_id, Null))) AS manual_reset_success_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('modalView')   AND LOWER(state__view__current_page__page_name) = LOWER('internetTab')   AND LOWER(state__view__modal__name) = LOWER('resetmodemfail-modal') , visit__visit_id, Null))) AS internet_modem_reset_failures_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('modalView')   AND LOWER(state__view__current_page__page_name) = LOWER('tvTab')   AND LOWER(state__view__modal__name) = LOWER('resetmodemfail-modal') , visit__visit_id, Null))) AS tv_modem_reset_failures_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('modalView')   AND LOWER(state__view__current_page__page_name) = LOWER('voiceTab')   AND LOWER(state__view__modal__name) = LOWER('resetmodemfail-modal') , visit__visit_id, Null))) AS voice_modem_reset_failures_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('apiCall')   AND LOWER(state__view__current_page__page_name) = LOWER('internetResettingEquip')   AND LOWER(application__api__api_name) = LOWER('equipmentV1ModemReset')   AND LOWER(state__view__previous_page__page_name) IN (LOWER('equipmentHome'),LOWER('internetTab'))   AND (application__api__response_code RLIKE '2.*' OR application__api__response_code = '429') , visit__visit_id, Null))) AS internet_modem_reset_successes_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('apiCall')   AND LOWER(state__view__current_page__page_name) = LOWER('internetResettingEquip')   AND LOWER(application__api__api_name) = LOWER('equipmentV1ModemReset')   AND LOWER(state__view__previous_page__page_name) IN (LOWER('tvTab'))   AND (application__api__response_code RLIKE '2.*' OR application__api__response_code = '429') , visit__visit_id, Null))) AS tv_modem_reset_successes_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('modalView')   AND LOWER(state__view__modal__name) = LOWER('internetresetsuccess-modal')   AND LOWER(state__view__current_page__page_name) = LOWER('voiceTab') , visit__visit_id, Null))) AS voice_modem_reset_successes_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('internetResetEquip')   AND state__view__current_page__page_name IN (LOWER('equipmentHome'),LOWER('internetTab')) , visit__visit_id, Null))) AS internet_modem_router_reset_starts_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('internetResetEquip')   AND state__view__current_page__page_name IN (LOWER('tvTab')) , visit__visit_id, Null))) AS tv_modem_router_reset_starts_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('internetResetEquip')   AND LOWER(state__view__current_page__page_name) IN (LOWER('voiceTab')) , visit__visit_id, Null))) AS voice_modem_router_reset_starts_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('movingButton') , visit__visit_id, Null))) AS moving_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('pageView')   AND LOWER(state__view__current_page__page_name) IN (LOWER('payUnsuccess'),LOWER('payUnsuccessAutoPay')) , visit__visit_id, Null))) AS one_time_payment_failure_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('makePaymentButton') , visit__visit_id, Null))) AS one_time_payment_start_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('pageView')   AND LOWER(state__view__current_page__page_name) IN (LOWER('paySuccess'),LOWER('paySuccessAutoPay')) , visit__visit_id, Null))) AS one_time_payment_success_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('pageView') , visit__visit_id, Null))) AS page_views_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('appTutorialButton') , visit__visit_id, Null))) AS quick_tutorial_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('apiCall')   AND LOWER(application__api__path) = LOWER('/care/api/v1/appointment/reschedule')    AND application__api__response_code RLIKE '2.*' , visit__visit_id, Null))) AS rescheduled_appointments_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('modalView')   AND LOWER(state__view__modal__name) = LOWER('resetrouterfail-modal')   AND LOWER(state__view__previous_page__page_name) IN (LOWER('internetTab'),LOWER('equipmentHome'))   AND LOWER(state__view__current_page__page_name) <> LOWER('voiceTab') , visit__visit_id, Null))) AS internet_router_reset_failures_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('modalView')   AND LOWER(state__view__current_page__page_name) = LOWER('tvTab')   AND LOWER(state__view__modal__name) = LOWER('resetrouterfail-modal') , visit__visit_id, Null))) AS tv_router_reset_failures_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('modalView')   AND LOWER(state__view__current_page__page_name) = LOWER('voiceTab')   AND LOWER(state__view__modal__name) = LOWER('resetrouterfail-modal') , visit__visit_id, Null))) AS voice_router_reset_failures_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('apiCall')   AND LOWER(state__view__current_page__page_name) = LOWER('internetResettingEquip')   AND LOWER(application__api__api_name) = LOWER('equipmentV1RouterReset')   AND LOWER(state__view__previous_page__page_name) IN (LOWER('equipmentHome'),LOWER('internetTab'))   AND (application__api__response_code RLIKE '2.*' OR application__api__response_code = '429') , visit__visit_id, Null))) AS internet_router_reset_successes_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('apiCall')   AND LOWER(state__view__current_page__page_name) = LOWER('internetResettingEquip')   AND LOWER(application__api__api_name) = LOWER('equipmentV1RouterReset')   AND LOWER(state__view__previous_page__page_name) IN (LOWER('tvTab'))   AND (application__api__response_code RLIKE '2.*' OR application__api__response_code = '429') , visit__visit_id, Null))) AS tv_router_reset_successes_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('apiCall')   AND LOWER(state__view__current_page__page_name) = LOWER('internetResettingEquip')   AND LOWER(application__api__api_name) = LOWER('equipmentV1RouterReset')   AND LOWER(state__view__previous_page__page_name) IN (LOWER('voiceTab'))   AND (application__api__response_code RLIKE '2.*' OR application__api__response_code = '429') , visit__visit_id, Null))) AS voice_router_reset_successes_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('apiCall')   AND LOWER(application__api__response_code) NOT RLIKE '2.*'   AND application__api__response_code <> '429' , visit__visit_id, Null))) AS service_call_failures_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('pageView')   AND LOWER(state__view__current_page__page_name) IN (LOWER('internetTab'),LOWER('voiceTab'),LOWER('tvTab')) , visit__visit_id, Null))) AS services_tab_views_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('startSession') , visit__visit_id, Null))) AS session_start_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum'), visit__visit_id, Null))) AS site_unique_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('supportHome') , visit__visit_id, Null))) AS support_button_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('pageView')   AND LOWER(state__view__current_page__app_section) = LOWER('support') , visit__visit_id, Null))) AS support_page_views_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('pageView')   AND LOWER(state__view__current_page__page_name) = LOWER('supportHome') , visit__visit_id, Null))) AS support_view_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('pageView')   AND LOWER(state__view__current_page__page_name) = LOWER('statementDetail') , visit__visit_id, Null))) AS view_statement_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('internetShowPassword') , visit__visit_id, Null))) AS wifi_password_views_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(visit__device__device_type) RLIKE LOWER('and.*') , visit__visit_id, Null))) AS os_android_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(visit__device__device_type) RLIKE LOWER('ip.*') , visit__visit_id, Null))) AS os_ios_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')  AND LOWER(message__name) = LOWER('pageView')   AND LOWER(state__view__current_page__app_section) = LOWER('support') , visit__visit_id, Null))) AS support_section_page_views_visits,
        'asp' AS platform,
        'app' AS domain,
        
        date_yearmonth(epoch_converter(cast(received__timestamp as bigint),'America/Denver')) as year_month_denver
    FROM asp_v_venona_events_portals_msa
         
    WHERE (partition_date_hour_utc >= '${env:START_DATE_TZ}'
       AND partition_date_hour_utc <  '${env:END_DATE_TZ}')
    GROUP BY
        date_yearmonth(epoch_converter(cast(received__timestamp as bigint),'America/Denver')),
        
        COALESCE (visit__account__details__mso,'Unknown')
    ;
INSERT OVERWRITE TABLE prod.asp_venona_counts_monthly
PARTITION(unit,platform,domain,company,year_month_denver,source_table)

    SELECT  value,
            metric,
            
            'visits',
            'asp',
            'app',
            company,
            year_month_denver,
            'asp_v_venona_events_portals_msa'
    FROM (SELECT  company,
                  year_month_denver,
                  
                  MAP(

                      'account_view|Account View|MySpectrum||', account_view_visits,
                      'set_up_auto_payment_starts|Auto Pay Enrollment Starts|MySpectrum||', set_up_auto_payment_starts_visits,
                      'set_up_auto_payment_successes|Auto Pay Enrollment Successes|MySpectrum||', set_up_auto_payment_successes_visits,
                      'autopay_enroll_radio_toggle|AutoPay Enroll Radio Toggle|MySpectrum||', autopay_enroll_radio_toggle_visits,
                      'bill_pay_view|Bill Pay View|MySpectrum||', bill_pay_view_visits,
                      'call_support|Call Support|MySpectrum||', call_support_visits,
                      'cancelled_appointments|Cancelled Appointments|MySpectrum||', cancelled_appointments_visits,
                      'contact_our_moving_specialist|Contact Our Moving Specialist|MySpectrum||', contact_our_moving_specialist_visits,
                      'create_username_flow_failure|Create Username Flow Failure|MySpectrum||', create_username_flow_failure_visits,
                      'create_username_flow_start|Create Username Flow Start|MySpectrum||', create_username_flow_start_visits,
                      'create_username_fow_success|Create Username Flow Success|MySpectrum||', create_username_fow_success_visits,
                      'dashboard_view|Dashboard View|MySpectrum||', dashboard_view_visits,
                      'download_wifi_profile_button_click|Download WiFi Profile Button Click|MySpectrum||', download_wifi_profile_button_click_visits,
                      'download_wifi_profile_success|Download WiFi Profile Success|MySpectrum||', download_wifi_profile_success_visits,
                      'equipment_detail_views|Equipment Details Views|MySpectrum||', equipment_detail_views_visits,
                      'equipment_list_view|Equipment List View|MySpectrum||', equipment_list_view_visits,
                      'all_equipment_reset_flow_failures|Equipment Reset Flow Failures All|MySpectrum||', all_equipment_reset_flow_failures_visits,
                      'internet_equipment_reset_flow_failures|Equipment Reset Flow Failures Internet|MySpectrum||', internet_equipment_reset_flow_failures_visits,
                      'tv_equipment_reset_flow_failures|Equipment Reset Flow Failures TV|MySpectrum||', tv_equipment_reset_flow_failures_visits,
                      'voice_equipment_reset_flow_failures|Equipment Reset Flow Failures Voice|MySpectrum||', voice_equipment_reset_flow_failures_visits,
                      'all_equipment_reset_flow_starts|Equipment Reset Flow Starts All|MySpectrum||', all_equipment_reset_flow_starts_visits,
                      'internet_equipment_reset_flow_starts|Equipment Reset Flow Starts Internet|MySpectrum||', internet_equipment_reset_flow_starts_visits,
                      'tv_equipment_reset_flow_starts|Equipment Reset Flow Starts TV|MySpectrum||', tv_equipment_reset_flow_starts_visits,
                      'voice_equipment_reset_flow_starts|Equipment Reset Flow Starts Voice|MySpectrum||', voice_equipment_reset_flow_starts_visits,
                      'all_equipment_reset_flow_successes|Equipment Reset Flow Successes All|MySpectrum||', all_equipment_reset_flow_successes_visits,
                      'internet_equipment_reset_flow_successes|Equipment Reset Flow Successes Internet|MySpectrum||', internet_equipment_reset_flow_successes_visits,
                      'tv_equipment_reset_flow_successes|Equipment Reset Flow Successes TV|MySpectrum||', tv_equipment_reset_flow_successes_visits,
                      'voice_equipment_reset_flow_successes|Equipment Reset Flow Successes Voice|MySpectrum||', voice_equipment_reset_flow_successes_visits,
                      'forgot_password_flow_failure|Forgot Password Flow Failure|MySpectrum||', forgot_password_flow_failure_visits,
                      'forgot_password_flow_start|Forgot Password Flow Start|MySpectrum||', forgot_password_flow_start_visits,
                      'forgot_password_flow_success|Forgot Password Flow Success|MySpectrum||', forgot_password_flow_success_visits,
                      'forgot_username_flow_failure|Forgot Username Flow Failure|MySpectrum||', forgot_username_flow_failure_visits,
                      'forgot_username_flow_start|Forgot Username Flow Start|MySpectrum||', forgot_username_flow_start_visits,
                      'forgot_username_flow_success|Forgot Username Flow Success|MySpectrum||', forgot_username_flow_success_visits,
                      'get_moving_info|Get Moving Info|MySpectrum||', get_moving_info_visits,
                      'install_wifi_profile_start|Install Wifi Profile Starts|MySpectrum||', install_wifi_profile_start_visits,
                      'install_wifi_profile_success|Install Wifi Profile Successes|MySpectrum||', install_wifi_profile_success_visits,
                      'internet_services_connected_experiencing_issues|Internet Services Connected Experiencing Issues|MySpectrum||', internet_services_connected_experiencing_issues_visits,
                      'internet_services_connection_issue_troubleshoot|Internet Services Connection Issue Troubleshoot|MySpectrum||', internet_services_connection_issue_troubleshoot_visits,
                      'login_success|Login Success|MySpectrum||', login_success_visits,
                      'login_failure|Logion Failure|MySpectrum||', login_failure_visits,
                      'manual_reset_failures|Manual Reset Failures|MySpectrum||', manual_reset_failures_visits,
                      'manual_reset_starts|Manual Reset Starts|MySpectrum||', manual_reset_starts_visits,
                      'manual_reset_success|Manual Reset Success|MySpectrum||', manual_reset_success_visits,
                      'internet_modem_reset_failures|Modem Reset Failures Internet|MySpectrum||', internet_modem_reset_failures_visits,
                      'tv_modem_reset_failures|Modem Reset Failures TV|MySpectrum||', tv_modem_reset_failures_visits,
                      'voice_modem_reset_failures|Modem Reset Failures Voice|MySpectrum||', voice_modem_reset_failures_visits,
                      'internet_modem_reset_successes|Modem Reset Successes Internet|MySpectrum||', internet_modem_reset_successes_visits,
                      'tv_modem_reset_successes|Modem Reset Successes TV|MySpectrum||', tv_modem_reset_successes_visits,
                      'voice_modem_reset_successes|Modem Reset Successes Voice|MySpectrum||', voice_modem_reset_successes_visits,
                      'internet_modem_router_reset_starts|Modem Router Reset Starts Internet|MySpectrum||', internet_modem_router_reset_starts_visits,
                      'tv_modem_router_reset_starts|Modem Router Reset Starts TV|MySpectrum||', tv_modem_router_reset_starts_visits,
                      'voice_modem_router_reset_starts|Modem Router Reset Starts Voice|MySpectrum||', voice_modem_router_reset_starts_visits,
                      'moving|Moving|MySpectrum||', moving_visits,
                      'one_time_payment_failure|One Time Payment Failure|MySpectrum||', one_time_payment_failure_visits,
                      'one_time_payment_start|One Time Payment Start|MySpectrum||', one_time_payment_start_visits,
                      'one_time_payment_success|One Time Payment Success|MySpectrum||', one_time_payment_success_visits,
                      'page_views|Page Views|MySpectrum||', page_views_visits,
                      'quick_tutorial|Quick Tutorial|MySpectrum||', quick_tutorial_visits,
                      'rescheduled_appointments|Rescheduled Appointments|MySpectrum||', rescheduled_appointments_visits,
                      'internet_router_reset_failures|Router Reset Failures Internet|MySpectrum||', internet_router_reset_failures_visits,
                      'tv_router_reset_failures|Router Reset Failures TV|MySpectrum||', tv_router_reset_failures_visits,
                      'voice_router_reset_failures|Router Reset Failures Voice|MySpectrum||', voice_router_reset_failures_visits,
                      'internet_router_reset_successes|Router Reset Successes Internet|MySpectrum||', internet_router_reset_successes_visits,
                      'tv_router_reset_successes|Router Reset Successes TV|MySpectrum||', tv_router_reset_successes_visits,
                      'voice_router_reset_successes|Router Reset Successes Voice|MySpectrum||', voice_router_reset_successes_visits,
                      'service_call_failures|Service Call Failures|MySpectrum||', service_call_failures_visits,
                      'services_tab_views|Services Tab Views|MySpectrum||', services_tab_views_visits,
                      'session_start|Session Starts|MySpectrum||', session_start_visits,
                      'site_unique|Site Unique Values|MySpectrum||', site_unique_visits,
                      'support_button|Support Button|MySpectrum||', support_button_visits,
                      'support_page_views|Support Page Views|MySpectrum||', support_page_views_visits,
                      'support_view|Support View|MySpectrum||', support_view_visits,
                      'view_statement|View Statement|MySpectrum||', view_statement_visits,
                      'wifi_password_views|Wifi Password Views|MySpectrum||', wifi_password_views_visits,
                      'os_android|Operating System - Android|MySpectrum||', os_android_visits,
                      'os_ios|Operating System - iOS|MySpectrum||', os_ios_visits,
                      'support_section_page_views|Support Section Page Views|MySpectrum||', support_section_page_views_visits
                  ) as map_column
          FROM ${env:TMP_db}.asp_venona_counts_monthly_counts_visits_columns
        ) as derived_table
    LATERAL VIEW EXPLODE (map_column) exploded_table AS metric, value
;
