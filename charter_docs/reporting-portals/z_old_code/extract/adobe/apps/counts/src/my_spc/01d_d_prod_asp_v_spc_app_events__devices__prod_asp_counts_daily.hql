set hive.vectorized.execution.enabled = false;

USE prod;

DROP TABLE IF EXISTS ${env:TMP_db}.asp_counts_daily_counts_devices_columns;
CREATE TEMPORARY TABLE ${env:TMP_db}.asp_counts_daily_counts_devices_columns AS
    SELECT
        CASE
            WHEN state__view__previous_page__sub_section = 'BH' THEN 'BHN'
            WHEN state__view__previous_page__sub_section = 'CHARTER' THEN 'CHTR'
            WHEN state__view__previous_page__sub_section = 'TWC' THEN 'TWCC'
            WHEN state__view__previous_page__sub_section IS NULL THEN 'UNAUTH'
            ELSE 'UNDEFINED'
        END as company,
        SIZE(COLLECT_SET(IF( message__name IN ('Account View'), visit__device__enc_uuid, Null))) AS account_view_devices,
        SIZE(COLLECT_SET(IF( message__name RLIKE 'AMACTION:AddAutoPay Svc Call Failure-.*'  AND message__category = 'Custom Link' , visit__device__enc_uuid, Null))) AS autopay_enroll_failure_devices,
        SIZE(COLLECT_SET(IF( message__name IN ('AMACTION:Payment View Auto Pay Enabled Trigger'), visit__device__enc_uuid, Null))) AS autopay_enroll_radio_toggle_devices,
        SIZE(COLLECT_SET(IF( message__name IN ('AMACTION:AddAutoPay Svc Call Success'), visit__device__enc_uuid, Null))) AS autopay_enroll_success_devices,
        SIZE(COLLECT_SET(IF( message__name IN ('Bill Pay View'), visit__device__enc_uuid, Null))) AS bill_pay_view_devices,
        SIZE(COLLECT_SET(IF( message__name IN ('AMACTION:Call Support Trigger'), visit__device__enc_uuid, Null))) AS call_support_devices,
        SIZE(COLLECT_SET(IF( message__name IN ('AMACTION:CancelAppointment Svc Call Success'), visit__device__enc_uuid, Null))) AS cancelled_appointments_devices,
        SIZE(COLLECT_SET(IF( visit__application_details__referrer_link RLIKE '.*200.*', visit__device__enc_uuid, Null))) AS crashes_devices,
        SIZE(COLLECT_SET(IF( visit__application_details__app_version RLIKE 'Android.*'  AND visit__application_details__referrer_link RLIKE '.*200.*' , visit__device__enc_uuid, Null))) AS crashes_android_devices,
        SIZE(COLLECT_SET(IF( visit__application_details__app_version RLIKE 'iOS.*'  AND visit__application_details__referrer_link RLIKE '.*200.*' , visit__device__enc_uuid, Null))) AS crashes_ios_devices,
        SIZE(COLLECT_SET(IF( message__name RLIKE 'AMACTION:Register Svc Call Failure-.*', visit__device__enc_uuid, Null))) AS create_username_flow_failure_devices,
        SIZE(COLLECT_SET(IF( message__name IN ('AMACTION:app.analytics.tracking.Login.Action.RegisterAccount')  AND message__category = 'Custom Link' , visit__device__enc_uuid, Null))) AS create_username_flow_start_devices,
        SIZE(COLLECT_SET(IF( message__name IN ('AMACTION:Register Svc Call Success'), visit__device__enc_uuid, Null))) AS create_username_fow_success_devices,
        SIZE(COLLECT_SET(IF( message__name IN ('Dashboard View'), visit__device__enc_uuid, Null))) AS dashboard_view_devices,
        SIZE(COLLECT_SET(IF( message__name IN ('Internet Equipment Details View','Voice Equipment Details View')  AND message__category = 'Page View' , visit__device__enc_uuid, Null))) AS equipment_detail_views_devices,
        SIZE(COLLECT_SET(IF( message__name IN ('Equipment List View'), visit__device__enc_uuid, Null))) AS equipment_list_view_devices,
        SIZE(COLLECT_SET(IF( message__name IN ('Equipment Internet Reset Failure View','AMACTION:Troubleshoot ProblemNotSolved Trigger','Equipment Voice Reset Failure View','Equipment TV Reset Failure View'), visit__device__enc_uuid, Null))) AS all_equipment_reset_flow_failures_devices,
        SIZE(COLLECT_SET(IF( message__name IN ('Equipment Internet Reset Failure View'), visit__device__enc_uuid, Null))) AS internet_equipment_reset_flow_failures_devices,
        SIZE(COLLECT_SET(IF( message__name IN ('Equipment TV Reset Failure View'), visit__device__enc_uuid, Null))) AS tv_equipment_reset_flow_failures_devices,
        SIZE(COLLECT_SET(IF( message__name IN ('Equipment Voice Reset Failure View'), visit__device__enc_uuid, Null))) AS voice_equipment_reset_flow_failures_devices,
        SIZE(COLLECT_SET(IF( message__name IN ('AMACTION:Troubleshoot Device AutoTroubleshoot Trigger','AMACTION:Equipment Internet Reset Equipment Trigger','AMACTION:Equipment Voice Reset Equipment Trigger','AMACTION:Equipment TV Reset Equipment Trigger'), visit__device__enc_uuid, Null))) AS all_equipment_reset_flow_starts_devices,
        SIZE(COLLECT_SET(IF( message__name IN ('AMACTION:Equipment Internet Reset Equipment Trigger'), visit__device__enc_uuid, Null))) AS internet_equipment_reset_flow_starts_devices,
        SIZE(COLLECT_SET(IF( message__name IN ('AMACTION:Equipment TV Reset Equipment Trigger'), visit__device__enc_uuid, Null))) AS tv_equipment_reset_flow_starts_devices,
        SIZE(COLLECT_SET(IF( message__name IN ('AMACTION:Equipment Voice Reset Equipment Trigger'), visit__device__enc_uuid, Null))) AS voice_equipment_reset_flow_starts_devices,
        SIZE(COLLECT_SET(IF( message__name IN ('Equipment Internet Reset Success View','AMACTION:Troubleshoot ProblemSolved Trigger','Equipment Voice Reset Success View','Equipment TV Reset Success View'), visit__device__enc_uuid, Null))) AS all_equipment_reset_flow_successes_devices,
        SIZE(COLLECT_SET(IF( message__name IN ('Equipment Internet Reset Success View'), visit__device__enc_uuid, Null))) AS internet_equipment_reset_flow_successes_devices,
        SIZE(COLLECT_SET(IF( message__name IN ('Equipment TV Reset Success View'), visit__device__enc_uuid, Null))) AS tv_equipment_reset_flow_successes_devices,
        SIZE(COLLECT_SET(IF( message__name IN ('Equipment Voice Reset Success View'), visit__device__enc_uuid, Null))) AS voice_equipment_reset_flow_successes_devices,
        SIZE(COLLECT_SET(IF( message__name RLIKE 'AMACTION:ForgotPasswordStep3 Svc Call Failure.*', visit__device__enc_uuid, Null))) AS forgot_password_flow_failure_devices,
        SIZE(COLLECT_SET(IF( message__name IN ('AMACTION:Forgot Password')  AND message__category = 'Custom Link' , visit__device__enc_uuid, Null))) AS forgot_password_flow_start_devices,
        SIZE(COLLECT_SET(IF( message__name IN ('AMACTION:ForgotPasswordStep3 Svc Call Success'), visit__device__enc_uuid, Null))) AS forgot_password_flow_success_devices,
        SIZE(COLLECT_SET(IF( message__name RLIKE 'AMACTION:ForgotUsername Svc Call Failure.*', visit__device__enc_uuid, Null))) AS forgot_username_flow_failure_devices,
        SIZE(COLLECT_SET(IF( message__name IN ('AMACTION:Forgot Username'), visit__device__enc_uuid, Null))) AS forgot_username_flow_start_devices,
        SIZE(COLLECT_SET(IF( message__name IN ('AMACTION:ForgotUsername Svc Call Success'), visit__device__enc_uuid, Null))) AS forgot_username_flow_success_devices,
        SIZE(COLLECT_SET(IF( message__name IN ('AMACTION:Internet Services Connected Experiencing Issues')  AND message__category = 'Custom Link' , visit__device__enc_uuid, Null))) AS internet_services_connected_experiencing_issues_devices,
        SIZE(COLLECT_SET(IF( message__name IN ('AMACTION:Internet Services ConnectionIssue Troubleshoot')  AND message__category = 'Custom Link' , visit__device__enc_uuid, Null))) AS internet_services_connection_issue_troubleshoot_devices,
        SIZE(COLLECT_SET(IF( visit__application_details__referrer_link RLIKE '.*201.*', visit__device__enc_uuid, Null))) AS launches_devices,
        SIZE(COLLECT_SET(IF( visit__application_details__app_version RLIKE 'Android.*'  AND visit__application_details__referrer_link RLIKE '.*201.*' , visit__device__enc_uuid, Null))) AS launches_android_devices,
        SIZE(COLLECT_SET(IF( visit__application_details__app_version RLIKE 'iOS.*'  AND visit__application_details__referrer_link RLIKE '.*201.*' , visit__device__enc_uuid, Null))) AS launches_ios_devices,
        SIZE(COLLECT_SET(IF( message__name IN ('AMACTION:Troubleshoot ProblemNotSolved Trigger'), visit__device__enc_uuid, Null))) AS legacy_modem_router_reset_flow_failure_devices,
        SIZE(COLLECT_SET(IF( message__name IN ('AMACTION:Troubleshoot Device AutoTroubleshoot Trigger'), visit__device__enc_uuid, Null))) AS legacy_modem_router_reset_flow_start_devices,
        SIZE(COLLECT_SET(IF( message__name IN ('AMACTION:Troubleshoot ProblemSolved Trigger'), visit__device__enc_uuid, Null))) AS legacy_modem_router_reset_flow_success_devices,
        SIZE(COLLECT_SET(IF( message__name IN ('Equipment Internet Manual Troubleshoot Article Continue'), visit__device__enc_uuid, Null))) AS manual_reset_failures_devices,
        SIZE(COLLECT_SET(IF( message__name IN ('AMACTION:Troubleshoot Device ManualTroubleshoot Trigger'), visit__device__enc_uuid, Null))) AS manual_reset_starts_devices,
        SIZE(COLLECT_SET(IF( message__name IN ('AMACTION:Equipment Manual Reset Svc Call Success'), visit__device__enc_uuid, Null))) AS manual_reset_success_devices,
        SIZE(COLLECT_SET(IF( message__name IN ('AMACTION:Equipment Internet reboot Modem failure')  AND message__category = 'Custom Link' , visit__device__enc_uuid, Null))) AS internet_modem_reset_failures_devices,
        SIZE(COLLECT_SET(IF( message__name IN ('AMACTION:Equipment TV reboot Modem failure')  AND message__category = 'Custom Link' , visit__device__enc_uuid, Null))) AS tv_modem_reset_failures_devices,
        SIZE(COLLECT_SET(IF( message__name IN ('AMACTION:Equipment Voice reboot Modem failure')  AND message__category = 'Custom Link' , visit__device__enc_uuid, Null))) AS voice_modem_reset_failures_devices,
        SIZE(COLLECT_SET(IF( message__name IN ('AMACTION:Equipment Internet reboot Modem success')  AND message__category = 'Custom Link' , visit__device__enc_uuid, Null))) AS internet_modem_reset_successes_devices,
        SIZE(COLLECT_SET(IF( message__name IN ('AMACTION:Equipment TV reboot Modem success')  AND message__category = 'Custom Link' , visit__device__enc_uuid, Null))) AS tv_modem_reset_successes_devices,
        SIZE(COLLECT_SET(IF( message__name IN ('AMACTION:Equipment Voice reboot Modem success')  AND message__category = 'Custom Link' , visit__device__enc_uuid, Null))) AS voice_modem_reset_successes_devices,
        SIZE(COLLECT_SET(IF( message__name IN ('AMACTION:Equipment Internet Reset Equipment Trigger')  AND message__category = 'Custom Link' , visit__device__enc_uuid, Null))) AS internet_modem_router_reset_starts_devices,
        SIZE(COLLECT_SET(IF( message__name IN ('AMACTION:Equipment TV Reset Equipment Trigger')  AND message__category = 'Custom Link' , visit__device__enc_uuid, Null))) AS tv_modem_router_reset_starts_devices,
        SIZE(COLLECT_SET(IF( message__name IN ('AMACTION:Equipment Voice Reset Equipment Trigger')  AND message__category = 'Custom Link' , visit__device__enc_uuid, Null))) AS voice_modem_router_reset_starts_devices,
        SIZE(COLLECT_SET(IF( message__name RLIKE 'AMACTION:MakeOneTimePayment Svc Call Failure-.*'  AND message__category = 'Custom Link' , visit__device__enc_uuid, Null))) AS one_time_payment_failure_devices,
        SIZE(COLLECT_SET(IF( message__name IN ('AMACTION:Dashboard Make Payment Button Trigger','AMACTION:Make Payment From BillPayTab'), visit__device__enc_uuid, Null))) AS one_time_payment_start_devices,
        SIZE(COLLECT_SET(IF( message__name IN ('AMACTION:Paid with new Payment Method','AMACTION:Paid with an existing Payment'), visit__device__enc_uuid, Null))) AS one_time_payment_success_devices,
        SIZE(COLLECT_SET(IF( message__category = 'Page View', visit__device__enc_uuid, Null))) AS page_views_devices,
        SIZE(COLLECT_SET(IF( message__name IN ('AMACTION:RefreshEquipment Svc Call Success'), visit__device__enc_uuid, Null))) AS refresh_digital_receiver_requests_devices,
        SIZE(COLLECT_SET(IF( message__name IN ('AMACTION:RescheduleAppointment Svc Call Success'), visit__device__enc_uuid, Null))) AS rescheduled_appointments_devices,
        SIZE(COLLECT_SET(IF( message__name IN ('AMACTION:Equipment Internet reboot Router failure')  AND message__category = 'Custom Link' , visit__device__enc_uuid, Null))) AS internet_router_reset_failures_devices,
        SIZE(COLLECT_SET(IF( message__name IN ('AMACTION:Equipment TV reboot Router failure')  AND message__category = 'Custom Link' , visit__device__enc_uuid, Null))) AS tv_router_reset_failures_devices,
        SIZE(COLLECT_SET(IF( message__name IN ('AMACTION:Equipment Voice reboot Router failure')  AND message__category = 'Custom Link' , visit__device__enc_uuid, Null))) AS voice_router_reset_failures_devices,
        SIZE(COLLECT_SET(IF( message__name IN ('AMACTION:Equipment Internet reboot Router success')  AND message__category = 'Custom Link' , visit__device__enc_uuid, Null))) AS internet_router_reset_successes_devices,
        SIZE(COLLECT_SET(IF( message__name IN ('AMACTION:Equipment TV reboot Router success')  AND message__category = 'Custom Link' , visit__device__enc_uuid, Null))) AS tv_router_reset_successes_devices,
        SIZE(COLLECT_SET(IF( message__name IN ('AMACTION:Equipment Voice reboot Router success')  AND message__category = 'Custom Link' , visit__device__enc_uuid, Null))) AS voice_router_reset_successes_devices,
        SIZE(COLLECT_SET(IF( message__name RLIKE '.*Svc Call Failure.*'  AND message__category = 'Custom Link' , visit__device__enc_uuid, Null))) AS service_call_failures_devices,
        SIZE(COLLECT_SET(IF( message__name IN ('Internet Services','Voice Services','TV Services')  AND message__category = 'Page View' , visit__device__enc_uuid, Null))) AS services_tab_views_devices,
        SIZE(COLLECT_SET(IF( TRUE, visit__device__enc_uuid, Null))) AS site_unique_devices,
        SIZE(COLLECT_SET(IF( message__name IN ('Support View','Passpoint Setup View','Tutorial Walkthrough First Use View','Channel Lineups','Moving Form NewAddress View','Program Your Remote','Locations Map','Terms And Conditions','AMACTION:Moving Trigger'), visit__device__enc_uuid, Null))) AS support_page_views_devices,
        SIZE(COLLECT_SET(IF( message__name IN ('Support View'), visit__device__enc_uuid, Null))) AS support_view_devices,
        SIZE(COLLECT_SET(IF( message__name IN ('AMACTION:RefreshEquipment Svc Call Success')  AND message__category = 'Custom Link' , visit__device__enc_uuid, Null))) AS video_refresh_success_devices,
        SIZE(COLLECT_SET(IF( message__name IN ('AMACTION:PDFStatementDownload Svc Call Success', 'AMACTION:PDF Single Statement Svc Call Success'), visit__device__enc_uuid, Null))) AS view_statement_devices,
        SIZE(COLLECT_SET(IF( message__name IN ('AMACTION:Show Router Password')  AND message__category = 'Custom Link' , visit__device__enc_uuid, Null))) AS wifi_password_views_devices,
        'asp' AS platform,
        'app' AS domain,
        
        epoch_converter(cast(message__timestamp * 1000 as bigint),'America/Denver') as date_denver
    FROM asp_v_spc_app_events
         
    WHERE (partition_date_hour_utc >= '${env:START_DATE_TZ}'
       AND partition_date_hour_utc <  '${env:END_DATE_TZ}')
    GROUP BY
        epoch_converter(cast(message__timestamp * 1000 as bigint),'America/Denver'),
        
        CASE
            WHEN state__view__previous_page__sub_section = 'BH' THEN 'BHN'
            WHEN state__view__previous_page__sub_section = 'CHARTER' THEN 'CHTR'
            WHEN state__view__previous_page__sub_section = 'TWC' THEN 'TWCC'
            WHEN state__view__previous_page__sub_section IS NULL THEN 'UNAUTH'
            ELSE 'UNDEFINED'
        END
    ;
INSERT OVERWRITE TABLE prod.asp_counts_daily
PARTITION(unit,platform,domain,company,date_denver,source_table)

    SELECT  value,
            metric,
            
            'devices',
            'asp',
            'app',
            company,
            date_denver,
            'asp_v_spc_app_events'
    FROM (SELECT  company,
                  date_denver,
                  
                  MAP(

                      'account_view|Account View|MySpectrum_adobe||', account_view_devices,
                      'autopay_enroll_failure|AutoPay Enroll Failure|MySpectrum_adobe||', autopay_enroll_failure_devices,
                      'autopay_enroll_radio_toggle|AutoPay Enroll Radio Toggle|MySpectrum_adobe||', autopay_enroll_radio_toggle_devices,
                      'autopay_enroll_success|AutoPay Enroll Success|MySpectrum_adobe||', autopay_enroll_success_devices,
                      'bill_pay_view|Bill Pay View|MySpectrum_adobe||', bill_pay_view_devices,
                      'call_support|Call Support|MySpectrum_adobe||', call_support_devices,
                      'cancelled_appointments|Cancelled Appointments|MySpectrum_adobe||', cancelled_appointments_devices,
                      'crashes|Crashes|MySpectrum_adobe||', crashes_devices,
                      'crashes_android|Crashes Android|MySpectrum_adobe||', crashes_android_devices,
                      'crashes_ios|Crashes iOS|MySpectrum_adobe||', crashes_ios_devices,
                      'create_username_flow_failure|Create Username Flow Failure|MySpectrum_adobe||', create_username_flow_failure_devices,
                      'create_username_flow_start|Create Username Flow Start|MySpectrum_adobe||', create_username_flow_start_devices,
                      'create_username_fow_success|Create Username Flow Success|MySpectrum_adobe||', create_username_fow_success_devices,
                      'dashboard_view|Dashboard View|MySpectrum_adobe||', dashboard_view_devices,
                      'equipment_detail_views|Equipment Details Views|MySpectrum_adobe||', equipment_detail_views_devices,
                      'equipment_list_view|Equipment List View|MySpectrum_adobe||', equipment_list_view_devices,
                      'all_equipment_reset_flow_failures|Equipment Reset Flow Failures All|MySpectrum_adobe||', all_equipment_reset_flow_failures_devices,
                      'internet_equipment_reset_flow_failures|Equipment Reset Flow Failures Internet|MySpectrum_adobe||', internet_equipment_reset_flow_failures_devices,
                      'tv_equipment_reset_flow_failures|Equipment Reset Flow Failures TV|MySpectrum_adobe||', tv_equipment_reset_flow_failures_devices,
                      'voice_equipment_reset_flow_failures|Equipment Reset Flow Failures Voice|MySpectrum_adobe||', voice_equipment_reset_flow_failures_devices,
                      'all_equipment_reset_flow_starts|Equipment Reset Flow Starts All|MySpectrum_adobe||', all_equipment_reset_flow_starts_devices,
                      'internet_equipment_reset_flow_starts|Equipment Reset Flow Starts Internet|MySpectrum_adobe||', internet_equipment_reset_flow_starts_devices,
                      'tv_equipment_reset_flow_starts|Equipment Reset Flow Starts TV|MySpectrum_adobe||', tv_equipment_reset_flow_starts_devices,
                      'voice_equipment_reset_flow_starts|Equipment Reset Flow Starts Voice|MySpectrum_adobe||', voice_equipment_reset_flow_starts_devices,
                      'all_equipment_reset_flow_successes|Equipment Reset Flow Successes All|MySpectrum_adobe||', all_equipment_reset_flow_successes_devices,
                      'internet_equipment_reset_flow_successes|Equipment Reset Flow Successes Internet|MySpectrum_adobe||', internet_equipment_reset_flow_successes_devices,
                      'tv_equipment_reset_flow_successes|Equipment Reset Flow Successes TV|MySpectrum_adobe||', tv_equipment_reset_flow_successes_devices,
                      'voice_equipment_reset_flow_successes|Equipment Reset Flow Successes Voice|MySpectrum_adobe||', voice_equipment_reset_flow_successes_devices,
                      'forgot_password_flow_failure|Forgot Password Flow Failure|MySpectrum_adobe||', forgot_password_flow_failure_devices,
                      'forgot_password_flow_start|Forgot Password Flow Start|MySpectrum_adobe||', forgot_password_flow_start_devices,
                      'forgot_password_flow_success|Forgot Password Flow Success|MySpectrum_adobe||', forgot_password_flow_success_devices,
                      'forgot_username_flow_failure|Forgot Username Flow Failure|MySpectrum_adobe||', forgot_username_flow_failure_devices,
                      'forgot_username_flow_start|Forgot Username Flow Start|MySpectrum_adobe||', forgot_username_flow_start_devices,
                      'forgot_username_flow_success|Forgot Username Flow Success|MySpectrum_adobe||', forgot_username_flow_success_devices,
                      'internet_services_connected_experiencing_issues|Internet Services Connected Experiencing Issues|MySpectrum_adobe||', internet_services_connected_experiencing_issues_devices,
                      'internet_services_connection_issue_troubleshoot|Internet Services Connection Issue Troubleshoot|MySpectrum_adobe||', internet_services_connection_issue_troubleshoot_devices,
                      'launches|Launches|MySpectrum_adobe||', launches_devices,
                      'launches_android|Launches Android|MySpectrum_adobe||', launches_android_devices,
                      'launches_ios|Launches iOS|MySpectrum_adobe||', launches_ios_devices,
                      'legacy_modem_router_reset_flow_failure|Legacy Modem Router Reset Flow Failure|MySpectrum_adobe||', legacy_modem_router_reset_flow_failure_devices,
                      'legacy_modem_router_reset_flow_start|Legacy Modem Router Reset Flow Start|MySpectrum_adobe||', legacy_modem_router_reset_flow_start_devices,
                      'legacy_modem_router_reset_flow_success|Legacy Modem Router Reset Flow Success|MySpectrum_adobe||', legacy_modem_router_reset_flow_success_devices,
                      'manual_reset_failures|Manual Reset Failures|MySpectrum_adobe||', manual_reset_failures_devices,
                      'manual_reset_starts|Manual Reset Starts|MySpectrum_adobe||', manual_reset_starts_devices,
                      'manual_reset_success|Manual Reset Success|MySpectrum_adobe||', manual_reset_success_devices,
                      'internet_modem_reset_failures|Modem Reset Failures Internet|MySpectrum_adobe||', internet_modem_reset_failures_devices,
                      'tv_modem_reset_failures|Modem Reset Failures TV|MySpectrum_adobe||', tv_modem_reset_failures_devices,
                      'voice_modem_reset_failures|Modem Reset Failures Voice|MySpectrum_adobe||', voice_modem_reset_failures_devices,
                      'internet_modem_reset_successes|Modem Reset Successes Internet|MySpectrum_adobe||', internet_modem_reset_successes_devices,
                      'tv_modem_reset_successes|Modem Reset Successes TV|MySpectrum_adobe||', tv_modem_reset_successes_devices,
                      'voice_modem_reset_successes|Modem Reset Successes Voice|MySpectrum_adobe||', voice_modem_reset_successes_devices,
                      'internet_modem_router_reset_starts|Modem Router Reset Starts Internet|MySpectrum_adobe||', internet_modem_router_reset_starts_devices,
                      'tv_modem_router_reset_starts|Modem Router Reset Starts TV|MySpectrum_adobe||', tv_modem_router_reset_starts_devices,
                      'voice_modem_router_reset_starts|Modem Router Reset Starts Voice|MySpectrum_adobe||', voice_modem_router_reset_starts_devices,
                      'one_time_payment_failure|One Time Payment Failure|MySpectrum_adobe||', one_time_payment_failure_devices,
                      'one_time_payment_start|One Time Payment Start|MySpectrum_adobe||', one_time_payment_start_devices,
                      'one_time_payment_success|One Time Payment Success|MySpectrum_adobe||', one_time_payment_success_devices,
                      'page_views|Page Views|MySpectrum_adobe||', page_views_devices,
                      'refresh_digital_receiver_requests|Refresh Digital Receiver Requests|MySpectrum_adobe||', refresh_digital_receiver_requests_devices,
                      'rescheduled_appointments|Rescheduled Appointments|MySpectrum_adobe||', rescheduled_appointments_devices,
                      'internet_router_reset_failures|Router Reset Failures Internet|MySpectrum_adobe||', internet_router_reset_failures_devices,
                      'tv_router_reset_failures|Router Reset Failures TV|MySpectrum_adobe||', tv_router_reset_failures_devices,
                      'voice_router_reset_failures|Router Reset Failures Voice|MySpectrum_adobe||', voice_router_reset_failures_devices,
                      'internet_router_reset_successes|Router Reset Successes Internet|MySpectrum_adobe||', internet_router_reset_successes_devices,
                      'tv_router_reset_successes|Router Reset Successes TV|MySpectrum_adobe||', tv_router_reset_successes_devices,
                      'voice_router_reset_successes|Router Reset Successes Voice|MySpectrum_adobe||', voice_router_reset_successes_devices,
                      'service_call_failures|Service Call Failures|MySpectrum_adobe||', service_call_failures_devices,
                      'services_tab_views|Services Tab Views|MySpectrum_adobe||', services_tab_views_devices,
                      'site_unique|Site Unique Values|MySpectrum_adobe||', site_unique_devices,
                      'support_page_views|Support Page Views|MySpectrum_adobe||', support_page_views_devices,
                      'support_view|Support View|MySpectrum_adobe||', support_view_devices,
                      'video_refresh_success|Video Refresh Success|MySpectrum_adobe||', video_refresh_success_devices,
                      'view_statement|View Statement|MySpectrum_adobe||', view_statement_devices,
                      'wifi_password_views|Wifi Password Views|MySpectrum_adobe||', wifi_password_views_devices
                  ) as map_column
          FROM ${env:TMP_db}.asp_counts_daily_counts_devices_columns
        ) as derived_table
    LATERAL VIEW EXPLODE (map_column) exploded_table AS metric, value
;
