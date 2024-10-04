set hive.vectorized.execution.enabled = false;

USE prod;

DROP TABLE IF EXISTS ${env:TMP_db}.asp_counts_fiscal_monthly_counts_instances_columns;
CREATE TEMPORARY TABLE ${env:TMP_db}.asp_counts_fiscal_monthly_counts_instances_columns AS
    SELECT
        CASE
            WHEN state__view__previous_page__sub_section = 'BH' THEN 'BHN'
            WHEN state__view__previous_page__sub_section = 'CHARTER' THEN 'CHTR'
            WHEN state__view__previous_page__sub_section = 'TWC' THEN 'TWCC'
            WHEN state__view__previous_page__sub_section IS NULL THEN 'UNAUTH'
            ELSE 'UNDEFINED'
        END as company,
        SUM(IF( message__name IN ('Account View'), 1, 0)) AS account_view,
        SUM(IF( message__name RLIKE 'AMACTION:AddAutoPay Svc Call Failure-.*'  AND message__category = 'Custom Link' , 1, 0)) AS autopay_enroll_failure,
        SUM(IF( message__name IN ('AMACTION:Payment View Auto Pay Enabled Trigger'), 1, 0)) AS autopay_enroll_radio_toggle,
        SUM(IF( message__name IN ('AMACTION:AddAutoPay Svc Call Success'), 1, 0)) AS autopay_enroll_success,
        SUM(IF( message__name IN ('Bill Pay View'), 1, 0)) AS bill_pay_view,
        SUM(IF( message__name IN ('AMACTION:Call Support Trigger'), 1, 0)) AS call_support,
        SUM(IF( message__name IN ('AMACTION:CancelAppointment Svc Call Success'), 1, 0)) AS cancelled_appointments,
        SUM(IF( visit__application_details__referrer_link RLIKE '.*200.*', 1, 0)) AS crashes,
        SUM(IF( visit__application_details__app_version RLIKE 'Android.*'  AND visit__application_details__referrer_link RLIKE '.*200.*' , 1, 0)) AS crashes_android,
        SUM(IF( visit__application_details__app_version RLIKE 'iOS.*'  AND visit__application_details__referrer_link RLIKE '.*200.*' , 1, 0)) AS crashes_ios,
        SUM(IF( message__name RLIKE 'AMACTION:Register Svc Call Failure-.*', 1, 0)) AS create_username_flow_failure,
        SUM(IF( message__name IN ('AMACTION:app.analytics.tracking.Login.Action.RegisterAccount')  AND message__category = 'Custom Link' , 1, 0)) AS create_username_flow_start,
        SUM(IF( message__name IN ('AMACTION:Register Svc Call Success'), 1, 0)) AS create_username_fow_success,
        SUM(IF( message__name IN ('Dashboard View'), 1, 0)) AS dashboard_view,
        SUM(IF( message__name IN ('Internet Equipment Details View','Voice Equipment Details View')  AND message__category = 'Page View' , 1, 0)) AS equipment_detail_views,
        SUM(IF( message__name IN ('Equipment List View'), 1, 0)) AS equipment_list_view,
        SUM(IF( message__name IN ('Equipment Internet Reset Failure View','AMACTION:Troubleshoot ProblemNotSolved Trigger','Equipment Voice Reset Failure View','Equipment TV Reset Failure View'), 1, 0)) AS all_equipment_reset_flow_failures,
        SUM(IF( message__name IN ('Equipment Internet Reset Failure View'), 1, 0)) AS internet_equipment_reset_flow_failures,
        SUM(IF( message__name IN ('Equipment TV Reset Failure View'), 1, 0)) AS tv_equipment_reset_flow_failures,
        SUM(IF( message__name IN ('Equipment Voice Reset Failure View'), 1, 0)) AS voice_equipment_reset_flow_failures,
        SUM(IF( message__name IN ('AMACTION:Troubleshoot Device AutoTroubleshoot Trigger','AMACTION:Equipment Internet Reset Equipment Trigger','AMACTION:Equipment Voice Reset Equipment Trigger','AMACTION:Equipment TV Reset Equipment Trigger'), 1, 0)) AS all_equipment_reset_flow_starts,
        SUM(IF( message__name IN ('AMACTION:Equipment Internet Reset Equipment Trigger'), 1, 0)) AS internet_equipment_reset_flow_starts,
        SUM(IF( message__name IN ('AMACTION:Equipment TV Reset Equipment Trigger'), 1, 0)) AS tv_equipment_reset_flow_starts,
        SUM(IF( message__name IN ('AMACTION:Equipment Voice Reset Equipment Trigger'), 1, 0)) AS voice_equipment_reset_flow_starts,
        SUM(IF( message__name IN ('Equipment Internet Reset Success View','AMACTION:Troubleshoot ProblemSolved Trigger','Equipment Voice Reset Success View','Equipment TV Reset Success View'), 1, 0)) AS all_equipment_reset_flow_successes,
        SUM(IF( message__name IN ('Equipment Internet Reset Success View'), 1, 0)) AS internet_equipment_reset_flow_successes,
        SUM(IF( message__name IN ('Equipment TV Reset Success View'), 1, 0)) AS tv_equipment_reset_flow_successes,
        SUM(IF( message__name IN ('Equipment Voice Reset Success View'), 1, 0)) AS voice_equipment_reset_flow_successes,
        SUM(IF( message__name RLIKE 'AMACTION:ForgotPasswordStep3 Svc Call Failure.*', 1, 0)) AS forgot_password_flow_failure,
        SUM(IF( message__name IN ('AMACTION:Forgot Password')  AND message__category = 'Custom Link' , 1, 0)) AS forgot_password_flow_start,
        SUM(IF( message__name IN ('AMACTION:ForgotPasswordStep3 Svc Call Success'), 1, 0)) AS forgot_password_flow_success,
        SUM(IF( message__name RLIKE 'AMACTION:ForgotUsername Svc Call Failure.*', 1, 0)) AS forgot_username_flow_failure,
        SUM(IF( message__name IN ('AMACTION:Forgot Username'), 1, 0)) AS forgot_username_flow_start,
        SUM(IF( message__name IN ('AMACTION:ForgotUsername Svc Call Success'), 1, 0)) AS forgot_username_flow_success,
        SUM(IF( message__name IN ('AMACTION:Internet Services Connected Experiencing Issues')  AND message__category = 'Custom Link' , 1, 0)) AS internet_services_connected_experiencing_issues,
        SUM(IF( message__name IN ('AMACTION:Internet Services ConnectionIssue Troubleshoot')  AND message__category = 'Custom Link' , 1, 0)) AS internet_services_connection_issue_troubleshoot,
        SUM(IF( visit__application_details__referrer_link RLIKE '.*201.*', 1, 0)) AS launches,
        SUM(IF( visit__application_details__app_version RLIKE 'Android.*'  AND visit__application_details__referrer_link RLIKE '.*201.*' , 1, 0)) AS launches_android,
        SUM(IF( visit__application_details__app_version RLIKE 'iOS.*'  AND visit__application_details__referrer_link RLIKE '.*201.*' , 1, 0)) AS launches_ios,
        SUM(IF( message__name IN ('AMACTION:Troubleshoot ProblemNotSolved Trigger'), 1, 0)) AS legacy_modem_router_reset_flow_failure,
        SUM(IF( message__name IN ('AMACTION:Troubleshoot Device AutoTroubleshoot Trigger'), 1, 0)) AS legacy_modem_router_reset_flow_start,
        SUM(IF( message__name IN ('AMACTION:Troubleshoot ProblemSolved Trigger'), 1, 0)) AS legacy_modem_router_reset_flow_success,
        SUM(IF( message__name IN ('Equipment Internet Manual Troubleshoot Article Continue'), 1, 0)) AS manual_reset_failures,
        SUM(IF( message__name IN ('AMACTION:Troubleshoot Device ManualTroubleshoot Trigger'), 1, 0)) AS manual_reset_starts,
        SUM(IF( message__name IN ('AMACTION:Equipment Manual Reset Svc Call Success'), 1, 0)) AS manual_reset_success,
        SUM(IF( message__name IN ('AMACTION:Equipment Internet reboot Modem failure')  AND message__category = 'Custom Link' , 1, 0)) AS internet_modem_reset_failures,
        SUM(IF( message__name IN ('AMACTION:Equipment TV reboot Modem failure')  AND message__category = 'Custom Link' , 1, 0)) AS tv_modem_reset_failures,
        SUM(IF( message__name IN ('AMACTION:Equipment Voice reboot Modem failure')  AND message__category = 'Custom Link' , 1, 0)) AS voice_modem_reset_failures,
        SUM(IF( message__name IN ('AMACTION:Equipment Internet reboot Modem success')  AND message__category = 'Custom Link' , 1, 0)) AS internet_modem_reset_successes,
        SUM(IF( message__name IN ('AMACTION:Equipment TV reboot Modem success')  AND message__category = 'Custom Link' , 1, 0)) AS tv_modem_reset_successes,
        SUM(IF( message__name IN ('AMACTION:Equipment Voice reboot Modem success')  AND message__category = 'Custom Link' , 1, 0)) AS voice_modem_reset_successes,
        SUM(IF( message__name IN ('AMACTION:Equipment Internet Reset Equipment Trigger')  AND message__category = 'Custom Link' , 1, 0)) AS internet_modem_router_reset_starts,
        SUM(IF( message__name IN ('AMACTION:Equipment TV Reset Equipment Trigger')  AND message__category = 'Custom Link' , 1, 0)) AS tv_modem_router_reset_starts,
        SUM(IF( message__name IN ('AMACTION:Equipment Voice Reset Equipment Trigger')  AND message__category = 'Custom Link' , 1, 0)) AS voice_modem_router_reset_starts,
        SUM(IF( message__name RLIKE 'AMACTION:MakeOneTimePayment Svc Call Failure-.*'  AND message__category = 'Custom Link' , 1, 0)) AS one_time_payment_failure,
        SUM(IF( message__name IN ('AMACTION:Dashboard Make Payment Button Trigger','AMACTION:Make Payment From BillPayTab'), 1, 0)) AS one_time_payment_start,
        SUM(IF( message__name IN ('AMACTION:Paid with new Payment Method','AMACTION:Paid with an existing Payment'), 1, 0)) AS one_time_payment_success,
        SUM(IF( message__category = 'Page View', 1, 0)) AS page_views,
        SUM(IF( message__name IN ('AMACTION:RefreshEquipment Svc Call Success'), 1, 0)) AS refresh_digital_receiver_requests,
        SUM(IF( message__name IN ('AMACTION:RescheduleAppointment Svc Call Success'), 1, 0)) AS rescheduled_appointments,
        SUM(IF( message__name IN ('AMACTION:Equipment Internet reboot Router failure')  AND message__category = 'Custom Link' , 1, 0)) AS internet_router_reset_failures,
        SUM(IF( message__name IN ('AMACTION:Equipment TV reboot Router failure')  AND message__category = 'Custom Link' , 1, 0)) AS tv_router_reset_failures,
        SUM(IF( message__name IN ('AMACTION:Equipment Voice reboot Router failure')  AND message__category = 'Custom Link' , 1, 0)) AS voice_router_reset_failures,
        SUM(IF( message__name IN ('AMACTION:Equipment Internet reboot Router success')  AND message__category = 'Custom Link' , 1, 0)) AS internet_router_reset_successes,
        SUM(IF( message__name IN ('AMACTION:Equipment TV reboot Router success')  AND message__category = 'Custom Link' , 1, 0)) AS tv_router_reset_successes,
        SUM(IF( message__name IN ('AMACTION:Equipment Voice reboot Router success')  AND message__category = 'Custom Link' , 1, 0)) AS voice_router_reset_successes,
        SUM(IF( message__name RLIKE '.*Svc Call Failure.*'  AND message__category = 'Custom Link' , 1, 0)) AS service_call_failures,
        SUM(IF( message__name IN ('Internet Services','Voice Services','TV Services')  AND message__category = 'Page View' , 1, 0)) AS services_tab_views,
        SUM(IF( TRUE, 1, 0)) AS site_unique,
        SUM(IF( message__name IN ('Support View','Passpoint Setup View','Tutorial Walkthrough First Use View','Channel Lineups','Moving Form NewAddress View','Program Your Remote','Locations Map','Terms And Conditions','AMACTION:Moving Trigger'), 1, 0)) AS support_page_views,
        SUM(IF( message__name IN ('Support View'), 1, 0)) AS support_view,
        SUM(IF( message__name IN ('AMACTION:RefreshEquipment Svc Call Success')  AND message__category = 'Custom Link' , 1, 0)) AS video_refresh_success,
        SUM(IF( message__name IN ('AMACTION:PDFStatementDownload Svc Call Success', 'AMACTION:PDF Single Statement Svc Call Success'), 1, 0)) AS view_statement,
        SUM(IF( message__name IN ('AMACTION:Show Router Password')  AND message__category = 'Custom Link' , 1, 0)) AS wifi_password_views,
        'asp' AS platform,
        'app' AS domain,
        
        fiscal_month as year_fiscal_month_denver
    FROM asp_v_spc_app_events
         LEFT JOIN prod_lkp.chtr_fiscal_month ON epoch_converter(cast(message__timestamp * 1000 as bigint),'America/Denver') = partition_date
    WHERE (partition_date_hour_utc >= '${env:START_DATE_TZ}'
       AND partition_date_hour_utc <  '${env:END_DATE_TZ}')
    GROUP BY
        fiscal_month,
        
        CASE
            WHEN state__view__previous_page__sub_section = 'BH' THEN 'BHN'
            WHEN state__view__previous_page__sub_section = 'CHARTER' THEN 'CHTR'
            WHEN state__view__previous_page__sub_section = 'TWC' THEN 'TWCC'
            WHEN state__view__previous_page__sub_section IS NULL THEN 'UNAUTH'
            ELSE 'UNDEFINED'
        END
    ;
INSERT OVERWRITE TABLE prod.asp_counts_fiscal_monthly
PARTITION(unit,platform,domain,company,year_fiscal_month_denver,source_table)

    SELECT  value,
            metric,
            
            'instances',
            'asp',
            'app',
            company,
            year_fiscal_month_denver,
            'asp_v_spc_app_events'
    FROM (SELECT  company,
                  year_fiscal_month_denver,
                  
                  MAP(

                      'account_view|Account View|MySpectrum_adobe||', account_view,
                      'autopay_enroll_failure|AutoPay Enroll Failure|MySpectrum_adobe||', autopay_enroll_failure,
                      'autopay_enroll_radio_toggle|AutoPay Enroll Radio Toggle|MySpectrum_adobe||', autopay_enroll_radio_toggle,
                      'autopay_enroll_success|AutoPay Enroll Success|MySpectrum_adobe||', autopay_enroll_success,
                      'bill_pay_view|Bill Pay View|MySpectrum_adobe||', bill_pay_view,
                      'call_support|Call Support|MySpectrum_adobe||', call_support,
                      'cancelled_appointments|Cancelled Appointments|MySpectrum_adobe||', cancelled_appointments,
                      'crashes|Crashes|MySpectrum_adobe||', crashes,
                      'crashes_android|Crashes Android|MySpectrum_adobe||', crashes_android,
                      'crashes_ios|Crashes iOS|MySpectrum_adobe||', crashes_ios,
                      'create_username_flow_failure|Create Username Flow Failure|MySpectrum_adobe||', create_username_flow_failure,
                      'create_username_flow_start|Create Username Flow Start|MySpectrum_adobe||', create_username_flow_start,
                      'create_username_fow_success|Create Username Flow Success|MySpectrum_adobe||', create_username_fow_success,
                      'dashboard_view|Dashboard View|MySpectrum_adobe||', dashboard_view,
                      'equipment_detail_views|Equipment Details Views|MySpectrum_adobe||', equipment_detail_views,
                      'equipment_list_view|Equipment List View|MySpectrum_adobe||', equipment_list_view,
                      'all_equipment_reset_flow_failures|Equipment Reset Flow Failures All|MySpectrum_adobe||', all_equipment_reset_flow_failures,
                      'internet_equipment_reset_flow_failures|Equipment Reset Flow Failures Internet|MySpectrum_adobe||', internet_equipment_reset_flow_failures,
                      'tv_equipment_reset_flow_failures|Equipment Reset Flow Failures TV|MySpectrum_adobe||', tv_equipment_reset_flow_failures,
                      'voice_equipment_reset_flow_failures|Equipment Reset Flow Failures Voice|MySpectrum_adobe||', voice_equipment_reset_flow_failures,
                      'all_equipment_reset_flow_starts|Equipment Reset Flow Starts All|MySpectrum_adobe||', all_equipment_reset_flow_starts,
                      'internet_equipment_reset_flow_starts|Equipment Reset Flow Starts Internet|MySpectrum_adobe||', internet_equipment_reset_flow_starts,
                      'tv_equipment_reset_flow_starts|Equipment Reset Flow Starts TV|MySpectrum_adobe||', tv_equipment_reset_flow_starts,
                      'voice_equipment_reset_flow_starts|Equipment Reset Flow Starts Voice|MySpectrum_adobe||', voice_equipment_reset_flow_starts,
                      'all_equipment_reset_flow_successes|Equipment Reset Flow Successes All|MySpectrum_adobe||', all_equipment_reset_flow_successes,
                      'internet_equipment_reset_flow_successes|Equipment Reset Flow Successes Internet|MySpectrum_adobe||', internet_equipment_reset_flow_successes,
                      'tv_equipment_reset_flow_successes|Equipment Reset Flow Successes TV|MySpectrum_adobe||', tv_equipment_reset_flow_successes,
                      'voice_equipment_reset_flow_successes|Equipment Reset Flow Successes Voice|MySpectrum_adobe||', voice_equipment_reset_flow_successes,
                      'forgot_password_flow_failure|Forgot Password Flow Failure|MySpectrum_adobe||', forgot_password_flow_failure,
                      'forgot_password_flow_start|Forgot Password Flow Start|MySpectrum_adobe||', forgot_password_flow_start,
                      'forgot_password_flow_success|Forgot Password Flow Success|MySpectrum_adobe||', forgot_password_flow_success,
                      'forgot_username_flow_failure|Forgot Username Flow Failure|MySpectrum_adobe||', forgot_username_flow_failure,
                      'forgot_username_flow_start|Forgot Username Flow Start|MySpectrum_adobe||', forgot_username_flow_start,
                      'forgot_username_flow_success|Forgot Username Flow Success|MySpectrum_adobe||', forgot_username_flow_success,
                      'internet_services_connected_experiencing_issues|Internet Services Connected Experiencing Issues|MySpectrum_adobe||', internet_services_connected_experiencing_issues,
                      'internet_services_connection_issue_troubleshoot|Internet Services Connection Issue Troubleshoot|MySpectrum_adobe||', internet_services_connection_issue_troubleshoot,
                      'launches|Launches|MySpectrum_adobe||', launches,
                      'launches_android|Launches Android|MySpectrum_adobe||', launches_android,
                      'launches_ios|Launches iOS|MySpectrum_adobe||', launches_ios,
                      'legacy_modem_router_reset_flow_failure|Legacy Modem Router Reset Flow Failure|MySpectrum_adobe||', legacy_modem_router_reset_flow_failure,
                      'legacy_modem_router_reset_flow_start|Legacy Modem Router Reset Flow Start|MySpectrum_adobe||', legacy_modem_router_reset_flow_start,
                      'legacy_modem_router_reset_flow_success|Legacy Modem Router Reset Flow Success|MySpectrum_adobe||', legacy_modem_router_reset_flow_success,
                      'manual_reset_failures|Manual Reset Failures|MySpectrum_adobe||', manual_reset_failures,
                      'manual_reset_starts|Manual Reset Starts|MySpectrum_adobe||', manual_reset_starts,
                      'manual_reset_success|Manual Reset Success|MySpectrum_adobe||', manual_reset_success,
                      'internet_modem_reset_failures|Modem Reset Failures Internet|MySpectrum_adobe||', internet_modem_reset_failures,
                      'tv_modem_reset_failures|Modem Reset Failures TV|MySpectrum_adobe||', tv_modem_reset_failures,
                      'voice_modem_reset_failures|Modem Reset Failures Voice|MySpectrum_adobe||', voice_modem_reset_failures,
                      'internet_modem_reset_successes|Modem Reset Successes Internet|MySpectrum_adobe||', internet_modem_reset_successes,
                      'tv_modem_reset_successes|Modem Reset Successes TV|MySpectrum_adobe||', tv_modem_reset_successes,
                      'voice_modem_reset_successes|Modem Reset Successes Voice|MySpectrum_adobe||', voice_modem_reset_successes,
                      'internet_modem_router_reset_starts|Modem Router Reset Starts Internet|MySpectrum_adobe||', internet_modem_router_reset_starts,
                      'tv_modem_router_reset_starts|Modem Router Reset Starts TV|MySpectrum_adobe||', tv_modem_router_reset_starts,
                      'voice_modem_router_reset_starts|Modem Router Reset Starts Voice|MySpectrum_adobe||', voice_modem_router_reset_starts,
                      'one_time_payment_failure|One Time Payment Failure|MySpectrum_adobe||', one_time_payment_failure,
                      'one_time_payment_start|One Time Payment Start|MySpectrum_adobe||', one_time_payment_start,
                      'one_time_payment_success|One Time Payment Success|MySpectrum_adobe||', one_time_payment_success,
                      'page_views|Page Views|MySpectrum_adobe||', page_views,
                      'refresh_digital_receiver_requests|Refresh Digital Receiver Requests|MySpectrum_adobe||', refresh_digital_receiver_requests,
                      'rescheduled_appointments|Rescheduled Appointments|MySpectrum_adobe||', rescheduled_appointments,
                      'internet_router_reset_failures|Router Reset Failures Internet|MySpectrum_adobe||', internet_router_reset_failures,
                      'tv_router_reset_failures|Router Reset Failures TV|MySpectrum_adobe||', tv_router_reset_failures,
                      'voice_router_reset_failures|Router Reset Failures Voice|MySpectrum_adobe||', voice_router_reset_failures,
                      'internet_router_reset_successes|Router Reset Successes Internet|MySpectrum_adobe||', internet_router_reset_successes,
                      'tv_router_reset_successes|Router Reset Successes TV|MySpectrum_adobe||', tv_router_reset_successes,
                      'voice_router_reset_successes|Router Reset Successes Voice|MySpectrum_adobe||', voice_router_reset_successes,
                      'service_call_failures|Service Call Failures|MySpectrum_adobe||', service_call_failures,
                      'services_tab_views|Services Tab Views|MySpectrum_adobe||', services_tab_views,
                      'site_unique|Site Unique Values|MySpectrum_adobe||', site_unique,
                      'support_page_views|Support Page Views|MySpectrum_adobe||', support_page_views,
                      'support_view|Support View|MySpectrum_adobe||', support_view,
                      'video_refresh_success|Video Refresh Success|MySpectrum_adobe||', video_refresh_success,
                      'view_statement|View Statement|MySpectrum_adobe||', view_statement,
                      'wifi_password_views|Wifi Password Views|MySpectrum_adobe||', wifi_password_views
                  ) as map_column
          FROM ${env:TMP_db}.asp_counts_fiscal_monthly_counts_instances_columns
        ) as derived_table
    LATERAL VIEW EXPLODE (map_column) exploded_table AS metric, value
;
