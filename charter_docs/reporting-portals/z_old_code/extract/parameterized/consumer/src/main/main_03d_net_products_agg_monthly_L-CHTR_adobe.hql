-------------------------------------------------------------------------------

--Populates the temp table net_products_agg_monthly with aggregated data for L-CHTR
--Rev'd 23 June 2018 added new temp table to pull in total visits because
----visits added to the original temp table kept blowing the query up
--Rev'd 07 March 2018 statement views where post prop 26 is not null
--Rev'd 07 Feb 2018 Rev'd Refresh Digital Receiver AND Reboot Internet Modem metrics (CL55 day-of-updates)
--Rev'd 01 Feb 2018 Updated OTP, OTP w-AP, AP, and Refresh Digital Receiver metrics (CL55)
--Rev'd 05 Jan 2018 Updated new identities created including sub accounts
--Rev'd 03 Jan 2018 Added pseudo device_type
--Rev'd 14 Dec 2017 Changes for Spectrum Daily
--Rev'd 04 Dec 2017 Changes to Spectrum.net appointment metrics
-------------------------------------------------------------------------------

-- [Error: Failure while running task:java.lang.RuntimeException:
-- java.lang.OutOfMemoryError: Java heap space


USE ${env:ENVIRONMENT};

set hive.auto.convert.join=false;


SET hive.tez.container.size=10682;



-- Drop and rebuild net_bill_pay_analytics_monthly temp table --
DROP TABLE IF EXISTS ${env:TMP_db}.net_chtr_${env:CADENCE};

-- For troubleshooting queries, remove 'TEMPORARY'
CREATE TABLE IF NOT EXISTS ${env:TMP_db}.net_chtr_${env:CADENCE}
AS

SELECT  SUM(case when message__category in('Page View') THEN 1 ELSE 0 END) AS total_page_views_count,
        SUM(case when state__view__current_page__section = 'My Account' and message__category ='Page View' THEN 1 ELSE 0 END) AS my_account_page_views_count,
        --spectrum.net daily - Support Page Views
        --- WHERE Site Section = 'support'

        SUM(IF(UPPER(state__view__current_page__section) = 'SUPPORT' AND message__category = 'Page View',1,0))  AS support_page_views_count, -- Updated 2017-12-14 DPrince
        SUM(case when message__name IN  ('Ask-Spectrum', 'open-ask-spectrum') THEN 1 ELSE 0 END) AS ask_charter_requests_count,

        -- Refresh STB is also known as: --- Reset-Equipment-TV-Success  - refresh_requests_count,
        -- updated 2018-02-07 CL 55 tagging updated to include views of "Reset-Equipment-TV-Success"
        SUM(IF((message__name) IN ('Refresh','equip.tv.troubleshoot.reset-equip.start')
              AND message__category = 'Custom Link',1,0))  +   -- pre CL55, below is post
        SUM(IF(state__view__current_page__name = 'Reset-Equipment-TV-Success'
              AND message__category = 'Page View',1,0))  AS refresh_requests_count,

        SUM(case when state__view__current_page__name IN ('TV','On Demand') and message__category ='Page View' THEN 1 ELSE 0 END) AS video_page_views_count,
        SUM(case when message__name IN ('Play') THEN 1 ELSE 0 END) AS video_plays_count,

        --New IDs Created on CHTR **This will NOT be used... "new_ids_charter_count_all" is the correct field(see main_03f)**
        SIZE(COLLECT_SET(IF(state__view__current_page__name IN('my-account.create-id-final.bam','my-account.create-id-final.btm') AND message__category = 'Page View', visit__visit_id,NULL)))
        +
        SIZE(COLLECT_SET(IF(state__view__current_page__name IN('my-account.create-id-final.nbtm') AND message__category = 'Page View', visit__visit_id,NULL))) AS new_ids_charter_count,

        --NEW New IDs Created
        SIZE(COLLECT_SET(case when message__name = 'my-account.create-id-final.bam' THEN visit__visit_id ELSE NULL END)) AS new_ids_charter_count_bam,
        SIZE(COLLECT_SET(case when message__name = 'my-account.create-id-final.btm' THEN visit__visit_id ELSE NULL END)) AS new_ids_charter_count_btm,
        SIZE(COLLECT_SET(case when message__name = 'my-account.create-id-final.nbtm' THEN visit__visit_id ELSE NULL END)) AS new_ids_charter_count_nbtm,

        --NEW New IDs Attempts
        (SIZE(COLLECT_SET(CASE WHEN state__view__current_page__name IN ('my-account.create-id-1.bam') THEN visit__visit_id ELSE NULL END))
        -
        SIZE(COLLECT_SET(CASE WHEN state__view__current_page__name IN ('my-account.create-id-2.bam') AND lower(message__name) RLIKE ('.*sign\-in\-now.*')
        THEN visit__visit_id ELSE NULL END))) AS attempts_create_id_count_bam,
        (SIZE(COLLECT_SET(CASE WHEN state__view__current_page__name IN ('my-account.create-id-1.btm') THEN visit__visit_id ELSE NULL END))
        -
        SIZE(COLLECT_SET(CASE WHEN state__view__current_page__name IN ('my-account.create-id-2.btm') AND lower(message__name) RLIKE ('.*sign\-in\-now.*')
        THEN visit__visit_id ELSE NULL END))) AS attempts_create_id_count_btm,
        (SIZE(COLLECT_SET(CASE WHEN state__view__current_page__name IN ('my-account.create-id-1.nbtm') THEN visit__visit_id ELSE NULL END))
        -
        SIZE(COLLECT_SET(CASE WHEN state__view__current_page__name IN ('my-account.create-id-2.nbtm') AND lower(message__name) RLIKE ('.*sign\-in\-now.*')
        THEN visit__visit_id ELSE NULL END))) AS attempts_create_id_count_nbtm,

        --OLD New IDs Created and Attempts off CHTR
        SIZE(COLLECT_SET(case when message__name IN ('my-account.create-id-final.nbtm',
        'my-account.create-id-final.nbtm_STVA') THEN visit__visit_id ELSE NULL END)) AS new_ids_not_charter_count,
        (SIZE(COLLECT_SET(case when state__view__current_page__name IN ('my-account.create-id-1.nbtm') THEN visit__visit_id ELSE NULL END))
        -
        SIZE(COLLECT_SET(case when state__view__current_page__name IN ('my-account.create-id-2.nbtm')
        AND message__name NOT RLIKE ('.*sign\-in\-now.*') THEN visit__visit_id ELSE NULL END))) AS total_attempts_id_off_count,

        --Recover Flow
        SIZE(COLLECT_SET(CASE WHEN message__name IN ('Recover-1.btm') THEN visit__visit_id ELSE NULL END))
        -
        SIZE(COLLECT_SET(CASE WHEN message__name IN ('Recover-noID.btm') THEN visit__visit_id ELSE NULL END)) AS attempts_recover_username_BTM_count,
        SIZE(COLLECT_SET(CASE WHEN message__name IN ('Recover-final1.btm', 'Recover-final2.btm') THEN visit__visit_id ELSE NULL END)) AS succesfull_username_recovery_BTM_count,
        SIZE(COLLECT_SET(CASE WHEN message__name IN ('Recover-1.bam') THEN visit__visit_id ELSE NULL END))
        -
        SIZE(COLLECT_SET(CASE WHEN message__name IN ('Recover-noID.bam') THEN visit__visit_id ELSE NULL END)) AS attempts_recover_username_BAM_count,
        SIZE(COLLECT_SET(CASE WHEN message__name IN ('Recover-final1.bam', 'Recover-final2.bam') THEN visit__visit_id ELSE NULL END)) AS succesfull_username_recovery_BAM_count,
        SIZE(COLLECT_SET(CASE WHEN message__name IN ('Recover-1.nbtm') THEN visit__visit_id ELSE NULL END))
        -
        SIZE(COLLECT_SET(CASE WHEN message__name IN ('Recover-noID.nbtm') THEN visit__visit_id ELSE NULL END)) AS attempts_recover_username_NBTM_count,
        SIZE(COLLECT_SET(CASE WHEN message__name IN ('Recover-final1.nbtm', 'Recover-final2.nbtm') THEN visit__visit_id ELSE NULL END)) succesfull_username_recovery_NBTM_count,

        --Reset Flow
        SIZE(COLLECT_SET(CASE WHEN message__name IN ('Reset-1.btm') THEN visit__visit_id ELSE NULL END)) AS attempts_reset_password_BTM_count,
        SIZE(COLLECT_SET(CASE WHEN message__name IN ('Reset-final.btm') THEN visit__visit_id ELSE NULL END)) AS successful_reset_password_BTM_count,
        SIZE(COLLECT_SET(CASE WHEN message__name IN ('Reset-1.bam') THEN visit__visit_id ELSE NULL END)) AS attempts_rest_password_BAM_count,
        SIZE(COLLECT_SET(CASE WHEN message__name IN ('Reset-final.bam') THEN visit__visit_id ELSE NULL END)) AS successful_reset_password_BAM_count,
        SIZE(COLLECT_SET(CASE WHEN message__name IN ('Reset-1.nbtm') THEN visit__visit_id ELSE NULL END)) AS attempts_rest_password_NBTM_count,
        SIZE(COLLECT_SET(CASE WHEN message__name IN ('Reset-final.nbtm') THEN visit__visit_id ELSE NULL END)) AS successful_reset_password_NBTM_count,

        --Recover Reset Flow
        SIZE(COLLECT_SET(CASE WHEN message__name IN ('RecoverReset-1.btm') THEN visit__visit_id ELSE NULL END)) AS attempts_recover_username_password_BTM_count,
        SIZE(COLLECT_SET(CASE WHEN message__name IN ('RecoverReset-final1.btm', 'RecoverReset-final2.btm') THEN visit__visit_id ELSE NULL END)) AS successfully_recover_username_password_BTM_count,
        SIZE(COLLECT_SET(CASE WHEN message__name IN ('RecoverReset-1.bam') THEN visit__visit_id ELSE NULL END)) AS attempts_recover_username_password_BAM_count,
        SIZE(COLLECT_SET(CASE WHEN message__name IN ('RecoverReset-final1.bam', 'RecoverReset-final2.bam') THEN visit__visit_id ELSE NULL END)) AS successfully_recover_username_password_BAM_count,
        SIZE(COLLECT_SET(CASE WHEN message__name IN ('RecoverReset-1.nbtm') THEN visit__visit_id ELSE NULL END)) AS attempts_recover_username_password_NBTM_count,
        SIZE(COLLECT_SET(CASE WHEN message__name IN ('RecoverReset-final1.nbtm', 'RecoverReset-final2.nbtm') THEN visit__visit_id ELSE NULL END)) AS successfully_recover_username_password_NBTM_count,

        SIZE(COLLECT_SET(CASE WHEN message__name = 'Save'
        AND state__view__current_page__elements__name =  'Billing Notifications'
        THEN visit__visit_id ELSE NULL END)) AS saved_bill_notifications_count,
        SIZE(COLLECT_SET(CASE WHEN message__name = 'Save'
        AND state__view__current_page__elements__name = 'Appointment Reminders'
        THEN visit__visit_id ELSE NULL END)) AS saved_appoint_reminders_count,
        --spectrum.net daily - Reschedules
        --- WHERE Custom Link = 'appointment-date-picker-confirm' HITS
        --- - Page Name = 'appointment-unable-to-reschedule-modal' Page Views
        --- + Custom Link = 'Reschedule-Confirm' HITS
        (SUM(IF(message__name = 'appointment-date-picker-confirm' AND message__category = 'Custom Link', 1, 0))
        - SUM(IF(state__view__current_page__name = 'appointment-unable-to-reschedule-modal' AND message__category = 'Page View', 1, 0)))
        + SUM(IF(message__name = 'Reschedule-Confirm' AND message__category = 'Custom Link', 1, 0)) AS rescheduled_service_appoint_count, --Updated 2017-12-14 DPrince
        --spectrum.net daily - Cancels
        --- WHERE Custom Link = 'appointment-cancel-review-cancel' HITS
        --- - Page Name = 'appointment-unable-to-cancel-modal' Page Views
        --- + Page Name = 'cancel-success-page' VISITS
        (SUM(IF(message__name = 'appointment-cancel-review-cancel' AND message__category = 'Custom Link', 1, 0))
        - SUM(IF(state__view__current_page__name = 'appointment-unable-to-cancel-modal' AND message__category = 'Page View', 1, 0)))
        + SIZE(COLLECT_SET(IF(state__view__current_page__name = 'cancel-success-page' AND message__category = 'Page View', visit__visit_id, NULL))) AS cancelled_service_appoint_count, --Updated 2017-12-14 DPrince
        SIZE(COLLECT_SET(CASE WHEN message__name = 'Save'
        AND state__view__current_page__elements__name = 'Service Alerts'
        THEN visit__visit_id ELSE NULL END)) AS saved_service_alerts_count,
        SIZE(COLLECT_SET(CASE WHEN message__name = 'Save'
        AND state__view__current_page__elements__name IN ('Email','Phone')
        THEN visit__visit_id ELSE NULL END)) AS saved_contact_information_details_count,

        --
        --
        SIZE(COLLECT_SET(IF(state__view__current_page__name IN('my-account.create-id-final.bam','my-account.create-id-final.btm') AND message__category = 'Page View', visit__visit_id,NULL))) AS new_ids_charter_count_on_net,
        SIZE(COLLECT_SET(IF(state__view__current_page__name IN('my-account.create-id-final.nbtm') AND message__category = 'Page View', visit__visit_id,NULL))) AS new_ids_charter_count_off_net,
        --
        SIZE(COLLECT_SET(IF(state__view__current_page__name IN('my-account.create-id-1.bam','my-account.create-id') AND message__category = 'Page View', visit__visit_id,NULL)))
        -
        SIZE(COLLECT_SET(IF(message__name = 'Sign-In-Now'
                        AND message__category = 'Custom Link'
                        AND state__view__current_page__name IN('my-account.create-id-2.btm','my-account.create-id-2.bam'), visit__visit_id,NULL)))
        +
        SIZE(COLLECT_SET(IF(state__view__current_page__name IN('my-account.create-id-1.nbtm') AND message__category = 'Page View', visit__visit_id,NULL)))
        -
        SIZE(COLLECT_SET(IF(message__name = 'Sign-In-Now'
                        AND message__category = 'Custom Link'
                        AND state__view__current_page__name IN('my-account.create-id-2.nbtm'), visit__visit_id,NULL)))
        -
        SIZE(COLLECT_SET(IF(state__view__current_page__name IN('Create.redirectTWC') AND message__category = 'Page View', visit__visit_id,NULL)))
        -
        SIZE(COLLECT_SET(IF(state__view__current_page__name IN('Create.redirectBHN') AND message__category = 'Page View', visit__visit_id,NULL))) AS attempts_create_id_count_all,
        SIZE(COLLECT_SET(IF(state__view__current_page__name IN('my-account.create-id-1.bam','my-account.create-id') AND message__category = 'Page View', visit__visit_id,NULL)))
        -
        SIZE(COLLECT_SET(IF(message__name = 'Sign-In-Now'
                        AND message__category = 'Custom Link'
                        AND state__view__current_page__name IN('my-account.create-id-2.btm','my-account.create-id-2.bam'), visit__visit_id,NULL))) AS attempts_create_id_count_on_net,
        SIZE(COLLECT_SET(IF(state__view__current_page__name IN('my-account.create-id-1.nbtm') AND message__category = 'Page View', visit__visit_id,NULL)))
        -
        SIZE(COLLECT_SET(IF(message__name = 'Sign-In-Now'
                        AND message__category = 'Custom Link'
                        AND state__view__current_page__name IN('my-account.create-id-2.nbtm'), visit__visit_id,NULL)))
        -
        SIZE(COLLECT_SET(IF(state__view__current_page__name IN('Create.redirectTWC') AND message__category = 'Page View', visit__visit_id,NULL)))
        -
        SIZE(COLLECT_SET(IF(state__view__current_page__name IN('Create.redirectBHN') AND message__category = 'Page View', visit__visit_id,NULL))) AS attempts_create_id_count_off_net,
        --
        --

        (SIZE(COLLECT_SET(if((state__view__current_page__name IN('Recover-1.btm','Recover-1.bam') AND message__category = 'Page View'),visit__visit_id,NULL)))
         - SIZE(COLLECT_SET(IF((state__view__current_page__name IN('Recover-noID.btm','Recover-noID.bam') AND message__category = 'Page View'),visit__visit_id,NULL))) )
        +
        (SIZE(COLLECT_SET(if((state__view__current_page__name = 'Recover-1.nbtm') AND (message__category = 'Page View'), visit__visit_id,NULL)))
        - SIZE(COLLECT_SET(IF((state__view__current_page__name = 'Recover-noID.nbtm') AND (message__category = 'Page View'), visit__visit_id,NULL)))) AS attempts_recover_username_count_all,
        --
        SIZE(COLLECT_SET(if((state__view__current_page__name IN('Recover-1.btm','Recover-1.bam') AND message__category = 'Page View'),visit__visit_id,NULL)))
        - SIZE(COLLECT_SET(IF((state__view__current_page__name IN('Recover-noID.btm','Recover-noID.bam') AND message__category = 'Page View'),visit__visit_id,NULL))) AS attempts_recover_username_count_on_net,
        --
        SIZE(COLLECT_SET(if((state__view__current_page__name = 'Recover-1.nbtm') AND (message__category = 'Page View'), visit__visit_id,NULL)))
        - SIZE(COLLECT_SET(IF((state__view__current_page__name = 'Recover-noID.nbtm') AND (message__category = 'Page View'), visit__visit_id,NULL))) AS attempts_recover_username_count_off_net,
        --
        --
        SIZE(COLLECT_SET(if((state__view__current_page__name IN('Recover-final2.btm','Recover-final1.bam','Recover-final2.bam') AND message__category = 'Page View'),visit__visit_id,NULL)))
        +
          SIZE(COLLECT_SET(if((state__view__current_page__name = 'Recover-final1.nbtm') AND (message__category = 'Page View'), visit__visit_id,NULL)))
        + SIZE(COLLECT_SET(IF(state__view__current_page__name = 'Recover-final2.nbtm' AND (message__category = 'Page View'), visit__visit_id,NULL))) AS successful_username_recovery_count_all,
        SIZE(COLLECT_SET(if((state__view__current_page__name IN('Recover-final2.btm','Recover-final1.bam','Recover-final2.bam') AND message__category = 'Page View'),visit__visit_id,NULL))) AS successful_username_recovery_count_on_net,
        SIZE(COLLECT_SET(if((state__view__current_page__name = 'Recover-final1.nbtm') AND (message__category = 'Page View'), visit__visit_id,NULL)))
        + SIZE(COLLECT_SET(IF((state__view__current_page__name = 'Recover-final2.nbtm') AND (message__category = 'Page View'), visit__visit_id,NULL))) AS successful_username_recovery_count_off_net,
        --
        --
        (SIZE(COLLECT_SET(if((state__view__current_page__name IN('Reset-1.bam','Reset-1.btm') AND message__category = 'Page View'),visit__visit_id,NULL))) )
        +
        (SIZE(COLLECT_SET(if((state__view__current_page__name = 'Reset-1.nbtm') AND (message__category = 'Page View'),visit__visit_id,NULL))) ) AS attempts_reset_password_count_all,
        SIZE(COLLECT_SET(if((state__view__current_page__name IN('Reset-1.bam','Reset-1.btm') AND message__category = 'Page View'),visit__visit_id,NULL))) AS attempts_reset_password_count_on_net,
        SIZE(COLLECT_SET(if((state__view__current_page__name = 'Reset-1.nbtm') AND (message__category = 'Page View'),visit__visit_id,NULL))) AS attempts_reset_password_count_off_net,
        --
        --
        (SIZE(COLLECT_SET(if((state__view__current_page__name IN('Reset-final.bam','Reset-final.btm') AND message__category = 'Page View'),visit__visit_id,NULL))) )
        +
        (SIZE(COLLECT_SET(if((state__view__current_page__name = 'Reset-final.nbtm') AND (message__category = 'Page View'),visit__visit_id,NULL))) ) AS successful_reset_password_count_all,
        SIZE(COLLECT_SET(if((state__view__current_page__name IN('Reset-final.bam','Reset-final.btm') AND message__category = 'Page View'),visit__visit_id,NULL))) AS successful_reset_password_count_on_net,
        SIZE(COLLECT_SET(if((state__view__current_page__name = 'Reset-final.nbtm') AND (message__category = 'Page View'),visit__visit_id,NULL))) AS successful_reset_password_count_off_net,
        --
        --
        (SIZE(COLLECT_SET(if((state__view__current_page__name IN('RecoverReset-1.btm', 'RecoverReset-1.bam') AND message__category = 'Page View'),visit__visit_id,NULL))) )
        +
        (SIZE(COLLECT_SET(if((state__view__current_page__name = 'RecoverReset-1.nbtm') AND (message__category = 'Page View'), visit__visit_id,NULL))) ) AS attempts_recover_username_password_count_all,
        SIZE(COLLECT_SET(if((state__view__current_page__name IN('RecoverReset-1.btm', 'RecoverReset-1.bam') AND message__category = 'Page View'),visit__visit_id,NULL))) AS attempts_recover_username_password_count_on_net,
        SIZE(COLLECT_SET(if((state__view__current_page__name = 'RecoverReset-1.nbtm') AND (message__category = 'Page View'), visit__visit_id,NULL))) AS attempts_recover_username_password_count_off_net,
        --
        --
        (SIZE(COLLECT_SET(if((state__view__current_page__name IN('RecoverReset-final2.btm','RecoverReset-final1.bam','RecoverReset-final2.bam') AND message__category = 'Page View'),visit__visit_id,NULL))) )
        +
        (SIZE(COLLECT_SET(if((state__view__current_page__name IN('RecoverReset-final1.nbtm','RecoverReset-final2.nbtm') AND message__category = 'Page View'), visit__visit_id,NULL))) ) AS successfully_recover_username_password_count_all,
        SIZE(COLLECT_SET(if((state__view__current_page__name IN('RecoverReset-final2.btm','RecoverReset-final1.bam','RecoverReset-final2.bam') AND message__category = 'Page View'),visit__visit_id,NULL))) AS successfully_recover_username_password_count_on_net,
        SIZE(COLLECT_SET(if((state__view__current_page__name IN('RecoverReset-final1.nbtm','RecoverReset-final2.nbtm') AND message__category = 'Page View'), visit__visit_id,NULL))) AS successfully_recover_username_password_count_off_net,
        --
        -- Equipment Reboot Completions = Reset-Equipment-Internet-Success = 'modem_router_resets'
        -- updated 2018-02-07 CL 55 tagging updated to include views of "Reset-Equipment-Internet-Success"
        SIZE(COLLECT_SET(
              IF(state__view__current_page__name
                 IN( 'Test-Connection-Internet',
                    'Test-Connection-InternetVoice',
                    'Test-Connection-Voice',
                    'Test-Connection-WiFi',
                    'Reset-Equipment-Internet-Success')
                 AND message__category = 'Page View', visit__visit_id, NULL
                )
              )
            ) AS modem_router_resets,
        --
        'L-CHTR' AS company,
        -------- Spectrum.net Daily Fields - 14 Dec 2017
        --spectrum.net daily - Login Attempts
        ---WHERE Custom Link = 'Sign-In'
        SUM(IF(message__name = 'Sign-In' AND message__category = 'Custom Link',1,0)) AS login_attempts_adobe,
        --spectrum.net daily - Authenticated Visits
        --- WHERE prop3 = 'Logged In'
        SIZE(COLLECT_SET(IF(visit__isp__status = 'Logged In',visit__visit_id,NULL))) AS authenticated_visits,
        --spectrum.net daily - Webmail visits
        --- WHERE Site Section = 'email'
        SIZE(COLLECT_SET(IF(UPPER(state__view__current_page__section) = 'EMAIL' AND message__category = 'Page View',visit__visit_id,NULL))) AS webmail_visits,
        --spectrum.net daily - Statement Views
        -- Revised 8 Feb 2018 for CL55 update
        --- add: New statement download link: pay-bill.billing-statement-download (custom link)
        --- add: (custom link): pay-bill.view-statements AND p26 contains "statementAge" -- revised again on 2018-03-07 where post prop 26 is not null.
        --- WHERE Custom Link IN('View Current Bill','Statement','View Statement')
        SUM(IF(message__name IN(
              'View Current Bill',
              'Statement',
              'View Statement',
              'pay-bill.billing-statement-download')
              AND message__category = 'Custom Link',1,0
              )
           )  +
           SUM(IF(message__name IN('pay-bill.view-statements')
                   AND message__category = 'Custom Link'
                   AND visit__settings["post_prop26"] IS NOT NULL
                  ,1,0)
           )        AS statement_views,
        --spectrum.net daily - One Time Payments
        --- WHERE Page Name IN('OneTime-noAutoPay-Credit-Confirm','OneTime-noAutoPay-Checking-Confirm','OneTime-noAutoPay-Savings-Confirm')
        SIZE(COLLECT_SET(IF(state__view__current_page__name
              IN('OneTime-noAutoPay-Credit-Confirm','OneTime-noAutoPay-Checking-Confirm','OneTime-noAutoPay-Savings-Confirm', 'pay-bill.onetime-confirmation')
              AND message__category = 'Page View',visit__visit_id,NULL))) AS one_time_payments_updated,
        --spectrum.net daily - Payments - One Time w/ AutoPay Enrollment
        --- WHERE Page Name IN('OneTime-wAutoPay-Credit-Confirm','OneTime-wAutoPay-Checking-Confirm','OneTime-wAutoPay-Savings-Confirm'
        SIZE(COLLECT_SET(IF(state__view__current_page__name
              IN('OneTime-wAutoPay-Credit-Confirm','OneTime-wAutoPay-Checking-Confirm','OneTime-wAutoPay-Savings-Confirm', 'pay-bill.onetime-confirmation-with-autopay-enrollment')
              AND message__category = 'Page View',visit__visit_id,NULL))) AS one_time_payment_with_auto_pay_updated,
        --spectrum.net daily - Payments - AutoPay Enrollment
        --- WHERE Page Name IN('AutoPay-wBalance-Credit-Confirm', 'AutoPay-noBalance-Credit-Confirm',
        --                      'AutoPay-wBalance-Checking-Confirm','AutoPay-noBalance-Checking-Confirm',
        --                      'AutoPay-wBalance-Savings-Confirm','AutoPay-noBalance-Savings-Confirm')
        SIZE(COLLECT_SET(IF(state__view__current_page__name
              IN('AutoPay-wBalance-Credit-Confirm', 'AutoPay-noBalance-Credit-Confirm',
                 'AutoPay-wBalance-Checking-Confirm','AutoPay-noBalance-Checking-Confirm',
                 'AutoPay-wBalance-Savings-Confirm','AutoPay-noBalance-Savings-Confirm', 'pay-bill.autopay-enrollment-confirmation')
              AND message__category = 'Page View',visit__visit_id,NULL))) AS auto_pay_enrollment_updated,
        --spectrum.net daily - New Identities Created incl.sub accounts  Revised 05 Jan 2017
        --- WHERE Page Name IN('my-account.create-id-final.btm','my-account.create-id-final.nbtm','my-account.create-id-final.bam') VISITS
        --- + url = 'https://www.spectrum.net/login/?targetUrl=https://www.spectrum.net/my-account/add-user#/confirmation' PAGE VIEWS
        SIZE(COLLECT_SET(IF(UPPER(state__view__current_page__name)
              IN('MY-ACCOUNT.CREATE-ID-FINAL.BTM')
              AND message__category = 'Page View',visit__visit_id,NULL)))
        +
        SIZE(COLLECT_SET(IF(UPPER(state__view__current_page__name)
              IN('MY-ACCOUNT.CREATE-ID-FINAL.NBTM')
              AND message__category = 'Page View',visit__visit_id,NULL)))
        +
        SIZE(COLLECT_SET(IF(UPPER(state__view__current_page__name)
              IN('MY-ACCOUNT.CREATE-ID-FINAL.BAM')
              AND message__category = 'Page View',visit__visit_id,NULL)))
        +
        SUM(IF(state__view__current_page__page_id
              = 'https://www.spectrum.net/my-account/add-user#/confirmation'
              AND message__category = 'Page View',1,0)) AS new_ids_incl_sub_accts,
        --spectrum.net daily - Combined Credential Recoveries
        --- WHERE Page Name IN('Recover-final1.btm' , 'RecoverReset-final1.bam' ,
        --                     'Recover-final1.bam' , 'RecoverReset-final1.nbtm' ,
        --                     'Recover-final1.nbtm' , 'Recover-final2.bam' ,
        --                     'RecoverReset-final2.bam' , 'Reset-final.bam' ,
        --                     'Recover-final2.nbtm' , 'RecoverReset-final2.nbtm' ,
        --                     'Reset-final.btm' , 'Recover-final2.btm' ,
        --                     'RecoverReset-final2.btm' , 'Reset-final.nbtm') VISITS SUMMED p/Page
          SIZE(COLLECT_SET(IF(UPPER(state__view__current_page__name) = 'RECOVERRESET-FINAL1.BTM' AND message__category = 'Page View', visit__visit_id,NULL)))
          + SIZE(COLLECT_SET(IF(UPPER(state__view__current_page__name) = 'RECOVER-FINAL1.BTM' AND message__category = 'Page View', visit__visit_id,NULL)))
          + SIZE(COLLECT_SET(IF(UPPER(state__view__current_page__name) = 'RECOVERRESET-FINAL1.BAM' AND message__category = 'Page View', visit__visit_id,NULL)))
          + SIZE(COLLECT_SET(IF(UPPER(state__view__current_page__name) = 'RECOVER-FINAL1.BAM' AND message__category = 'Page View', visit__visit_id,NULL)))
          + SIZE(COLLECT_SET(IF(UPPER(state__view__current_page__name) = 'RECOVERRESET-FINAL1.NBTM' AND message__category = 'Page View', visit__visit_id,NULL)))
          + SIZE(COLLECT_SET(IF(UPPER(state__view__current_page__name) = 'RECOVER-FINAL1.NBTM' AND message__category = 'Page View', visit__visit_id,NULL)))
          + SIZE(COLLECT_SET(IF(UPPER(state__view__current_page__name) = 'RECOVER-FINAL2.BAM' AND message__category = 'Page View', visit__visit_id,NULL)))
          + SIZE(COLLECT_SET(IF(UPPER(state__view__current_page__name) = 'RECOVERRESET-FINAL2.BAM' AND message__category = 'Page View', visit__visit_id,NULL)))
          + SIZE(COLLECT_SET(IF(UPPER(state__view__current_page__name) = 'RESET-FINAL.BAM' AND message__category = 'Page View', visit__visit_id,NULL)))
          + SIZE(COLLECT_SET(IF(UPPER(state__view__current_page__name) = 'RECOVER-FINAL2.NBTM' AND message__category = 'Page View', visit__visit_id,NULL)))
          + SIZE(COLLECT_SET(IF(UPPER(state__view__current_page__name) = 'RECOVERRESET-FINAL2.NBTM' AND message__category = 'Page View', visit__visit_id,NULL)))
          + SIZE(COLLECT_SET(IF(UPPER(state__view__current_page__name) = 'RESET-FINAL.BTM' AND message__category = 'Page View', visit__visit_id,NULL)))
          + SIZE(COLLECT_SET(IF(UPPER(state__view__current_page__name) = 'RECOVER-FINAL2.BTM' AND message__category = 'Page View', visit__visit_id,NULL)))
          + SIZE(COLLECT_SET(IF(UPPER(state__view__current_page__name) = 'RECOVERRESET-FINAL2.BTM' AND message__category = 'Page View', visit__visit_id,NULL)))
          + SIZE(COLLECT_SET(IF(UPPER(state__view__current_page__name) = 'RESET-FINAL.NBTM' AND message__category = 'Page View', visit__visit_id,NULL)))
          AS combined_credential_recoveries,
          --- spectrum.net daily - Search Results Clicked
          SUM(IF((UPPER(message__name) RLIKE '.*SEARCH-RESULT-.*' AND message__category = 'Custom Link')
            OR (UPPER(message__name) IN('SEARCH-VIEW-ALL','SEARCH-THE-WEB') AND message__category = 'Custom Link'),1,0)) AS search_results_clicked,
            --spectrum.net daily - IVA Opens
            --- WHERE Site Section = 'support'
            SUM(IF(UPPER(message__name) = 'ASK-SPECTRUM' AND message__category = 'Custom Link',1,0))
            +
            SUM(IF(UPPER(message__name) = 'OPEN-ASK-SPECTRUM' AND message__category = 'Custom Link',1,0)) AS iva_opens,
            -- added 2018-01-03, pseudo device_type by way of OS
            SIZE(COLLECT_SET(case when visit__device__operating_system     RLIKE '.*iOS|Android.*' THEN visit__visit_id ELSE NULL END)) as count_os_is_iOSAndroid,
            SIZE(COLLECT_SET(case when visit__device__operating_system NOT RLIKE '.*iOS|Android.*' THEN visit__visit_id ELSE NULL END)) as count_os_not_iOSAndroid,
            ${env:pf}

FROM  (SELECt  ${env:apl} as ${env:pf},
               message__name,
               message__category,
               visit__device__operating_system,
               visit__isp__status,
               visit__settings,
               visit__visit_id,
               state__view__current_page__name,
               state__view__current_page__elements__name,
               state__view__current_page__page_id,
               state__view__current_page__section
       FROM asp_v_net_events ne
       LEFT JOIN ${env:LKP_db}.${env:fm_lkp} fm on ne.partition_date = fm.partition_date
       WHERE (ne.partition_date >= ("${env:START_DATE}") AND ne.partition_date < ("${env:END_DATE}"))
     ) dictionary
GROUP BY ${env:pf}
;
--Separate Qry to resolve overly heavy visits
USE ${env:ENVIRONMENT};

set hive.auto.convert.join=false;

SET hive.tez.container.size=10682;



-- Drop and rebuild visits temp table visits  --
DROP TABLE IF EXISTS ${env:TMP_db}.net_chtr_visit_${env:CADENCE};

-- For troubleshooting queries, remove 'TEMPORARY'
CREATE TABLE IF NOT EXISTS ${env:TMP_db}.net_chtr_visit_${env:CADENCE}
AS

SELECT
'L-CHTR' AS company,
SIZE(COLLECT_SET(IF(visit__visit_id IS NOT NULL, visit__visit_id, NULL))) AS visits,
${env:pf}
FROM  (SELECt  ${env:apl} as ${env:pf},
               visit__visit_id
       FROM asp_v_net_events ne
       LEFT JOIN ${env:LKP_db}.${env:fm_lkp} fm on ne.partition_date = fm.partition_date
       WHERE (ne.partition_date >= ("${env:START_DATE}") AND ne.partition_date < ("${env:END_DATE}"))
     ) dictionary
GROUP BY ${env:pf}
;

SELECT '*****-- End L-CHTR net_products_agg_monthly Temp Table Insert --*****' -- 931.971 seconds
;

-- End L-CHTR net_products_agg_monthly Temp Table Insert --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------

----- Insert into agg table -------
INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)

SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (  SELECT  'total_page_views_count' as metric,
                   total_page_views_count AS value,
                   'page_views' as unit,
                   'resi' as domain,
                   company,
                   ${env:pf}
          FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
      ) q;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

          SELECT  'my_account_page_views_count' as metric,
                   my_account_page_views_count AS value,
                   'instances' as unit,
                   'resi' as domain,
                   company,
                   ${env:pf}
          FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
          ) q;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

          SELECT  'support_page_views_count' as metric, --SpectrumDaily also used AS 'Support Page Views'
                   support_page_views_count AS value,
                   'page_views' as unit,
                   'resi' as domain,
                   company,
                   ${env:pf}
          FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
          ) q;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

          SELECT  'ask_charter_requests_count' as metric,
                   ask_charter_requests_count AS value,
                   'visits' as unit,
                   'resi' as domain,
                   company,
                   ${env:pf}
          FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
          ) q;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

          SELECT  'refresh_requests_count' as metric, --Also In SpectrumDaily AS 'Refresh STB'
                   refresh_requests_count AS value,
                   'visits' as unit,
                   'resi' as domain,
                   company,
                   ${env:pf}
          FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
          ) q;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

          SELECT  'video_page_views_count' as metric,
                   video_page_views_count AS value,
                   'visits' as unit,
                   'resi' as domain,
                   company,
                   ${env:pf}
          FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
          ) q;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

          SELECT  'video_plays_count' as metric,
                   video_plays_count AS value,
                   'instances' as unit,
                   'resi' as domain,
                   company,
                   ${env:pf}
          FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
          ) q;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

          SELECT  'new_ids_charter_count' as metric,
                   new_ids_charter_count AS value,
                   'visits' as unit,
                   'resi' as domain,
                   company,
                   ${env:pf}
          FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
          ) q;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

          SELECT  'new_ids_charter_count_bam' as metric,
                   new_ids_charter_count_bam AS value,
                   'visits' as unit,
                   'resi' as domain,
                   company,
                   ${env:pf}
          FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
          ) q;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

          SELECT  'new_ids_charter_count_btm' as metric,
                   new_ids_charter_count_btm AS value,
                   'visits' as unit,
                   'resi' as domain,
                   company,
                   ${env:pf}
          FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
          ) q;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

          SELECT  'new_ids_charter_count_nbtm' as metric,
                   new_ids_charter_count_nbtm AS value,
                   'visits' as unit,
                   'resi' as domain,
                   company,
                   ${env:pf}
          FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
          ) q;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

          SELECT  'attempts_create_id_count_bam' as metric,
                   attempts_create_id_count_bam AS value,
                   'visits' as unit,
                   'resi' as domain,
                   company,
                   ${env:pf}
          FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
          ) q;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

          SELECT  'attempts_create_id_count_btm' as metric,
                   attempts_create_id_count_btm AS value,
                   'visits' as unit,
                   'resi' as domain,
                   company,
                   ${env:pf}
          FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
          ) q;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

          SELECT  'attempts_create_id_count_nbtm' as metric,
                   attempts_create_id_count_nbtm AS value,
                   'visits' as unit,
                   'resi' as domain,
                   company,
                   ${env:pf}
          FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
          ) q;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

          SELECT  'new_ids_not_charter_count' as metric,
                   new_ids_not_charter_count AS value,
                   'visits' as unit,
                   'resi' as domain,
                   company,
                   ${env:pf}
          FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
          ) q;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

          SELECT  'total_attempts_id_off_count' as metric,
                   total_attempts_id_off_count AS value,
                   'visits' as unit,
                   'resi' as domain,
                   company,
                   ${env:pf}
          FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
          ) q;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

          SELECT  'attempts_recover_username_btm_count' as metric,
                   attempts_recover_username_btm_count AS value,
                   'visits' as unit,
                   'resi' as domain,
                   company,
                   ${env:pf}
          FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
          ) q;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

          SELECT  'succesfull_username_recovery_btm_count' as metric,
                   succesfull_username_recovery_btm_count AS value,
                   'visits' as unit,
                   'resi' as domain,
                   company,
                   ${env:pf}
          FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
          ) q;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

          SELECT  'attempts_recover_username_bam_count' as metric,
                   attempts_recover_username_bam_count AS value,
                   'visits' as unit,
                   'resi' as domain,
                   company,
                   ${env:pf}
          FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
          ) q;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

          SELECT  'succesfull_username_recovery_bam_count' as metric,
                   succesfull_username_recovery_bam_count AS value,
                   'visits' as unit,
                   'resi' as domain,
                   company,
                   ${env:pf}
          FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
          ) q;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

          SELECT  'attempts_recover_username_nbtm_count' as metric,
                   attempts_recover_username_nbtm_count AS value,
                   'visits' as unit,
                   'resi' as domain,
                   company,
                   ${env:pf}
          FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
          ) q;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

          SELECT  'succesfull_username_recovery_nbtm_count' as metric,
                   succesfull_username_recovery_nbtm_count AS value,
                   'visits' as unit,
                   'resi' as domain,
                   company,
                   ${env:pf}
          FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
          ) q;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

          SELECT  'attempts_reset_password_btm_count' as metric,
                   attempts_reset_password_btm_count AS value,
                   'visits' as unit,
                   'resi' as domain,
                   company,
                   ${env:pf}
          FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
          ) q;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

          SELECT  'successful_reset_password_btm_count' as metric,
                   successful_reset_password_btm_count AS value,
                   'visits' as unit,
                   'resi' as domain,
                   company,
                   ${env:pf}
          FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
          ) q;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

          SELECT  'attempts_rest_password_bam_count' as metric,
                   attempts_rest_password_bam_count AS value,
                   'visits' as unit,
                   'resi' as domain,
                   company,
                   ${env:pf}
          FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
          ) q;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

          SELECT  'successful_reset_password_bam_count' as metric,
                   successful_reset_password_bam_count AS value,
                   'visits' as unit,
                   'resi' as domain,
                   company,
                   ${env:pf}
          FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
          ) q;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

          SELECT  'attempts_rest_password_nbtm_count' as metric,
                   attempts_rest_password_nbtm_count AS value,
                   'visits' as unit,
                   'resi' as domain,
                   company,
                   ${env:pf}
          FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
          ) q;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

          SELECT  'successful_reset_password_nbtm_count' as metric,
                   successful_reset_password_nbtm_count AS value,
                   'visits' as unit,
                   'resi' as domain,
                   company,
                   ${env:pf}
          FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
          ) q;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

          SELECT  'attempts_recover_username_password_btm_count' as metric,
                   attempts_recover_username_password_btm_count AS value,
                   'visits' as unit,
                   'resi' as domain,
                   company,
                   ${env:pf}
          FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
          ) q;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

          SELECT  'successfully_recover_username_password_btm_count' as metric,
                   successfully_recover_username_password_btm_count AS value,
                   'visits' as unit,
                   'resi' as domain,
                   company,
                   ${env:pf}
          FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
          ) q;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

          SELECT  'attempts_recover_username_password_bam_count' as metric,
                   attempts_recover_username_password_bam_count AS value,
                   'visits' as unit,
                   'resi' as domain,
                   company,
                   ${env:pf}
          FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
          ) q;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

          SELECT  'successfully_recover_username_password_bam_count' as metric,
                   successfully_recover_username_password_bam_count AS value,
                   'visits' as unit,
                   'resi' as domain,
                   company,
                   ${env:pf}
          FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
          ) q;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

          SELECT  'attempts_recover_username_password_nbtm_count' as metric,
                   attempts_recover_username_password_nbtm_count AS value,
                   'visits' as unit,
                   'resi' as domain,
                   company,
                   ${env:pf}
          FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
          ) q;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

          SELECT  'successfully_recover_username_password_nbtm_count' as metric,
                   successfully_recover_username_password_nbtm_count AS value,
                   'visits' as unit,
                   'resi' as domain,
                   company,
                   ${env:pf}
          FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
          ) q;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

          SELECT  'saved_bill_notifications_count' as metric,
                   saved_bill_notifications_count AS value,
                   'visits' as unit,
                   'resi' as domain,
                   company,
                   ${env:pf}
          FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
          ) q;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

          SELECT  'saved_appoint_reminders_count' as metric,
                   saved_appoint_reminders_count AS value,
                   'visits' as unit,
                   'resi' as domain,
                   company,
                   ${env:pf}
          FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
          ) q;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

          SELECT  'rescheduled_service_appoint_count' as metric, --SpectrumDaily also used AS 'Reschedules'
                   rescheduled_service_appoint_count AS value,
                   'visits' as unit,
                   'resi' as domain,
                   company,
                   ${env:pf}
          FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
          ) q;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

          SELECT  'cancelled_service_appoint_count' as metric, -- SpectrumDaily also used AS 'Cancels'
                   cancelled_service_appoint_count AS value,
                   'visits' as unit,
                   'resi' as domain,
                   company,
                   ${env:pf}
          FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
          ) q;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

          SELECT  'saved_service_alerts_count' as metric,
                   saved_service_alerts_count AS value,
                   'visits' as unit,
                   'resi' as domain,
                   company,
                   ${env:pf}
          FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
          ) q;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

          SELECT  'saved_contact_information_details_count' as metric,
                   saved_contact_information_details_count AS value,
                   'visits' as unit,
                   'resi' as domain,
                   company,
                   ${env:pf}
          FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
          ) q;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

          SELECT  'new_ids_charter_count_on_net' as metric,
                   new_ids_charter_count_on_net AS value,
                   'visits' as unit,
                   'resi' as domain,
                   company,
                   ${env:pf}
          FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
          ) q;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

          SELECT  'new_ids_charter_count_off_net' as metric,
                   new_ids_charter_count_off_net AS value,
                   'visits' as unit,
                   'resi' as domain,
                   company,
                   ${env:pf}
          FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
          ) q;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

          SELECT  'attempts_create_id_count_all' as metric,
                   attempts_create_id_count_all AS value,
                   'visits' as unit,
                   'resi' as domain,
                   company,
                   ${env:pf}
          FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
          ) q;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

          SELECT  'attempts_create_id_count_on_net' as metric,
                   attempts_create_id_count_on_net AS value,
                   'visits' as unit,
                   'resi' as domain,
                   company,
                   ${env:pf}
          FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
          ) q;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

          SELECT  'attempts_create_id_count_off_net' as metric,
                   attempts_create_id_count_off_net AS value,
                   'visits' as unit,
                   'resi' as domain,
                   company,
                   ${env:pf}
          FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
          ) q;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

          SELECT  'attempts_recover_username_count_all' as metric,
                   attempts_recover_username_count_all AS value,
                   'visits' as unit,
                   'resi' as domain,
                   company,
                   ${env:pf}
          FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
          ) q;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

          SELECT  'attempts_recover_username_count_on_net' as metric,
                   attempts_recover_username_count_on_net AS value,
                   'visits' as unit,
                   'resi' as domain,
                   company,
                   ${env:pf}
          FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
          ) q;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

          SELECT  'attempts_recover_username_count_off_net' as metric,
                   attempts_recover_username_count_off_net AS value,
                   'visits' as unit,
                   'resi' as domain,
                   company,
                   ${env:pf}
          FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
          ) q;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

          SELECT  'successful_username_recovery_count_all' as metric,
                   successful_username_recovery_count_all AS value,
                   'visits' as unit,
                   'resi' as domain,
                   company,
                   ${env:pf}
          FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
          ) q;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

          SELECT  'successful_username_recovery_count_on_net' as metric,
                   successful_username_recovery_count_on_net AS value,
                   'visits' as unit,
                   'resi' as domain,
                   company,
                   ${env:pf}
          FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
          ) q;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

          SELECT  'successful_username_recovery_count_off_net' as metric,
                   successful_username_recovery_count_off_net AS value,
                   'visits' as unit,
                   'resi' as domain,
                   company,
                   ${env:pf}
          FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
          ) q;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

          SELECT  'attempts_reset_password_count_all' as metric,
                   attempts_reset_password_count_all AS value,
                   'visits' as unit,
                   'resi' as domain,
                   company,
                   ${env:pf}
          FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
          ) q;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

          SELECT  'attempts_reset_password_count_on_net' as metric,
                   attempts_reset_password_count_on_net AS value,
                   'visits' as unit,
                   'resi' as domain,
                   company,
                   ${env:pf}
          FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
          ) q;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

          SELECT  'attempts_reset_password_count_off_net' as metric,
                   attempts_reset_password_count_off_net AS value,
                   'visits' as unit,
                   'resi' as domain,
                   company,
                   ${env:pf}
          FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
          ) q;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

          SELECT  'successful_reset_password_count_all' as metric,
                   successful_reset_password_count_all AS value,
                   'visits' as unit,
                   'resi' as domain,
                   company,
                   ${env:pf}
          FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
          ) q;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

          SELECT  'successful_reset_password_count_on_net' as metric,
                   successful_reset_password_count_on_net AS value,
                   'visits' as unit,
                   'resi' as domain,
                   company,
                   ${env:pf}
          FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
          ) q;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

          SELECT  'successful_reset_password_count_off_net' as metric,
                   successful_reset_password_count_off_net AS value,
                   'visits' as unit,
                   'resi' as domain,
                   company,
                   ${env:pf}
          FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
          ) q;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

          SELECT  'attempts_recover_username_password_count_all' as metric,
                   attempts_recover_username_password_count_all AS value,
                   'visits' as unit,
                   'resi' as domain,
                   company,
                   ${env:pf}
          FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
          ) q;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

          SELECT  'attempts_recover_username_password_count_on_net' as metric,
                   attempts_recover_username_password_count_on_net AS value,
                   'visits' as unit,
                   'resi' as domain,
                   company,
                   ${env:pf}
          FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
          ) q;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

          SELECT  'attempts_recover_username_password_count_off_net' as metric,
                   attempts_recover_username_password_count_off_net AS value,
                   'visits' as unit,
                   'resi' as domain,
                   company,
                   ${env:pf}
          FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
          ) q;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

          SELECT  'successfully_recover_username_password_count_all' as metric,
                   successfully_recover_username_password_count_all AS value,
                   'visits' as unit,
                   'resi' as domain,
                   company,
                   ${env:pf}
          FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
          ) q;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

          SELECT  'successfully_recover_username_password_count_on_net' as metric,
                   successfully_recover_username_password_count_on_net AS value,
                   'visits' as unit,
                   'resi' as domain,
                   company,
                   ${env:pf}
          FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
          ) q;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

          SELECT  'successfully_recover_username_password_count_off_net' as metric,
                   successfully_recover_username_password_count_off_net AS value,
                   'visits' as unit,
                   'resi' as domain,
                   company,
                   ${env:pf}
         FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
                   ) q;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

         SELECT  'modem_router_resets' as metric,  -- Also in SpectrumDaily as 'Equipment Reboot Completions'
                  modem_router_resets AS value,
                  'visits' as unit,
                  'resi' as domain,
                  company,
                  ${env:pf}
        FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
        ) q
;

-- 14 Dec 2017 additions below for Spectrum Daily

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

         SELECT  'login_attempts_adobe' as metric,
                  login_attempts_adobe AS value,
                  'instances' as unit,
                  'resi' as domain,
                  company,
                  ${env:pf}
        FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
        ) q
;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

         SELECT  'authenticated_visits' as metric,
                  authenticated_visits AS value,
                  'visits' as unit,
                  'resi' as domain,
                  company,
                  ${env:pf}
        FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
        ) q
;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

         SELECT  'webmail_visits' as metric,
                  webmail_visits AS value,
                  'visits' as unit,
                  'resi' as domain,
                  company,
                  ${env:pf}
        FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
        ) q
;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

         SELECT  'statement_views' as metric,
                  statement_views AS value,
                  'instances' as unit,
                  'resi' as domain,
                  company,
                  ${env:pf}
        FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
        ) q
;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

         SELECT  'one_time_payments_updated' as metric,
                  one_time_payments_updated AS value,
                  'visits' as unit,
                  'resi' as domain,
                  company,
                  ${env:pf}
        FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
        ) q
;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

         SELECT  'one_time_payment_with_auto_pay_updated' as metric,
                  one_time_payment_with_auto_pay_updated AS value,
                  'visits' as unit,
                  'resi' as domain,
                  company,
                  ${env:pf}
        FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
        ) q
;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

         SELECT  'auto_pay_enrollment_updated' as metric,
                  auto_pay_enrollment_updated AS value,
                  'visits' as unit,
                  'resi' as domain,
                  company,
                  ${env:pf}
        FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
        ) q
;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

         SELECT  'new_ids_incl_sub_accts' as metric,
                  new_ids_incl_sub_accts AS value,
                  'visits' as unit,
                  'resi' as domain,
                  company,
                  ${env:pf}
        FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
        ) q
;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

         SELECT  'combined_credential_recoveries' as metric,
                  combined_credential_recoveries AS value,
                  'visits' as unit,
                  'resi' as domain,
                  company,
                  ${env:pf}
        FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
        ) q
;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

         SELECT  'search_results_clicked' as metric,
                  search_results_clicked AS value,
                  'instances' as unit,
                  'resi' as domain,
                  company,
                  ${env:pf}
        FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
        ) q
;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

         SELECT  'iva_opens' as metric,
                  iva_opens AS value,
                  'instances' as unit,
                  'resi' as domain,
                  company,
                  ${env:pf}
        FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
        ) q
;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (

         SELECT  'count_os_is_iOSAndroid' as metric,
                  count_os_is_iOSAndroid AS value,
                  'visits' as unit,
                  'resi' as domain,
                  company,
                  ${env:pf}
        FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
        ) q
;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (
         SELECT  'count_os_not_iOSAndroid' as metric,
                  count_os_not_iOSAndroid AS value,
                  'visits' as unit,
                  'resi' as domain,
                  company,
                  ${env:pf}
        FROM ${env:TMP_db}.net_chtr_${env:CADENCE}
        ) q
;

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (
         SELECT  'visits' as metric,
                  visits AS value,
                  'visits' as unit,
                  'resi' as domain,
                  company,
                  ${env:pf}
        FROM ${env:TMP_db}.net_chtr_visit_${env:CADENCE}
        ) q
;
