--------------------------------------------------------------------
-- Unique HHs Logged In (provides total sub count that logged in)
---CHTR, TWC, BHN
--------------------------------------------------------------------

SELECT
  source_app
--  , COUNT(trace_id) as total_login_attempts -- trace_ids (aka transaction_id) are unique per attempt
--  , COUNT(if(is_success = true,trace_id,null)) as total_successes  --boolean flag if the attempt was successful or not
--  , SIZE(COLLECT_SET(IF(username_aes256 IS NOT NULL,username_aes256, NULL))) as users_logged_in -- mimicking what was done on the old version
  , SIZE(COLLECT_SET(IF(account_number_aes256 IS NOT NULL,account_number_aes256, NULL))) as hhs_logged_in -- mimicking what was done on the old version
  , CASE WHEN is_success = true then footprint ELSE 'UNAUTH' END as company

FROM asp_v_federated_identity
WHERE (partition_date_denver >= '2018-09-22' AND partition_date_denver < '2018-10-22')
      AND LOWER(source_app) IN('portals-idp','myspectrum','portals-idp-comm')
      AND account_number_aes256 IS NOT NULL
      AND is_success = true
GROUP BY source_app, case when is_success = true then footprint else 'UNAUTH' END
;

--Fiscal July------- CHTR: 81,755 | TWC: 115,363 | BHN: 25,363  << -- Different from report due to date treatment from auth team.
-- ^^These are correct and report numbers are not^^
--Fiscal August----- CHTR: 85,683 | TWC: 125,916 | BHN: 27,046
--Fiscal September-- CHTR: 86,928 | TWC: 129,251 | BHN: 28,462
--Fiscal October---- CHTR: 84,343 | TWC: 122,660 | BHN: 25,600
--Fiscal November--- CHTR: 90,561 | TWC: 135,268 | BHN: 27,303

-----------------------------------------------------------------------------------------
-- Quantum CHTR SMB Prod Monthly Definitions
-----------------------------------------------------------------------------------------

SELECT

--Split by MSO
---This is likely NOT needed in future reporting for SMB
--visit__account__details__mso AS legacy_footprint, --Comment this out if you don't need MSO

--Web Sessions (visits) (unique visit count by either tagging suite OR MSO split) << -- Current metric is pulling "site_unique_auth" and we should change it to "site_unique"
---2018-11 fiscal: 667,157 (Ran this on 2018-11-22 and again on 2018-11-23 and numbers matched)
---2018-10 fiscal: 608,827
---2018-09 fiscal: 643,061
SIZE(COLLECT_SET(visit__visit_id)) AS web_sessions_visits,

--Support Section Page Views (count of page views where section = 'Support') << -- counts.tsv definition used
---2018-11 fiscal: 334,899
---2018-10 fiscal: 315,802
---2018-09 fiscal: 200,020
SUM(IF(LOWER(visit__application_details__application_name)= LOWER('SMB')
   AND LOWER(message__name) = LOWER('pageView')
   AND LOWER(state__view__current_page__app_section) = LOWER('support'), 1, 0)) AS support_section_page_views,

--Online Statement Views << -- counts.tsv definitions used
---2018-11 fiscal: 28,052
---2018-10 fiscal: 26,424
---2018-09 fiscal: 26,974
SUM(IF(LOWER(visit__application_details__application_name) = LOWER('SMB')
   AND LOWER(message__name) = LOWER('selectAction')
   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('downloadStatement'), 1, 0)) AS view_statements,

--One-Time Payments (OTP) (**NOTE** This is combined visits that did OTP + visits that signed up for AP during payment)
---^^^Using counts.tsv definition: combined_payment_successes^^^
---2018-11 fiscal: 43,904
---2018-10 fiscal: 39,380
---2018-09 fiscal: 29,631 (Report has 30,076... % difference of -1.479585%)
SIZE(COLLECT_SET(IF(LOWER(visit__application_details__application_name) = LOWER('SMB')
              AND ((LOWER(message__name) = LOWER('featureStop'))
                OR (LOWER(message__name) = LOWER('pageView')
                AND LOWER(state__view__current_page__page_name) = LOWER('autoPayEnrollOtpSuccess'))), visit__visit_id, NULL))) AS one_time_payments,

--Set-up Auto-Payments (AP) (**NOTE** This is combined visits that did AP Setup + visits that signed up for AP during payment)
---^^^Using counts.tsv definitions:'otp_with_autopay_successes' & 'auto_pay_setup_successes'^^^
---2018-11 fiscal: 2,575
---2018-10 fiscal: 2,413
---2018-09 fiscal: 1,867 (Report has 1,893... % difference of -1.3926085%)
SIZE(COLLECT_SET(IF(LOWER(visit__application_details__application_name) = LOWER('SMB')
                AND LOWER(message__name) = LOWER('pageView')
                AND LOWER(state__view__current_page__page_name) = LOWER('autoPayEnrollOtpSuccess'), visit__visit_id, NULL)))
+
SIZE(COLLECT_SET(IF(LOWER(visit__application_details__application_name) = LOWER('SMB')
                AND LOWER(message__name) = LOWER('pageView')
                AND LOWER(state__view__current_page__page_name) = LOWER('autoPayEnrollSuccess'), visit__visit_id, NULL))) AS set_up_auto_payments,

--New IDs Created (Not displaying in fiscal Sep or fiscal Oct report but need to display for fiscal Nov)
---Using counts.tsv metric: new_admin_accounts_created
---2018-11 fiscal: 831
---2018-10 fiscal:  77
---2018-09 fiscal:
SIZE(COLLECT_SET
      (CASE
        WHEN LOWER(visit__application_details__application_name) = LOWER('SMB')
         AND LOWER(message__name) = LOWER('applicationActivity')
         AND LOWER(operation__operation_type) = LOWER('userAddSuccessBanner')
         AND LOWER(message__context) = LOWER('Administrator')
        THEN visit__visit_id
        ELSE NULL
      END
      )
    ) AS ids_created,

--New ID Creation Attempts (Included definition for posterity... but AAcosta Implores us NOT to use Attempt and Success)
---^^^Using counts.tsv definition: 'new_ids_created_attempts'^^^
---2018-11 fiscal: 833
---2018-10 fiscal: 709
---2018-09 fiscal:
--OLD DEFINITION
--SIZE(COLLECT_SET(IF(LOWER(visit__application_details__application_name) = LOWER('SMB')
--                AND LOWER(message__name) = LOWER('selectAction')
--                AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('createUsername'), visit__visit_id, NULL))) AS id_creation_attempts,
--NEW DEFINITION
SIZE(COLLECT_SET(CASE
                  WHEN LOWER(visit__application_details__application_name) = LOWER('SMB')
                   AND LOWER(message__name) = LOWER('selectAction')
                   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('addNewUserConfirm')
                   AND LOWER(message__context) = LOWER('Administrator')
                  THEN visit__visit_id
                  ELSE NULL
                END)) AS id_creation_attempts,

--New Sub Users Created (NEW DEFINITION FROM Not displaying in fiscal Sep report but need to display for fiscal Oct)
---2018-11 fiscal: 220
---2018-10 fiscal:
---2018-09 fiscal:
SIZE(COLLECT_SET(CASE
                  WHEN LOWER(visit__application_details__application_name) = LOWER('SMB')
                   AND LOWER(message__name) = LOWER('applicationActivity')
                   AND LOWER(operation__operation_type) = LOWER('userAddSuccessBanner')
                   AND LOWER(message__context) = LOWER('Standard')
                  THEN visit__visit_id
                  ELSE NULL
                 END)) AS new_sub_users_created,

--New Sub User Creation Attempts (Included definition for posterity... but AAcosta Implores us NOT to use Attempt and Success)
---2018-11 fiscal: 227
---2018-10 fiscal: 267
---2018-09 fiscal: 250
--OLD DEFINITION
--SIZE(COLLECT_SET(IF(LOWER(visit__application_details__application_name) = LOWER('SMB')
--                AND LOWER(message__name) = LOWER('selectAction')
--                AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('addNewUserConfirm')
--                AND LOWER(state__view__current_page__elements__element_string_value) = LOWER('Standard'), visit__visit_id, NULL))) AS new_sub_user_creation_attempts,
--NEW DEFINITION
SIZE(COLLECT_SET(CASE
                  WHEN LOWER(visit__application_details__application_name) = LOWER('SMB')
                   AND LOWER(message__name) = LOWER('selectAction')
                   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('addNewUserConfirm')
                   AND LOWER(message__context) = LOWER('Standard')
                  THEN visit__visit_id
                  ELSE NULL
                 END)) AS new_sub_user_creation_attempts,

--Total Successful ID Recovery (This is NOT displayed in Prod Monthly... but should be displayed if we can get the definition in Quantum)
---2018-11 fiscal:
---2018-10 fiscal:
---2018-09 fiscal:
--AS total_successful_id_recovery

--Total ID Recovery Attempts (**NOTE** No definition in counts.tsv currently. Definition provided by AAcosta)
---2018-11 fiscal: 8,320
---2018-10 fiscal: 7,859
---2018-09 fiscal: 7,683
SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name)= LOWER('SMB')
                 AND LOWER(message__name) = LOWER('pageView')
                 AND LOWER(state__view__current_page__page_name) = LOWER('recoveryEmailSent'), visit__visit_id, NULL))) AS id_recovery_attempts,

--Successful Password Resets (Added definition to potentially change layout of SMB prod monthly.
---If we can get attempts and successes for TWC and BHN as well, then we'll change to show the success count and the % Success)
---2018-11 fiscal: 2,412
---2018-10 fiscal: 2,319
---2018-09 fiscal:
SIZE(COLLECT_SET(CASE
                  WHEN LOWER(visit__application_details__application_name)= LOWER('SMB')
                   AND LOWER(message__name) = LOWER('selectAction')
                   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('passwordRecoveryComplete')
                  THEN visit__visit_id
                  ELSE NULL
                 END)) AS successful_password_resets,

--Password Reset Attempts (**NOTE** No definition in counts.tsv currently. Definition provided by AAcosta)
---2018-11 fiscal: 2,932
---2018-10 fiscal: 2,934
---2018-09 fiscal: 2,851
SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')
                 AND LOWER(message__name) = LOWER('selectAction')
                 AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('resetAccountPassword'), visit__visit_id, Null))) AS password_reset_attempts,

--Canceled Service Appointments (Used to be displayed "Cancelled" but Doug has relented and gone back to Americanized spelling)
---2018-11 fiscal: 50
---2018-10 fiscal: 74
---2018-09 fiscal: 66
SIZE(COLLECT_SET(IF(LOWER(visit__application_details__application_name) = LOWER('SMB')
                AND LOWER(message__name) = LOWER('pageView')
                AND LOWER(state__view__current_page__page_name) = LOWER('cancelAppointmentSuccess'), visit__visit_id, NULL))) AS canceled_appointments,

--Rescheduled Service Appointments (Counting instances of appointment reschedules)
---2018-11 fiscal: 49
---2018-10 fiscal: 36
---2018-09 fiscal: 49
SIZE(COLLECT_SET(IF(LOWER(visit__application_details__application_name) = LOWER('SMB')
                AND LOWER(message__name) = LOWER('pageView')
                AND LOWER(state__view__current_page__page_name) = LOWER('rescheduleAppointmentSuccess'), visit__visit_id, NULL))) AS rescheduled_appointments,

'This concludes Quantum CHTR SMB Definitions pt1' AS report_summary

FROM prod.asp_v_venona_events_portals_smb
WHERE (partition_date_hour_utc >= '2018-10-22_06' AND partition_date_hour_utc < '2018-11-22_06')

--GROUP BY visit__account__details__mso --Comment this out if you don't need MSO
;

-----------------------------------------------------------------------------------------
-- Quantum CHTR SMB Prod Monthly Definitions pt2 (New Admin and New Sub Users)
--**NOTE** No longer needed  DON'T use the below!
-----------------------------------------------------------------------------------------

--SELECT
--company,
----New IDs Created (Not displaying in fiscal Sep report but need to display for fiscal Oct)
-----2018-10 fiscal:   770
-----2018-09 fiscal: 1,205 (1,205 / 47,208 = 2.55253347% Success)
-----^^^Can't show this until we resolve the issue^^^
--SUM(b.admin_attempt) AS new_ids_created,
--
----New Sub Users Created (Not displaying in fiscal Sep report but need to display for fiscal Oct)
-----2018-10 fiscal:  76
-----2018-09 fiscal: 627 (627 / 250 = 250.8% Success)
-----^^^Can't show this until we resolve the issue^^^
--SUM(b.sub_attempt) AS new_sub_users_created,
--        'asp' AS platform,
--        'sb' AS domain,
--        fiscal_month as year_fiscal_month_denver,
--        'This concludes Quantum CHTR SMB Definitions pt2' AS report_summary
--
--FROM prod.asp_v_venona_events_portals_smb a
--left join
--  (  SELECT COALESCE (visit__account__details__mso,'Unknown') as company,
--            visit__visit_id,
--            message__sequence_number,
--            visit__account__enc_account_number,
--            SUM(case when state__view__current_page__elements__element_string_value = 'Administrator' then 1 else 0 end) AS admin_attempt,
--            SUM(case when state__view__current_page__elements__element_string_value = 'Standard' then 1 else 0 end) AS sub_attempt,
--            epoch_converter(cast(received__timestamp as bigint),'America/Denver') as date_denver
--      FROM  prod.asp_v_venona_events_portals_smb
--      WHERE (partition_date_hour_utc >= '2018-09-22_06'
--      AND partition_date_hour_utc <  '2018-10-22_06')
--       AND message__name = 'selectAction'
--       AND state__view__current_page__elements__standardized_name = 'addNewUserConfirm'
--      group by  visit__visit_id,
--                message__sequence_number,
--                visit__account__enc_account_number,
--                COALESCE (visit__account__details__mso,'Unknown'),
--                epoch_converter(cast(received__timestamp as bigint),'America/Denver')
--) b
--on  a.visit__visit_id = b.visit__visit_id
--LEFT JOIN prod_lkp.chtr_fiscal_month ON date_denver = partition_date
--where (partition_date_hour_utc >= '2018-09-22_06'
--  AND partition_date_hour_utc <  '2018-10-22_06')
--  AND message__name = 'apiCall'
--  AND application__api__api_name = 'sbNetMemberEdgeV2MembersCreate'
--  AND (application__api__response_code = 'SUCCESS' or application__api__response_code RLIKE '2.*')
--group by fiscal_month,
--         company
--;
