USE ${env:ENVIRONMENT};

set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager;
set hive.support.concurrency=false;
set hive.tez.container.size=16000;

set hive.auto.convert.join=false;
set hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set mapreduce.input.fileinputformat.split.minsize=524288000;
set mapreduce.input.fileinputformat.split.maxsize=524288000;
set hive.optimize.sort.dynamic.partition = false;

INSERT OVERWRITE TABLE asp_idm_metric_agg PARTITION (denver_date)

-- Dimensions go here
select
  state__view__current_page__page_name AS page_name,
  state__view__current_page__app_section as app_section,
  visit__user__role as user_role,
  visit__device__enc_uuid as device_id,
  visit__visit_id as visit_id,
  visit__application_details__application_type AS application_type,
  LOWER(visit__device__device_type) AS device_type,
  visit__application_details__app_version AS app_version,
  visit__login__logged_in AS logged_in,
  LOWER(visit__application_details__application_name) AS application_name,
  'All OS Names' AS os_name,
  visit__device__operating_system AS operating_system,
  visit__device__browser__name AS browser_name,
  visit__device__browser__version AS browser_version,
  state__view__current_page__ui_responsive_breakpoint as browser_size_breakpoint,
  visit__device__device_form_factor AS form_factor,
  parse_url(visit__application_details__referrer_link,'HOST') AS referrer_link,

-------------------------------------------------
--- Begin Rosetta-Generated Metric Defintions ---
---------------- IDM Metric Agg -----------------
-------------------------------------------------
        SUM(IF(visit__application_details__application_name ='IDManagement' AND message__name = 'apiCall'  AND application__api__response_code='400'  AND application__api__response_text='BAD_REQUEST' , 1, 0)) AS api_badrequest_counts,
        SUM(IF(visit__application_details__application_name ='IDManagement' AND message__name = 'apiCall'  AND application__api__response_code='500'  AND application__api__response_text='GENERAL_FAILURE' , 1, 0)) AS api_general_failure_counts,
        SUM(IF(visit__application_details__application_name ='IDManagement' AND message__name = 'apiCall'  AND application__api__response_code='401'  AND application__api__response_text='INSUFFICIENT_PERMISSION' , 1, 0)) AS api_insufficient_permission_counts,
        SUM(IF(visit__application_details__application_name ='IDManagement' AND message__name = 'apiCall'  AND application__api__response_code='500'  AND (application__api__response_text='INTERNAL_SERVER ERROR' OR application__api__response_text='Internal Server Error' OR application__api__response_text='SERVER_ERROR') , 1, 0)) AS api_internal_servererror_counts,
        SUM(IF(visit__application_details__application_name ='IDManagement' AND message__name = 'apiCall'  AND application__api__response_code='403'  AND application__api__response_text='INVALID_TOKEN' , 1, 0)) AS api_invalid_token_counts,
        SUM(IF(visit__application_details__application_name ='IDManagement' AND message__name = 'apiCall'  AND application__api__response_code='500'  AND application__api__response_text='INVITE_EXPIRED' , 1, 0)) AS api_invite_expired_counts,
        SUM(IF(visit__application_details__application_name ='IDManagement' AND message__name = 'apiCall'  AND application__api__response_code='403'  AND application__api__response_text='OUTSIDE_US' , 1, 0)) AS api_outside_us_counts,
        SUM(IF(visit__application_details__application_name ='IDManagement' AND message__name = 'apiCall'  AND application__api__response_code='403'  AND application__api__response_text='PROXY' , 1, 0)) AS api_proxy_counts,
        SUM(IF(visit__application_details__application_name ='IDManagement' AND message__name = 'apiCall'  AND application__api__response_code='403'  AND application__api__response_text='TOKEN_EXPIRED' , 1, 0)) AS api_token_expired_counts,
        SUM(IF(visit__application_details__application_name ='IDManagement' AND message__name = 'apiCall'  AND application__api__response_code='429'  AND application__api__response_text='TOO_MANY_ATTEMPTS' , 1, 0)) AS api_too_many_attempts_counts,
        SUM(IF(visit__application_details__application_name ='IDManagement' AND message__name = 'apiCall'  AND application__api__response_code='401'  AND application__api__response_text='Unauthorized' , 1, 0)) AS api_unauthorized_counts,
        SUM(IF(visit__application_details__application_name ='IDManagement' AND message__name = 'selectAction'  AND operation__operation_type='buttonClick'  AND state__view__current_page__elements__standardized_name='checkYourInfoCancel' , 1, 0)) AS buttonclick_checkyourinfocancel_counts,
        SUM(IF(visit__application_details__application_name ='IDManagement' AND message__name = 'selectAction'  AND operation__operation_type='buttonClick'  AND state__view__current_page__elements__standardized_name='checkYourInfoConfirm' , 1, 0)) AS buttonclick_checkyourinfoconfirm_counts,
        SUM(IF(visit__application_details__application_name ='IDManagement' AND message__name = 'selectAction'  AND operation__operation_type='buttonClick'  AND state__view__current_page__elements__standardized_name='chooseYourUsernameBack' , 1, 0)) AS buttonclick_chooseyourusernameback_counts,
        SUM(IF(visit__application_details__application_name ='IDManagement' AND message__name = 'selectAction'  AND operation__operation_type='buttonClick'  AND state__view__current_page__elements__standardized_name='chooseYourUsernameNext' , 1, 0)) AS buttonclick_chooseyourusernamenext_counts,
        SUM(IF(visit__application_details__application_name ='IDManagement' AND message__name = 'selectAction'  AND operation__operation_type='buttonClick'  AND state__view__current_page__elements__standardized_name='closeButton' , 1, 0)) AS buttonclick_closebutton_counts,
        SUM(IF(visit__application_details__application_name ='IDManagement' AND message__name = 'selectAction'  AND operation__operation_type='buttonClick'  AND state__view__current_page__elements__standardized_name='confirmYourAccountCancel' , 1, 0)) AS buttonclick_confirmyouraccountcancel_counts,
        SUM(IF(visit__application_details__application_name ='IDManagement' AND message__name = 'selectAction'  AND operation__operation_type='buttonClick'  AND state__view__current_page__elements__standardized_name='confirmYourAccountNext' , 1, 0)) AS buttonclick_confirmyouraccountnext_counts,
        SUM(IF(visit__application_details__application_name ='IDManagement' AND message__name = 'selectAction'  AND state__view__current_page__page_name = 'enterYourPassword'  AND state__view__current_page__app_section='primaryCreate'  AND state__view__current_page__elements__standardized_name='enterYourPasswordSaveAndSignIn'  AND operation__operation_type='buttonClick' , 1, 0)) AS buttonclick_create_new_id_counts,
        SUM(IF(visit__application_details__application_name ='IDManagement' AND message__name = 'selectAction'  AND operation__operation_type='buttonClick'  AND state__view__current_page__elements__standardized_name='enterYourPasswordCancel' , 1, 0)) AS buttonclick_enteryourpasswordcancel_counts,
        SUM(IF(visit__application_details__application_name ='IDManagement' AND message__name = 'selectAction'  AND operation__operation_type='buttonClick'  AND state__view__current_page__elements__standardized_name='enterYourPasswordSaveAndSignIn' , 1, 0)) AS buttonclick_enteryourpasswordsaveandsignin_counts,
        SUM(IF(visit__application_details__application_name ='IDManagement' AND message__name = 'selectAction'  AND operation__operation_type='buttonClick'  AND state__view__current_page__elements__standardized_name='footer-contact-contactUS' , 1, 0)) AS buttonclick_footer_contact_contactus_counts,
        SUM(IF(visit__application_details__application_name ='IDManagement' AND message__name = 'selectAction'  AND operation__operation_type='buttonClick'  AND state__view__current_page__elements__standardized_name='footer-legal-californiaPrivacyRights' , 1, 0)) AS buttonclick_footer_legal_californiaprivacyrights_counts,
        SUM(IF(visit__application_details__application_name ='IDManagement' AND message__name = 'selectAction'  AND operation__operation_type='buttonClick'  AND state__view__current_page__elements__standardized_name='footer-legal-californiaResidentDontSellMyInfo' , 1, 0)) AS buttonclick_footer_legal_californiaresidentdontsellmyinfo_counts,
        SUM(IF(visit__application_details__application_name ='IDManagement' AND message__name = 'selectAction'  AND operation__operation_type='buttonClick'  AND state__view__current_page__elements__standardized_name='footer-legal-goToAssist' , 1, 0)) AS buttonclick_footer_legal_gotoassist_counts,
        SUM(IF(visit__application_details__application_name ='IDManagement' AND message__name = 'selectAction'  AND operation__operation_type='buttonClick'  AND state__view__current_page__elements__standardized_name='footer-legal-policies' , 1, 0)) AS buttonclick_footer_legal_policies_counts,
        SUM(IF(visit__application_details__application_name ='IDManagement' AND message__name = 'selectAction'  AND operation__operation_type='buttonClick'  AND state__view__current_page__elements__standardized_name='footer-legal-privacyRights' , 1, 0)) AS buttonclick_footer_legal_privacyrights_counts,
        SUM(IF(visit__application_details__application_name ='IDManagement' AND message__name = 'selectAction'  AND operation__operation_type='buttonClick'  AND state__view__current_page__elements__standardized_name='forcedPasswordResetCancel' , 1, 0)) AS buttonclick_forcedpasswordresetcancel_counts,
        SUM(IF(visit__application_details__application_name ='IDManagement' AND message__name = 'selectAction'  AND operation__operation_type='buttonClick'  AND state__view__current_page__elements__standardized_name='forcedPasswordResetContinue' , 1, 0)) AS buttonclick_forcedpasswordresetcontinue_counts,
        SUM(IF(visit__application_details__application_name ='IDManagement' AND message__name = 'selectAction'  AND operation__operation_type='buttonClick'  AND state__view__current_page__elements__standardized_name='multipleAddressesFoundConfirmAnotherWay' , 1, 0)) AS buttonclick_multipleaddressesfoundconfirmanotherway_counts,
        SUM(IF(visit__application_details__application_name ='IDManagement' AND message__name = 'selectAction'  AND operation__operation_type='buttonClick'  AND state__view__current_page__elements__standardized_name='multipleAddressesFoundNext' , 1, 0)) AS buttonclick_multipleaddressesfoundnext_counts,
        SUM(IF(visit__application_details__application_name ='IDManagement' AND message__name = 'selectAction'  AND operation__operation_type='buttonClick'  AND state__view__current_page__elements__standardized_name='multipleUsernamesFoundConfirmAnotherWay' , 1, 0)) AS buttonclick_multipleusernamesfoundconfirmanotherway_counts,
        SUM(IF(visit__application_details__application_name ='IDManagement' AND message__name = 'selectAction'  AND operation__operation_type='buttonClick'  AND state__view__current_page__elements__standardized_name='multipleUsernamesFoundNext' , 1, 0)) AS buttonclick_multipleusernamesfoundnext_counts,
        SUM(IF(visit__application_details__application_name ='IDManagement' AND message__name = 'selectAction'  AND operation__operation_type='buttonClick'  AND state__view__current_page__elements__standardized_name='needMoreInfoNext' , 1, 0)) AS buttonclick_needmoreinfonext_counts,
        SUM(IF(visit__application_details__application_name ='IDManagement' AND message__name = 'selectAction'  AND state__view__current_page__page_name = 'resetYourPassword'  AND state__view__current_page__app_section='recover'  AND state__view__current_page__elements__standardized_name='resetYourPasswordSaveAndSignIn'  AND operation__operation_type='buttonClick' , 1, 0)) AS buttonclick_recover_password_success,
        SUM(IF(visit__application_details__application_name ='IDManagement' AND message__name = 'selectAction'  AND state__view__current_page__page_name = 'username'  AND state__view__current_page__app_section='recover'  AND state__view__current_page__elements__standardized_name='userNameSignIn'  AND operation__operation_type='buttonClick' , 1, 0)) AS buttonclick_recover_username_success_counts,
        SUM(IF(visit__application_details__application_name ='IDManagement' AND message__name = 'selectAction'  AND operation__operation_type='buttonClick'  AND state__view__current_page__elements__standardized_name='resetYourPasswordCancel' , 1, 0)) AS buttonclick_resetyourpasswordcancel_counts,
        SUM(IF(visit__application_details__application_name ='IDManagement' AND message__name = 'selectAction'  AND operation__operation_type='buttonClick'  AND state__view__current_page__elements__standardized_name='resetYourPasswordSaveAndSignIn' , 1, 0)) AS buttonclick_resetyourpasswordsaveandsignin_counts,
        SUM(IF(visit__application_details__application_name ='IDManagement' AND message__name = 'selectAction'  AND operation__operation_type='buttonClick'  AND state__view__current_page__elements__standardized_name='termsAndConditionsClose' , 1, 0)) AS buttonclick_termsandconditionsclose_counts,
        SUM(IF(visit__application_details__application_name ='IDManagement' AND message__name = 'selectAction'  AND operation__operation_type='buttonClick'  AND state__view__current_page__elements__standardized_name='updateSecurityQuestionDropDown' , 1, 0)) AS buttonclick_updatesecurityquestiondropdown_counts,
        SUM(IF(visit__application_details__application_name ='IDManagement' AND message__name = 'selectAction'  AND operation__operation_type='buttonClick'  AND state__view__current_page__elements__standardized_name='updateSecurityQuestionNext' , 1, 0)) AS buttonclick_updatesecurityquestionnext_counts,
        SUM(IF(visit__application_details__application_name ='IDManagement' AND message__name = 'selectAction'  AND operation__operation_type='buttonClick'  AND state__view__current_page__elements__standardized_name='usernameResetPassword' , 1, 0)) AS buttonclick_usernameresetpassword_counts,
        SUM(IF(visit__application_details__application_name ='IDManagement' AND message__name = 'selectAction'  AND operation__operation_type='buttonClick'  AND state__view__current_page__elements__standardized_name='userNameSignIn' , 1, 0)) AS buttonclick_usernamesignin_counts,
        SUM(IF(visit__application_details__application_name ='IDManagement' AND message__name = 'selectAction'  AND operation__operation_type='buttonClick'  AND state__view__current_page__elements__standardized_name='verificationCodeNext' , 1, 0)) AS buttonclick_verificationcodenext_counts,
        SUM(IF(visit__application_details__application_name ='IDManagement' AND message__name = 'selectAction'  AND operation__operation_type='buttonClick'  AND state__view__current_page__elements__standardized_name='verifyAnotherWay' , 1, 0)) AS buttonclick_verifyanotherway_counts,
        SUM(IF(visit__application_details__application_name ='IDManagement' AND message__name = 'selectAction'  AND operation__operation_type='buttonClick'  AND state__view__current_page__elements__standardized_name='verifyYourIdentityModalOk' , 1, 0)) AS buttonclick_verifyyouridentitymodalok_counts,
        SUM(IF(visit__application_details__application_name ='IDManagement' AND message__name = 'selectAction'  AND operation__operation_type='buttonClick'  AND state__view__current_page__elements__standardized_name='verifyYourIdentityNext' , 1, 0)) AS buttonclick_verifyyouridentitynext_counts,
        SUM(IF(visit__application_details__application_name ='IDManagement' AND message__name = 'selectAction'  AND operation__operation_type='buttonClick'  AND state__view__current_page__elements__standardized_name='welcomeGetStarted' , 1, 0)) AS buttonclick_welcomegetstarted_counts,
        SUM(IF(visit__application_details__application_name ='IDManagement' AND message__name = 'pageView' , 1, 0)) AS pageview_counts,
-------------------------------------------------
---- End Rosetta-Generated Metric Defintions ----
---------------- IDM Metric Agg -----------------
-------------------------------------------------
visit__account__enc_account_number as portals_unique_acct_key,
prod.epoch_converter(received__timestamp, 'America/Denver') as denver_date
FROM core_quantum_events_portals_v
WHERE partition_date_hour_utc >= '${env:START_DATE_TZ}'
  AND partition_date_hour_utc <  '${env:END_DATE_TZ}'
  and partition_date_hour_utc >= '2019-12-11_07'
and visit__application_details__application_name ='IDManagement'
and message__name IN('pageView', 'selectAction', 'error', 'apiCall')

group by prod.epoch_converter(received__timestamp, 'America/Denver'),
    epoch_converter(received__timestamp, 'America/Denver'),
    state__view__current_page__page_name,
    state__view__current_page__app_section,
    visit__user__role,
    visit__device__enc_uuid,
    visit__visit_id,
    visit__application_details__application_type,
    LOWER(visit__device__device_type),
    visit__application_details__app_version,
    visit__login__logged_in,
    LOWER(visit__application_details__application_name),
    'All OS Names',
    visit__device__operating_system,
    visit__device__browser__name,
    visit__device__browser__version,
    state__view__current_page__ui_responsive_breakpoint,
    visit__device__device_form_factor,
    parse_url(visit__application_details__referrer_link,'HOST'),
    visit__account__enc_account_number
;
