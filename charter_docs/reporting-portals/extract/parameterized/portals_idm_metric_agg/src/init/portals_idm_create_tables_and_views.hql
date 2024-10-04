USE ${env:TMP_db};

CREATE TABLE IF NOt EXISTS ${env:ReprocessDateTable_daily} (run_date STRING);
INSERT OVERWRITE TABLE ${env:ReprocessDateTable_daily} VALUES('${env:RUN_DATE}');

-----------------------------------------

USE ${env:ENVIRONMENT};

CREATE TABLE IF NOT EXISTS asp_idm_metric_agg
(
  page_name                 STRING,
  app_section               STRING,
  user_role                 STRING,
  device_id                 STRING,
  visit_id                  STRING,
  application_type          STRING,
  device_type               STRING,
  app_version               STRING,
  logged_in                 STRING,
  application_name          STRING,
  os_name                   STRING,
  operating_system          STRING,
  browser_name              STRING,
  browser_version           STRING,
  browser_size_breakpoint   STRING,
  form_factor               STRING,
  referrer_link             STRING,
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
  api_badrequest_counts INT,
  api_general_failure_counts INT,
  api_insufficient_permission_counts INT,
  api_internal_servererror_counts INT,
  api_invalid_token_counts INT,
  api_invite_expired_counts INT,
  api_outside_us_counts INT,
  api_proxy_counts INT,
  api_token_expired_counts INT,
  api_too_many_attempts_counts INT,
  api_unauthorized_counts INT,
  buttonclick_checkyourinfocancel_counts INT,
  buttonclick_checkyourinfoconfirm_counts INT,
  buttonclick_chooseyourusernameback_counts INT,
  buttonclick_chooseyourusernamenext_counts INT,
  buttonclick_closebutton_counts INT,
  buttonclick_confirmyouraccountcancel_counts INT,
  buttonclick_confirmyouraccountnext_counts INT,
  buttonclick_create_new_id_counts INT,
  buttonclick_enteryourpasswordcancel_counts INT,
  buttonclick_enteryourpasswordsaveandsignin_counts INT,
  buttonclick_footer_contact_contactus_counts INT,
  buttonclick_footer_legal_californiaprivacyrights_counts INT,
  buttonclick_footer_legal_californiaresidentdontsellmyinfo_counts INT,
  buttonclick_footer_legal_gotoassist_counts INT,
  buttonclick_footer_legal_policies_counts INT,
  buttonclick_footer_legal_privacyrights_counts INT,
  buttonclick_forcedpasswordresetcancel_counts INT,
  buttonclick_forcedpasswordresetcontinue_counts INT,
  buttonclick_multipleaddressesfoundconfirmanotherway_counts INT,
  buttonclick_multipleaddressesfoundnext_counts INT,
  buttonclick_multipleusernamesfoundconfirmanotherway_counts INT,
  buttonclick_multipleusernamesfoundnext_counts INT,
  buttonclick_needmoreinfonext_counts INT,
  buttonclick_recover_password_success INT,
  buttonclick_recover_username_success_counts INT,
  buttonclick_resetyourpasswordcancel_counts INT,
  buttonclick_resetyourpasswordsaveandsignin_counts INT,
  buttonclick_termsandconditionsclose_counts INT,
  buttonclick_updatesecurityquestiondropdown_counts INT,
  buttonclick_updatesecurityquestionnext_counts INT,
  buttonclick_usernameresetpassword_counts INT,
  buttonclick_usernamesignin_counts INT,
  buttonclick_verificationcodenext_counts INT,
  buttonclick_verifyanotherway_counts INT,
  buttonclick_verifyyouridentitymodalok_counts INT,
  buttonclick_verifyyouridentitynext_counts INT,
  buttonclick_welcomegetstarted_counts INT,
  pageview_counts INT,


--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
  portals_unique_acct_key       STRING
)
PARTITIONED BY (denver_date STRING)
TBLPROPERTIES ('retention_policy'='Aggregate - With PII (3 Years)')
;


--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
