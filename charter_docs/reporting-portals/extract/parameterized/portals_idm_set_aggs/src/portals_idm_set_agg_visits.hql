-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
--
--  Cadence parameterized Portals set aggregation HQL
--
--  '${env:grain}' AS grain
--  '${env:label_date_denver}',
--  "${env:START_DATE}" "${env:END_DATE}"
--  "${env:ProcessTimestamp}"  "${env:ProcessUser}"
--
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------

USE ${env:ENVIRONMENT};
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager;
set hive.support.concurrency=false;
set hive.tez.container.size=16000;
set hive.auto.convert.join=false;
set hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
--
-- visits metrics
--
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------

INSERT INTO ${env:TMP_db}.venona_set_agg_idm_stage_visits_${env:execid}
SELECT
  page_name,
  app_section,
  user_role,
  device_id,
  visit_id,
  application_type,
  device_type,
  app_version,
  logged_in,
  application_name,
  os_name,
  operating_system,
  browser_name,
  browser_version,
  browser_size_breakpoint,
  form_factor,
  referrer_link,
  grouping_id,
  metric_name,
  metric_value,
  '${env:ProcessTimestamp}' as process_date_time_denver,
  '${env:ProcessUser}' AS process_identity,
  unit_type,
  '${env:grain}' AS grain,
  label_date_denver
FROM
  (
  SELECT
    '${env:label_date_denver}' AS label_date_denver,
    page_name,
    app_section,
    user_role,
    device_id,
    visit_id,
    application_type,
    device_type,
    app_version,
    logged_in,
    application_name,
    os_name,
    operating_system,
    browser_name,
    browser_version,
    browser_size_breakpoint,
    form_factor,
    referrer_link,
    grouping_id,
    unit_type,
      MAP(

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
     'api_badrequest_counts', SUM(api_badrequest_counts),
     'api_general_failure_counts', SUM(api_general_failure_counts),
     'api_insufficient_permission_counts', SUM(api_insufficient_permission_counts),
     'api_internal_servererror_counts', SUM(api_internal_servererror_counts),
     'api_invalid_token_counts', SUM(api_invalid_token_counts),
     'api_invite_expired_counts', SUM(api_invite_expired_counts),
     'api_outside_us_counts', SUM(api_outside_us_counts),
     'api_proxy_counts', SUM(api_proxy_counts),
     'api_token_expired_counts', SUM(api_token_expired_counts),
     'api_too_many_attempts_counts', SUM(api_too_many_attempts_counts),
     'api_unauthorized_counts', SUM(api_unauthorized_counts),
     'buttonclick_checkyourinfocancel_counts', SUM(buttonclick_checkyourinfocancel_counts),
     'buttonclick_checkyourinfoconfirm_counts', SUM(buttonclick_checkyourinfoconfirm_counts),
     'buttonclick_chooseyourusernameback_counts', SUM(buttonclick_chooseyourusernameback_counts),
     'buttonclick_chooseyourusernamenext_counts', SUM(buttonclick_chooseyourusernamenext_counts),
     'buttonclick_closebutton_counts', SUM(buttonclick_closebutton_counts),
     'buttonclick_confirmyouraccountcancel_counts', SUM(buttonclick_confirmyouraccountcancel_counts),
     'buttonclick_confirmyouraccountnext_counts', SUM(buttonclick_confirmyouraccountnext_counts),
     'buttonclick_create_new_id_counts', SUM(buttonclick_create_new_id_counts),
     'buttonclick_enteryourpasswordcancel_counts', SUM(buttonclick_enteryourpasswordcancel_counts),
     'buttonclick_enteryourpasswordsaveandsignin_counts', SUM(buttonclick_enteryourpasswordsaveandsignin_counts),
     'buttonclick_footer_contact_contactus_counts', SUM(buttonclick_footer_contact_contactus_counts),
     'buttonclick_footer_legal_californiaprivacyrights_counts', SUM(buttonclick_footer_legal_californiaprivacyrights_counts),
     'buttonclick_footer_legal_californiaresidentdontsellmyinfo_counts', SUM(buttonclick_footer_legal_californiaresidentdontsellmyinfo_counts),
     'buttonclick_footer_legal_gotoassist_counts', SUM(buttonclick_footer_legal_gotoassist_counts),
     'buttonclick_footer_legal_policies_counts', SUM(buttonclick_footer_legal_policies_counts),
     'buttonclick_footer_legal_privacyrights_counts', SUM(buttonclick_footer_legal_privacyrights_counts),
     'buttonclick_forcedpasswordresetcancel_counts', SUM(buttonclick_forcedpasswordresetcancel_counts),
     'buttonclick_forcedpasswordresetcontinue_counts', SUM(buttonclick_forcedpasswordresetcontinue_counts),
     'buttonclick_multipleaddressesfoundconfirmanotherway_counts', SUM(buttonclick_multipleaddressesfoundconfirmanotherway_counts),
     'buttonclick_multipleaddressesfoundnext_counts', SUM(buttonclick_multipleaddressesfoundnext_counts),
     'buttonclick_multipleusernamesfoundconfirmanotherway_counts', SUM(buttonclick_multipleusernamesfoundconfirmanotherway_counts),
     'buttonclick_multipleusernamesfoundnext_counts', SUM(buttonclick_multipleusernamesfoundnext_counts),
     'buttonclick_needmoreinfonext_counts', SUM(buttonclick_needmoreinfonext_counts),
     'buttonclick_recover_password_success', SUM(buttonclick_recover_password_success),
     'buttonclick_recover_username_success_counts', SUM(buttonclick_recover_username_success_counts),
     'buttonclick_resetyourpasswordcancel_counts', SUM(buttonclick_resetyourpasswordcancel_counts),
     'buttonclick_resetyourpasswordsaveandsignin_counts', SUM(buttonclick_resetyourpasswordsaveandsignin_counts),
     'buttonclick_termsandconditionsclose_counts', SUM(buttonclick_termsandconditionsclose_counts),
     'buttonclick_updatesecurityquestiondropdown_counts', SUM(buttonclick_updatesecurityquestiondropdown_counts),
     'buttonclick_updatesecurityquestionnext_counts', SUM(buttonclick_updatesecurityquestionnext_counts),
     'buttonclick_usernameresetpassword_counts', SUM(buttonclick_usernameresetpassword_counts),
     'buttonclick_usernamesignin_counts', SUM(buttonclick_usernamesignin_counts),
     'buttonclick_verificationcodenext_counts', SUM(buttonclick_verificationcodenext_counts),
     'buttonclick_verifyanotherway_counts', SUM(buttonclick_verifyanotherway_counts),
     'buttonclick_verifyyouridentitymodalok_counts', SUM(buttonclick_verifyyouridentitymodalok_counts),
     'buttonclick_verifyyouridentitynext_counts', SUM(buttonclick_verifyyouridentitynext_counts),
     'buttonclick_welcomegetstarted_counts', SUM(buttonclick_welcomegetstarted_counts),
     'pageview_counts', SUM(pageview_counts)

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

    ) AS tmp_map
  FROM
    (
    SELECT
      page_name,
      app_section,
      user_role,
      device_id,
      visit_id,
      application_type,
      device_type,
      app_version,
      logged_in,
      application_name,
      os_name,
      operating_system,
      browser_name,
      browser_version,
      browser_size_breakpoint,
      form_factor,
      referrer_link,
      unit_type,
      unit_identifier,
      CAST(grouping__id AS INT) AS grouping_id,

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
        IF(SUM(api_badrequest_counts) > 0, 1, 0) AS api_badrequest_counts,
        IF(SUM(api_general_failure_counts) > 0, 1, 0) AS api_general_failure_counts,
        IF(SUM(api_insufficient_permission_counts) > 0, 1, 0) AS api_insufficient_permission_counts,
        IF(SUM(api_internal_servererror_counts) > 0, 1, 0) AS api_internal_servererror_counts,
        IF(SUM(api_invalid_token_counts) > 0, 1, 0) AS api_invalid_token_counts,
        IF(SUM(api_invite_expired_counts) > 0, 1, 0) AS api_invite_expired_counts,
        IF(SUM(api_outside_us_counts) > 0, 1, 0) AS api_outside_us_counts,
        IF(SUM(api_proxy_counts) > 0, 1, 0) AS api_proxy_counts,
        IF(SUM(api_token_expired_counts) > 0, 1, 0) AS api_token_expired_counts,
        IF(SUM(api_too_many_attempts_counts) > 0, 1, 0) AS api_too_many_attempts_counts,
        IF(SUM(api_unauthorized_counts) > 0, 1, 0) AS api_unauthorized_counts,
        IF(SUM(buttonclick_checkyourinfocancel_counts) > 0, 1, 0) AS buttonclick_checkyourinfocancel_counts,
        IF(SUM(buttonclick_checkyourinfoconfirm_counts) > 0, 1, 0) AS buttonclick_checkyourinfoconfirm_counts,
        IF(SUM(buttonclick_chooseyourusernameback_counts) > 0, 1, 0) AS buttonclick_chooseyourusernameback_counts,
        IF(SUM(buttonclick_chooseyourusernamenext_counts) > 0, 1, 0) AS buttonclick_chooseyourusernamenext_counts,
        IF(SUM(buttonclick_closebutton_counts) > 0, 1, 0) AS buttonclick_closebutton_counts,
        IF(SUM(buttonclick_confirmyouraccountcancel_counts) > 0, 1, 0) AS buttonclick_confirmyouraccountcancel_counts,
        IF(SUM(buttonclick_confirmyouraccountnext_counts) > 0, 1, 0) AS buttonclick_confirmyouraccountnext_counts,
        IF(SUM(buttonclick_create_new_id_counts) > 0, 1, 0) AS buttonclick_create_new_id_counts,
        IF(SUM(buttonclick_enteryourpasswordcancel_counts) > 0, 1, 0) AS buttonclick_enteryourpasswordcancel_counts,
        IF(SUM(buttonclick_enteryourpasswordsaveandsignin_counts) > 0, 1, 0) AS buttonclick_enteryourpasswordsaveandsignin_counts,
        IF(SUM(buttonclick_footer_contact_contactus_counts) > 0, 1, 0) AS buttonclick_footer_contact_contactus_counts,
        IF(SUM(buttonclick_footer_legal_californiaprivacyrights_counts) > 0, 1, 0) AS buttonclick_footer_legal_californiaprivacyrights_counts,
        IF(SUM(buttonclick_footer_legal_californiaresidentdontsellmyinfo_counts) > 0, 1, 0) AS buttonclick_footer_legal_californiaresidentdontsellmyinfo_counts,
        IF(SUM(buttonclick_footer_legal_gotoassist_counts) > 0, 1, 0) AS buttonclick_footer_legal_gotoassist_counts,
        IF(SUM(buttonclick_footer_legal_policies_counts) > 0, 1, 0) AS buttonclick_footer_legal_policies_counts,
        IF(SUM(buttonclick_footer_legal_privacyrights_counts) > 0, 1, 0) AS buttonclick_footer_legal_privacyrights_counts,
        IF(SUM(buttonclick_forcedpasswordresetcancel_counts) > 0, 1, 0) AS buttonclick_forcedpasswordresetcancel_counts,
        IF(SUM(buttonclick_forcedpasswordresetcontinue_counts) > 0, 1, 0) AS buttonclick_forcedpasswordresetcontinue_counts,
        IF(SUM(buttonclick_multipleaddressesfoundconfirmanotherway_counts) > 0, 1, 0) AS buttonclick_multipleaddressesfoundconfirmanotherway_counts,
        IF(SUM(buttonclick_multipleaddressesfoundnext_counts) > 0, 1, 0) AS buttonclick_multipleaddressesfoundnext_counts,
        IF(SUM(buttonclick_multipleusernamesfoundconfirmanotherway_counts) > 0, 1, 0) AS buttonclick_multipleusernamesfoundconfirmanotherway_counts,
        IF(SUM(buttonclick_multipleusernamesfoundnext_counts) > 0, 1, 0) AS buttonclick_multipleusernamesfoundnext_counts,
        IF(SUM(buttonclick_needmoreinfonext_counts) > 0, 1, 0) AS buttonclick_needmoreinfonext_counts,
        IF(SUM(buttonclick_recover_password_success) > 0, 1, 0) AS buttonclick_recover_password_success,
        IF(SUM(buttonclick_recover_username_success_counts) > 0, 1, 0) AS buttonclick_recover_username_success_counts,
        IF(SUM(buttonclick_resetyourpasswordcancel_counts) > 0, 1, 0) AS buttonclick_resetyourpasswordcancel_counts,
        IF(SUM(buttonclick_resetyourpasswordsaveandsignin_counts) > 0, 1, 0) AS buttonclick_resetyourpasswordsaveandsignin_counts,
        IF(SUM(buttonclick_termsandconditionsclose_counts) > 0, 1, 0) AS buttonclick_termsandconditionsclose_counts,
        IF(SUM(buttonclick_updatesecurityquestiondropdown_counts) > 0, 1, 0) AS buttonclick_updatesecurityquestiondropdown_counts,
        IF(SUM(buttonclick_updatesecurityquestionnext_counts) > 0, 1, 0) AS buttonclick_updatesecurityquestionnext_counts,
        IF(SUM(buttonclick_usernameresetpassword_counts) > 0, 1, 0) AS buttonclick_usernameresetpassword_counts,
        IF(SUM(buttonclick_usernamesignin_counts) > 0, 1, 0) AS buttonclick_usernamesignin_counts,
        IF(SUM(buttonclick_verificationcodenext_counts) > 0, 1, 0) AS buttonclick_verificationcodenext_counts,
        IF(SUM(buttonclick_verifyanotherway_counts) > 0, 1, 0) AS buttonclick_verifyanotherway_counts,
        IF(SUM(buttonclick_verifyyouridentitymodalok_counts) > 0, 1, 0) AS buttonclick_verifyyouridentitymodalok_counts,
        IF(SUM(buttonclick_verifyyouridentitynext_counts) > 0, 1, 0) AS buttonclick_verifyyouridentitynext_counts,
        IF(SUM(buttonclick_welcomegetstarted_counts) > 0, 1, 0) AS buttonclick_welcomegetstarted_counts,
        IF(SUM(pageview_counts) > 0, 1, 0) AS pageview_counts

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

    FROM
      (
      SELECT
        page_name,
        app_section,
        user_role,
        device_id,
        visit_id,
        application_type,
        device_type,
        app_version,
        logged_in,
        os_name,
        operating_system,
        browser_name,
        browser_version,
        browser_size_breakpoint,
        form_factor,
        referrer_link,
        application_name,
        'visits' AS unit_type,
        visit_id AS unit_identifier,

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
        SUM(api_badrequest_counts) AS api_badrequest_counts,
        SUM(api_general_failure_counts) AS api_general_failure_counts,
        SUM(api_insufficient_permission_counts) AS api_insufficient_permission_counts,
        SUM(api_internal_servererror_counts) AS api_internal_servererror_counts,
        SUM(api_invalid_token_counts) AS api_invalid_token_counts,
        SUM(api_invite_expired_counts) AS api_invite_expired_counts,
        SUM(api_outside_us_counts) AS api_outside_us_counts,
        SUM(api_proxy_counts) AS api_proxy_counts,
        SUM(api_token_expired_counts) AS api_token_expired_counts,
        SUM(api_too_many_attempts_counts) AS api_too_many_attempts_counts,
        SUM(api_unauthorized_counts) AS api_unauthorized_counts,
        SUM(buttonclick_checkyourinfocancel_counts) AS buttonclick_checkyourinfocancel_counts,
        SUM(buttonclick_checkyourinfoconfirm_counts) AS buttonclick_checkyourinfoconfirm_counts,
        SUM(buttonclick_chooseyourusernameback_counts) AS buttonclick_chooseyourusernameback_counts,
        SUM(buttonclick_chooseyourusernamenext_counts) AS buttonclick_chooseyourusernamenext_counts,
        SUM(buttonclick_closebutton_counts) AS buttonclick_closebutton_counts,
        SUM(buttonclick_confirmyouraccountcancel_counts) AS buttonclick_confirmyouraccountcancel_counts,
        SUM(buttonclick_confirmyouraccountnext_counts) AS buttonclick_confirmyouraccountnext_counts,
        SUM(buttonclick_create_new_id_counts) AS buttonclick_create_new_id_counts,
        SUM(buttonclick_enteryourpasswordcancel_counts) AS buttonclick_enteryourpasswordcancel_counts,
        SUM(buttonclick_enteryourpasswordsaveandsignin_counts) AS buttonclick_enteryourpasswordsaveandsignin_counts,
        SUM(buttonclick_footer_contact_contactus_counts) AS buttonclick_footer_contact_contactus_counts,
        SUM(buttonclick_footer_legal_californiaprivacyrights_counts) AS buttonclick_footer_legal_californiaprivacyrights_counts,
        SUM(buttonclick_footer_legal_californiaresidentdontsellmyinfo_counts) AS buttonclick_footer_legal_californiaresidentdontsellmyinfo_counts,
        SUM(buttonclick_footer_legal_gotoassist_counts) AS buttonclick_footer_legal_gotoassist_counts,
        SUM(buttonclick_footer_legal_policies_counts) AS buttonclick_footer_legal_policies_counts,
        SUM(buttonclick_footer_legal_privacyrights_counts) AS buttonclick_footer_legal_privacyrights_counts,
        SUM(buttonclick_forcedpasswordresetcancel_counts) AS buttonclick_forcedpasswordresetcancel_counts,
        SUM(buttonclick_forcedpasswordresetcontinue_counts) AS buttonclick_forcedpasswordresetcontinue_counts,
        SUM(buttonclick_multipleaddressesfoundconfirmanotherway_counts) AS buttonclick_multipleaddressesfoundconfirmanotherway_counts,
        SUM(buttonclick_multipleaddressesfoundnext_counts) AS buttonclick_multipleaddressesfoundnext_counts,
        SUM(buttonclick_multipleusernamesfoundconfirmanotherway_counts) AS buttonclick_multipleusernamesfoundconfirmanotherway_counts,
        SUM(buttonclick_multipleusernamesfoundnext_counts) AS buttonclick_multipleusernamesfoundnext_counts,
        SUM(buttonclick_needmoreinfonext_counts) AS buttonclick_needmoreinfonext_counts,
        SUM(buttonclick_recover_password_success) AS buttonclick_recover_password_success,
        SUM(buttonclick_recover_username_success_counts) AS buttonclick_recover_username_success_counts,
        SUM(buttonclick_resetyourpasswordcancel_counts) AS buttonclick_resetyourpasswordcancel_counts,
        SUM(buttonclick_resetyourpasswordsaveandsignin_counts) AS buttonclick_resetyourpasswordsaveandsignin_counts,
        SUM(buttonclick_termsandconditionsclose_counts) AS buttonclick_termsandconditionsclose_counts,
        SUM(buttonclick_updatesecurityquestiondropdown_counts) AS buttonclick_updatesecurityquestiondropdown_counts,
        SUM(buttonclick_updatesecurityquestionnext_counts) AS buttonclick_updatesecurityquestionnext_counts,
        SUM(buttonclick_usernameresetpassword_counts) AS buttonclick_usernameresetpassword_counts,
        SUM(buttonclick_usernamesignin_counts) AS buttonclick_usernamesignin_counts,
        SUM(buttonclick_verificationcodenext_counts) AS buttonclick_verificationcodenext_counts,
        SUM(buttonclick_verifyanotherway_counts) AS buttonclick_verifyanotherway_counts,
        SUM(buttonclick_verifyyouridentitymodalok_counts) AS buttonclick_verifyyouridentitymodalok_counts,
        SUM(buttonclick_verifyyouridentitynext_counts) AS buttonclick_verifyyouridentitynext_counts,
        SUM(buttonclick_welcomegetstarted_counts) AS buttonclick_welcomegetstarted_counts,
        SUM(pageview_counts) AS pageview_counts

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------


      FROM asp_idm_metric_agg
      WHERE (denver_date >= ("${env:START_DATE}") AND denver_date < ("${env:END_DATE}"))
      GROUP BY
        page_name,
        application_type,
        app_section,
        app_version,
        logged_in,
        application_name,
        os_name,
        operating_system,
        browser_name,
        browser_version,
        browser_size_breakpoint,
        user_role,
        device_id,
        visit_id,
        device_type,
        form_factor,
        referrer_link,
        visit_id,
        'visits'
      ) sumfirst
    GROUP BY
      unit_identifier,
      page_name,
      application_type,
      app_section,
      app_version,
      logged_in,
      application_name,
      os_name,
      operating_system,
      browser_name,
      browser_version,
      browser_size_breakpoint,
      user_role,
      device_id,
      visit_id,
      device_type,
      form_factor,
      referrer_link,
      unit_type
GROUPING SETS (
    (unit_identifier),
    (unit_identifier, referrer_link, page_name),
    (unit_identifier, referrer_link, browser_name, page_name),
    (unit_identifier, referrer_link, browser_name, app_section, page_name),
    (unit_identifier, referrer_link, device_type, page_name),
    (unit_identifier, referrer_link, browser_name, browser_size_breakpoint, page_name),
    (unit_identifier, referrer_link, app_section, browser_size_breakpoint, page_name),
    (unit_identifier, referrer_link, browser_name, page_name),
    (unit_identifier, page_name),
    (unit_identifier, browser_name, page_name),
    (unit_identifier, browser_name, app_section, page_name),
    (unit_identifier, device_type, page_name),
    (unit_identifier, browser_name, browser_size_breakpoint, page_name),
    (unit_identifier, app_section, browser_size_breakpoint, page_name),
    (unit_identifier, browser_name, page_name))
  ) sets
  GROUP BY
    '${env:label_date_denver}',
    page_name,
    application_type,
    app_section,
    app_version,
    logged_in,
    application_name,
    os_name,
    operating_system,
    browser_name,
    browser_version,
    browser_size_breakpoint,
    user_role,
    device_id,
    visit_id,
    device_type,
    form_factor,
    referrer_link,
    grouping_id,
    unit_type
  ) mapit
LATERAL VIEW EXPLODE(tmp_map) explode_table AS metric_name, metric_value;

--------------------------------------------------------------------------------
--------------------------------***** END *****---------------------------------
--------------------------------------------------------------------------------
