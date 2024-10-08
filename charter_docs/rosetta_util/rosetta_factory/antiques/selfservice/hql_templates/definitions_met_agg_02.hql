visit__account__enc_account_number as portals_unique_acct_key,
visit__activated_experiments as activated_experiments,
visit__application_details__technology_type as technology_type,
epoch_converter(received__timestamp, 'America/Denver') AS denver_date
FROM

(SELECT application__api__api_name,
      application__api__path,
      application__api__response_code,
      application__api__response_text,
      application__api__service_result,
      application__error__error_code,
      application__error__error_message,
      application__error__error_type,
      message__context,
      message__event_case_id,
      message__feature__feature_name,
      message__feature__feature_step_changed,
      message__feature__feature_step_name,
      message__name,
      operation__billing__payment_amount_usd,
      operation__billing__payment_date,
      operation__billing__payment_due_date,
      operation__billing__payment_method,
      operation__operation_type,
      operation__success,
      operation__toggle_state,
      operation__user_entry__enc_feedback,
      operation__user_entry__entry_type,
      operation__user_entry__str_value,
      received__timestamp,
      state__content__content_class,
      state__content__details__episode_number,
      state__content__details__genres,
      state__content__details__season_number,
      state__content__identifiers__tms_program_id,
      state__content__programmer__linear__channel_name,
      state__content__stream__playback_id,
      state__content__stream__playback_type,
      state__view__current_page__app_section,
      state__view__current_page__current_balance_amount,
      state__view__current_page__elements__element_int_value,
      state__view__current_page__elements__element_string_value,
      state__view__current_page__elements__selected_options,
      state__view__current_page__elements__standardized_name,
      state__view__current_page__elements__ui_name,
      state__view__current_page__page_name,
      state__view__current_page__page_section__name,
      state__view__current_page__page_sub_section__name,
      state__view__current_page__page_title,
      state__view__current_page__page_url,
      state__view__current_page__past_due_amount,
      state__view__current_page__total_amount_due,
      state__view__modal__modal_type,
      state__view__modal__name,
      state__view__modal__text,
      state__view__previous_page__app_section,
      state__view__previous_page__page_name,
      visit__account__details__auto_pay_status,
      visit__account__details__billing_status,
      visit__account__details__mso,
      visit__account__enc_account_billing_division,
      visit__account__enc_account_billing_division_id,
      visit__account__enc_account_billing_id,
      visit__account__enc_account_number,
      visit__activated_experiments,
      visit__application_details__app_version,
      visit__application_details__application_name,
      visit__application_details__application_type,
      visit__application_details__technology_type,
      visit__connection__connection_type,
      visit__connection__network_status,
      visit__device__browser__name,
      visit__device__browser__user_agent,
      visit__device__browser__version,
      visit__device__device_form_factor,
      visit__device__device_type,
      visit__device__enc_uuid,
      visit__device__model,
      visit__device__operating_system,
      visit__location__enc_city,
      visit__location__enc_zip_code,
      visit__location__state,
      visit__login__logged_in,
      visit__login__login_duration_ms,
      visit__variant_uuids,
      visit__visit_id
FROm  core_quantum_events
WHERE partition_date_hour_utc >= '${hiveconf:START_DATE_TZ}'
AND partition_date_hour_utc <  '${hiveconf:END_DATE_TZ}'
AND visit__application_details__application_name
  IN ('SpecNet',
      'SMB',
      'MySpectrum',
      'IDManagement',
      'PrivacyMicrosite'
     )
) dictionary

GROUP BY
  CASE
    WHEN  visit__account__details__mso IN ('CHARTER','CHTR','"CHTR"')       THEN 'L-CHTR'
    WHEN  visit__account__details__mso IN ('TWC','"TWC"', 'NONECAPTURED')   THEN 'L-TWC'
    WHEN  visit__account__details__mso IN ('BH','BHN','"BHN"')              THEN 'L-BHN'
    WHEN (visit__account__details__mso IN ('','unknown','"NONE"', 'NONE')
          OR visit__account__details__mso IS NULL )                         THEN 'MSO-MISSING'
    ELSE visit__account__details__mso
  END,
 visit__application_details__application_type,
LOWER(visit__device__device_type),
visit__device__model,
visit__connection__connection_type,
visit__connection__network_status,
state__content__stream__playback_type,
IF(visit__application_details__application_type = 'Android', IF(visit__device__operating_system   RLIKE '^(Android 4.0)' OR visit__device__operating_system RLIKE '^(Android 4.1)' OR   visit__device__operating_system RLIKE '^(Android 4.2)' OR visit__device__operating_system RLIKE  '^(Android 4.3)', 'Android 4.1 - 4.3', 'Android 4.4+'), NULL),
CASE
  WHEN visit__application_details__application_type IN('iOS','Android') THEN 'Mobile'
  WHEN visit__application_details__application_type IN('Xbox','SamsungTV') THEN 'Connected Devices'
 ELSE visit__application_details__application_type
END,
state__content__stream__playback_id,
visit__visit_id,
visit__device__enc_uuid,
visit__account__enc_account_number,
visit__variant_uuids,
state__content__details__episode_number,
state__content__details__season_number,
state__content__details__genres,
state__content__programmer__linear__channel_name,
visit__device__browser__name,
visit__device__browser__user_agent,
visit__device__browser__version,
state__content__identifiers__tms_program_id,
visit__account__enc_account_billing_id,
visit__account__enc_account_billing_division,
visit__account__enc_account_billing_division_id,
visit__location__enc_city,
visit__location__state,
visit__location__enc_zip_code,
visit__application_details__app_version,
state__content__content_class,
CONCAT(
visit__account__enc_account_billing_division_id ,
visit__account__enc_account_billing_division ,
visit__account__enc_account_billing_id
),
IF(LOWER(visit__application_details__application_name) IN('oneapp','specu','bulkmdu'), 'IPTV',  LOWER(visit__application_details__application_name)),
LOWER(visit__application_details__application_name),
visit__login__logged_in,
visit__device__operating_system,
visit__device__device_form_factor,
visit__account__details__billing_status,
visit__account__details__auto_pay_status,
visit__account__enc_account_number,
visit__activated_experiments,
visit__application_details__technology_type,
epoch_converter(received__timestamp, 'America/Denver')
;
