CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_adhoc_cpni_verification(
  visit__visit_id string,
  visit__account__enc_account_number string,
  visit__account__details__mso string,
  visit__application_details__application_name string,
  visit__application_details__app_version string,
  visit__account__details__statement_method string,
  visit__login__login_completed_timestamp bigint,
  visit__visit_start_timestamp bigint,
  sub_voice_status string,
  message__sequence_number int,
  message__event_case_id string,
  received__timestamp bigint,
  state__view__current_page__elements__selected_options string,
  lag_state__view__current_page__elements__selected_options string,
  state__view__current_page__elements__element_string_value string,
  application__error__client_error_code string,
  state__view__current_page__page_name string,
  state__view__previous_page__page_name string,
  state__view__previous_page__app_section string,
  custom_visit_had_call int,
  custom_visit_had_call_inclusive int,
  custom_attempt_number int)
PARTITIONED BY (partition_date_utc string)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ('orc.compress'='SNAPPY', 'comment'='requests from xg21791')
;
