CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_adhoc_outage_alerts (
  visit__visit_id string,
  visit__account__enc_account_number string,
  visit__account__details__mso string,
  visit__application_details__application_name string,
  visit__application_details__app_version string,
  message__sequence_number int,
  message__event_case_id string,
  received__timestamp bigint,
  operation__additional_information string,
  state__view__current_page__elements__element_string_value string,
  application__error__error_type string,
  application__api__response_code string,
  custom_visit_had_call int,
  custom_visit_had_call_inclusive int)
PARTITIONED BY (partition_date_utc string)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ('orc.compress'='SNAPPY', 'comment'='requests from xg23537')
;
