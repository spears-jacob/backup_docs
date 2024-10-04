CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_m2dot0_apiJoin (
  visit__account__enc_account_number                                    STRING
  ,AGG_visit__account__details__mso                                     STRING
  ,visit__account__enc_mobile_account_number                            STRING
  ,visit__application_details__application_name                         STRING
  ,visit__application_details__application_type                         STRING
  ,visit__application_details__app_version                              STRING
  ,AGG_visit__account__configuration_factors                            STRING
  ,AGG_CUSTOM_visit__account__details__service_subscriptions            STRING
  ,AGG_CUSTOM_customer_group                                            STRING
  ,visit__visit_id                                                      STRING
  ,application__api__gql__operation_name                                STRING
  ,application__api__service_result                                     STRING
  ,application__api__response_time_ms                                   bigint
  ,application__api__api_name                                           STRING
  ,message__sequence_number                                             int
  ,partition_date_utc_week1                                             STRING
  ,partition_date_utc_week4                                             STRING
)
PARTITIONED BY
(partition_date_utc STRING)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY");
