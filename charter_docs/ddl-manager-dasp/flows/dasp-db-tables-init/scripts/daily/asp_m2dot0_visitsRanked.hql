CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_m2dot0_visitsRanked (
visit__account__enc_account_number                                    string
,AGG_visit__account__details__mso                                     string
,visit__account__enc_mobile_account_number                            string
,visit__application_details__application_name                         string
,visit__application_details__application_type                         string
,visit__application_details__app_version                              string
,CUSTOM_visit__application_details__app_version                       string
,AGG_visit__account__configuration_factors                            string
,AGG_CUSTOM_visit__account__configuration_factors                     string
,AGG_CUSTOM_visit__account__details__service_subscriptions            string
,AGG_CUSTOM_visit__account__details__service_subscriptions_other      string
,AGG_CUSTOM_visit__account__details__service_subscriptions_core       string
,AGG_CUSTOM_visit__account__details__service_subscriptions_mobile     string
,AGG_CUSTOM_customer_group                                            string
,visit__visit_id                                                      string
,visit__visit_start_timestamp                                         bigint
,CUSTOM_visit_num_acctMSOappMS                                        bigint
)
PARTITIONED BY
(partition_date_utc string)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY");
