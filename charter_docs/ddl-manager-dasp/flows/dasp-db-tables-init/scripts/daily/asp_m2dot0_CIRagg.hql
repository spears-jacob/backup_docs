CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_m2dot0_CIRagg (
AGG_visit__account__details__mso                                      string
,visit__application_details__application_name                         string
,CUSTOM_visit__application_details__app_version                       string
,AGG_visit__account__configuration_factors                            string
,AGG_CUSTOM_visit__account__configuration_factors                     string
,AGG_CUSTOM_visit__account__details__service_subscriptions_core       string
,AGG_CUSTOM_visit__account__details__service_subscriptions_mobile     string
,AGG_CUSTOM_customer_group                                            string
,distinct_visits                                                      int
,visits_with_call                                                     int
,visits_with_call_inclusive                                           int
,visits_first                                                         int
,visits_first_with_call_inclusive                                     int
)
PARTITIONED BY
(partition_date_utc string)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY");
