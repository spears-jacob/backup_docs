CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_m2dot0_metric_visit_lookup (
visit__application_details__application_name                        STRING,
visit__application_details__application_type                        STRING,
visit__application_details__app_version                             STRING,
CUSTOM_visit__application_details__app_version                      STRING,
agg_custom_visit__account__details__service_subscriptions           STRING,
agg_custom_visit__account__details__service_subscriptions_other     STRING,
agg_custom_visit__account__details__service_subscriptions_core      STRING,
agg_custom_visit__account__details__service_subscriptions_mobile    STRING,
agg_custom_customer_group                                           STRING,
agg_custom_visit__account__configuration_factors                    STRING,
agg_visit__account__configuration_factors                           STRING,
visit_id                                                            STRING,
metric_name                                                         STRING,
metric_value                                                        decimal (12,4)
)
PARTITIONED BY
(partition_date_utc string)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY")
;
