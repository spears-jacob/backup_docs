CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_m2dot0_apiagg (
  visit__application_details__application_name                            STRING
  ,AGG_visit__account__configuration_factors                              STRING
  ,application__api__gql__operation_name                                  STRING
  ,response_time_ms_mean                                                  bigint
  ,response_time_ms_median                                                bigint
  ,response_time_ms_75                                                    bigint
  ,response_time_ms_90                                                    bigint
  ,response_time_ms_95                                                    bigint
  ,response_time_ms_99                                                    bigint
  ,num_visits                                                             INT
  ,num_events                                                             INT
  ,num_success                                                            INT
  ,num_failure                                                            INT
  ,num_partial_failure                                                    INT
  ,partition_date_utc_week1                                               STRING
  ,partition_date_utc_week4                                               STRING
)
PARTITIONED BY
(partition_date_utc STRING)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY");
