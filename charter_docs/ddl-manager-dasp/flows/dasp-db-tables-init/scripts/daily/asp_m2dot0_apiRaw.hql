CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_m2dot0_apiRaw (
  visit__visit_id                                                       string
  ,visit__application_details__application_name                         string
  ,application__api__gql__operation_name                                string
  ,application__api__service_result                                     string
  ,application__api__response_time_ms                                   string
  ,application__api__api_name                                           string
  ,message__sequence_number                                             int
)
PARTITIONED BY
(partition_date_utc string)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY");
