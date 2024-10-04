CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_extract_voice_of_customer_mobile
(
  datetime_denver                                    string,
  mobile_account_number_enc                          string,
  mobile_device_number_enc                           string,
  final_supportPage                                  string
)
PARTITIONED BY (denver_date string)
STORED AS PARQUET
LOCATION '${s3_location}'
TBLPROPERTIES ('parquet.compression'='SNAPPY')
; 
