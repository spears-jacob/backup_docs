CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_pmai_sia_device_out (
  sia_id string,
  application_platform string,
  visit_id string,
  device_id string,
  device_manufacturer string,
  device_model string,
  device_os string,
  cell_carrier string,
  cell_network_type string,
  last_date_accessed_received string
)
PARTITIONED BY (run_date string, application_name string)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY")
;
