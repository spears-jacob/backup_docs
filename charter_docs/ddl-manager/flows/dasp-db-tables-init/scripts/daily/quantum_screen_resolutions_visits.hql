CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.quantum_screen_resolutions_visits (
  screen_resolution string,
  application_name string,
  application_type string,
  operating_system string,
  device_type string,
  width string,
  height string,
  visits int
)
  PARTITIONED BY (date_denver string)
  STORED AS ORC
  LOCATION '${s3_location}'
  TBLPROPERTIES ("orc.compress"="SNAPPY");
