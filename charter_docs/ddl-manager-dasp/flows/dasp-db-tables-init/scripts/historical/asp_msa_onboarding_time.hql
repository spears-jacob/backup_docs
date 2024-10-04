create external table IF NOT EXISTS ${db_name}.asp_msa_onboarding_time (
  mso string,
  device_type string,
  tutorial_completed STRING,
  metric_type string,
  pagename string,
  number_of_item int,
  time_avg_sec double,
  time_25th_sec double,
  time_50th_sec double,
  time_75th_sec double,
  process_date_time_denver STRING,
  process_identity STRING
)
PARTITIONED BY (label_date_denver string, grain STRING)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY")
; 
