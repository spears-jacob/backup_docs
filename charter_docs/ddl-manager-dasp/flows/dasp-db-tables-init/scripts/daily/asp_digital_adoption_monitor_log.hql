create table ${db_name}.asp_digital_adoption_monitor_log
(
  script_version string,
  start_date_denver string,
  end_date_denver string,
  additional_params string,
  run_time string,
  run_date string
)
PARTITIONED BY (run_month STRING)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY")
;
