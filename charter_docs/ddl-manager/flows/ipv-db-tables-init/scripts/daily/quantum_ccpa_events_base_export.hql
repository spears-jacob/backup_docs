CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.quantum_ccpa_events_base (
  mso STRING,
  billing_id_enc STRING,
  division_enc STRING,
  division_id_enc STRING,
  application_type STRING,
  device_type STRING,
  network_status STRING,
  connection_type STRING,
  message_category STRING,
  page_name STRING,
  event_count INT
)
    PARTITIONED BY (denver_date STRING)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY");
