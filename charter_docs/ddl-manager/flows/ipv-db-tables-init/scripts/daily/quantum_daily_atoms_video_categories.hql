CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.quantum_daily_atoms_video_categories
(
  encrypted_padded_account_number_256 STRING,
  encrypted_division_id_256 STRING,
  encrypted_billing_slice_256 STRING,
  cust_type STRING
)
  PARTITIONED BY (denver_date STRING)
  STORED AS ORC
  LOCATION '${s3_location}'
  TBLPROPERTIES ("orc.compress" = "SNAPPY");
