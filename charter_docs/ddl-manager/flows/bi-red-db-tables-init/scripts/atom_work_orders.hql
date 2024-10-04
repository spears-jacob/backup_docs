CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.atom_work_orders (
  legacy_company STRING,
  account_key STRING,
  order_number STRING,
  job_number STRING,
  job_reason_code STRING,
  job_reason_code_description STRING,
  job_resolution_code STRING,
  job_resolution_code_description STRING,
  job_type_code STRING,
  job_type_code_description STRING,
  job_category_code STRING,
  job_class_code STRING,
  job_class_category_code STRING,
  installation_category STRING,
  includes_truck_roll BOOLEAN,
  truck_roll_reason_code_category STRING,
  technician_id STRING,
  job_status_code STRING,
  encrypted_technician_contracting_firm_name_256 STRING,
  encrypted_technician_first_name_256 STRING,
  encrypted_technician_last_name_256 STRING,
  job_entry_date_eastern STRING,
  job_completed_date_eastern STRING,
  service_code STRING,
  service_code_description STRING,
  spectrum_guide_install_type STRING,
  encrypted_account_key_256 STRING,
  encrypted_technician_id_256 STRING)
PARTITIONED BY (
  partition_date_denver STRING,
  extract_source STRING)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress" = "ZLIB");