CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.quantum_daily_app_entry_quality
(
  visit__visit_id	STRING,
  max_timestamp INT,
  verifier_auth_failures INT,
  verifier_auth_success INT,
  auto_auth_failures INT,
  auto_auth_success	INT,
  resumeauth_failures INT,
  resumeauth_success INT,
  location_failures INT,
  location_success INT,
  capabilities_failures	INT,
  capbilities_success	INT,
  lc_errors	INT
)
    PARTITIONED BY (denver_date STRING)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY");
