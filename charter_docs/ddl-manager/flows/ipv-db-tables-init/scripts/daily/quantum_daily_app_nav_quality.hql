CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.quantum_daily_app_nav_quality
(
  visit__visit_id	STRING,
  max_timestamp	INT,
  is_active	INT,
  nowandnext_failures INT,
  nowandnext_success INT,
  nmdepgs_failures INT,
  nmdepgs_success INT,
  guide_failures INT,
  guide_success	INT,
  nns_failures INT,
  nns_success	INT
)
    PARTITIONED BY (denver_date STRING)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY");
