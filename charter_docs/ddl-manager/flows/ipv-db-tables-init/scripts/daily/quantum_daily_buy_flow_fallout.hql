CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.quantum_daily_buy_flow_fallout
(
  grouping_id string,
  bools string,
  type string,
  mso string,
  application_type string,
  campaign_id string,
  is_true bigint,
  is_false bigint,
  is_null bigint
)
PARTITIONED BY (denver_date STRING)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress" = "SNAPPY");