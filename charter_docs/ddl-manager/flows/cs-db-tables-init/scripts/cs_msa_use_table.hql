CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.cs_msa_use_table
(
  `portals_account_number`   STRING,
  `visit__visit_id`          STRING,
  `min_received__timestamp`  STRING
)
PARTITIONED BY (label_date STRING)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress" = "SNAPPY")
