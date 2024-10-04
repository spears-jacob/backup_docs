CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.chtr_fiscal_month
(
  partition_date string,
  fiscal_month string
)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress" = "SNAPPY")