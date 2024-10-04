CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_msa_ahw (
   AHW_accts bigint
  ,monthly_accounts_with_visits bigint
  ,num_visits bigint
  ,accounts_with_visits bigint
  ,accounts_with_visits_running bigint
  ,accounts_with_visits_percent decimal(12,4)
  ,accounts_with_visits_percent_running decimal(12,4)
  ,monthly_accounts_with_visits_AHW_events bigint
  )
  PARTITIONED BY (month_end_date string)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY");
