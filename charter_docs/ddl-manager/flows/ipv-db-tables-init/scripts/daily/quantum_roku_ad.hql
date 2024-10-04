CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.quantum_roku_ad
(
  visit_grouping STRING,
  visit_id STRING,
  refer_location STRING,
  mso STRING,
  acct_id STRING,
  billing_id STRING,
  billing_division STRING,
  distinct_accounts INT,
  min_date_hour STRING,
  max_date_hour STRING,
  event_count INT,
  purchase_success INT,
  eligible_api_calls INT,
  eligible_result STRING,
  has_transaction_iD INT,
  watch_time_sec BIGINT,
  login INT,
  SignUp INT,
  Premiums INT,
  FinalPrice INT,
  addons INT,
  PurchaseAgreement INT,
  PurchaseConfirmation INT,
  startUpScreen INT,
  playerLiveTv INT,
  application_type String
)
PARTITIONED BY (denver_date STRING)
STORED AS ORC
    LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY")
