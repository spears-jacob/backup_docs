CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_support_quantum_events (
  application_name string,
  visit_id string,
  device_id string,
  page_name string,
  visit_unique_id string,
  account_number string,
  received__timestamp bigint,
  message__name STRING,
  operation__operation_type STRING,
  search_id STRING,
  search_text STRING,
  seq_num INT,
  element_url STRING,
  helpful_yes INT,
  helpful_no INT,
  wasnt_what_i_searched INT,
  incorrect_info INT,
  confusing INT
)
PARTITIONED BY (
  denver_date STRING
)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY")
;
