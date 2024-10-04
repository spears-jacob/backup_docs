CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.cs_msa_adoption_testing
(
  `call_inbound_key`           STRING,
  `call_start_date_utc`        STRING,
  `call_start_datetime_utc`    STRING,
  `call_end_datetime_utc`      STRING,
  `agent_location`             STRING,
  `decrypted_agent_id`         STRING,
  `call_start_timestamp_utc`   STRING,
  `call_end_timestamp_utc`     STRING,
  `call_account_number`        STRING,
  `line_of_business`           STRING,
  `cause_description`          STRING
)
PARTITIONED BY (label_date STRING)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress" = "SNAPPY")
