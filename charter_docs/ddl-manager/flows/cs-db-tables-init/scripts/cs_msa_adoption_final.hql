CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.cs_msa_adoption_final
(
  `call_start_date_utc`               STRING,
  `decrypted_agent_id`                STRING,
  `agent_location`                    STRING,
  `line_of_business`                  STRING,
  `cause_description`                 STRING,
  `identified_opportunities`          STRING,
  `identified_successes_and_fails`    STRING,
  `counts`                            INT
)
PARTITIONED BY (label_date STRING)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress" = "SNAPPY")
