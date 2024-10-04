USE ${env:TMP_db};

CREATE TABLE IF NOt EXISTS ${env:ReprocessDateTable_daily} (run_date STRING);
INSERT OVERWRITE TABLE ${env:ReprocessDateTable_daily} VALUES('${env:RUN_DATE}');

-----------------------------------------

USE ${env:ENVIRONMENT};

CREATE TABLE IF NOT EXISTS msa_adoption_testing_ijg
(
  call_inbound_key           STRING,
  call_start_date_utc        STRING,
  call_start_datetime_utc    STRING,
  call_end_datetime_utc      STRING,
  agent_location             STRING,
  decrypted_agent_id         STRING,
  call_start_timestamp_utc   STRING,
  call_end_timestamp_utc     STRING,
  call_account_number        STRING,
  line_of_business           STRING,
  cause_description          STRING
)
PARTITIONED BY (label_date STRING)
;

CREATE TABLE IF NOT EXISTS msa_use_table_ijg
(
  portals_account_number   STRING,
  visit__visit_id          STRING,
  min_received__timestamp  STRING
)
PARTITIONED BY (label_date STRING)
;

CREATE TABLE IF NOT EXISTS msa_adoption_final
(
  call_start_date_utc               STRING,
  decrypted_agent_id                STRING,
  agent_location                    STRING,
  line_of_business                  STRING,
  cause_description                 STRING,
  identified_opportunities          STRING,
  identified_successes_and_fails    STRING,
  counts                            INT
)
PARTITIONED BY (label_date STRING)
;

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
