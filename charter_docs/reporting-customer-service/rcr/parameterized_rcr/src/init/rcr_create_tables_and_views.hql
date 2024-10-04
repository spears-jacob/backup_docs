USE ${env:TMP_db};

CREATE TABLE IF NOt EXISTS ${env:ReprocessDateTable_daily} (run_date STRING);
INSERT OVERWRITE TABLE ${env:ReprocessDateTable_daily} VALUES('${env:RUN_DATE}');

-----------------------------------------

USE ${env:ENVIRONMENT};

CREATE TABLE IF NOT EXISTS rcr_calls_analysis
(
  call_inbound_key               STRING,
  encrypted_account_number_256   STRING,
  customer_type                  STRING,
  account_agent_mso              STRING,
  product                        STRING,
  location_name                  STRING,
  created_by                     STRING,
  bef_num_calls_product          INT,
  bef_num_calls_retention        INT,
  aft_num_calls_product          INT,
  aft_num_calls_retention        INT
)
PARTITIONED BY (call_end_date_utc STRING)
;
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
