CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_extract_voice_of_customer_cable (
   partition_date_utc string,
   acct_number_enc string,
   mobile_acct_enc string,
   visit_id string,
   app string,
   journey array<string>,
   sub_journey array<string>,
   full_journey array<string>,
   component_name array<string>,
   component_std_name array<string>,
   component_ui_name array<string>,
   transaction_timestamp bigint,
   division_id string,
   site_sys string,
   prn string,
   agn string,
   division string,
   biller_type string,
   acct_site_id string,
   acct_company string,
   acct_franchise string,
   mso string,
   case_id string,
   page_element string,
   message__name string,
   operation__success boolean,
   event_type string)
PARTITIONED BY (denver_date string)
STORED AS PARQUET
LOCATION '${s3_location}'
TBLPROPERTIES ('parquet.compression'='SNAPPY')
;
