CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.si_agg_activation_channel (
  accountnumber string, 
  date_level string, 
  user_package string, 
  application_group string, 
  identity_created string, 
  login string, 
  setup_equipment string, 
  get_started string, 
  accept_terms_conditions string, 
  initiated_self_install string, 
  activation_failure string, 
  activation_success string, 
  msa_collateral string, 
  msa_elig_sawhomecard string, 
  full_screen_takeover_page_view string, 
  activation_complete string, 
  msa_activation_errors string, 
  web_activation_errors string,
  mso string,
  application_type string,
  activation_success_partial string)
	PARTITIONED BY (denver_date STRING)
	STORED AS ORC
	LOCATION '${s3_location}'
	TBLPROPERTIES ("orc.compress" = "SNAPPY", "retention_policy" = "Aggregate - With PII (3 Years)")