!echo "Alter call care tables - Amayak Urumyan";

ALTER TABLE prod.cs_call_care_data ADD COLUMNS (account_key STRING, account_agent_mso STRING, enhanced_mso BOOLEAN);
ALTER TABLE prod.cs_calls_with_prior_visits ADD COLUMNS (account_key STRING, account_agent_mso STRING, enhanced_mso BOOLEAN, segment_id STRING);