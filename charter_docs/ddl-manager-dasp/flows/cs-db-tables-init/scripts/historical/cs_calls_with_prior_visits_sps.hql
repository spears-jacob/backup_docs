CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.cs_calls_with_prior_visits_sps
(
    account_key            string,
    account_number         string,
    customer_type          string,
    customer_subtype       string,
    call_inbound_key       string,
    product                string,
    agent_mso              string,
    visit_type             string,
    issue_description      string,
    cause_description      string,
    resolution_description string,
    minutes_to_call        bigint,
    account_agent_mso      string,
    enhanced_mso           boolean,
    segment_id             string,
    truck_roll_flag        boolean,
    visit_id               string,
    sps_flag               string
)
    PARTITIONED BY (`call_date` string)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY") 
