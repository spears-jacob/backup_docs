CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.quantum_daily_buy_flow_eligible_agg
(
    application_type                   STRING,
    visit_id                           STRING,
    page_name                          STRING,
    page_sequence                      STRING,
    campaign_id                        STRING,
    mso                                STRING,
    acct_id                            STRING,
    billing_division                   STRING,
    billing_id                         STRING,
    billing_combo_key                  STRING,
    cust_type                          STRING,
    device_type                        STRING,
    device_id                          STRING,
    app_version                        STRING,
    technology_type                    STRING,
    referrer_location                  STRING,
    message_context                    STRING,
    buyflow_eligible_api_path          STRING,
    buyflow_eligible_api_response_text STRING,
    response_count                     INT
)
    PARTITIONED BY (denver_date STRING)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY");
