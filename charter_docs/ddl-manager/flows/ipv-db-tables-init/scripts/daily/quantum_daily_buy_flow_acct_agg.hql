CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.quantum_daily_buy_flow_acct_agg
(
    billing_division string,
    billing_id string,
    billing_combo_key string,
    acct_id string,
    mso string,
    device_type string,
    application_type string,
    applicationplatform_partition string,
    promo_type string,
    feature_name string,
    feature_type string,
    promo_offer_id string,
    activated_experiments map<string,string>,
    variant_uuid array<string>,
    activity_type string,
    promochange_event string,
    buyflow_entries bigint,
    first_buyflow_entry_time bigint,
    max_completed_steps_buyflow array<string>,
    purchase_successes bigint,
    purchase_success_timestamp bigint,
    promo_start_date bigint,
    promo_start_timestamp_denver string,
    promo_end_timestamp_denver string,
    min_received_timestamp bigint
)
    PARTITIONED BY (denver_date STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
    WITH SERDEPROPERTIES ('orc.column.index.access'='false')
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY")
;
