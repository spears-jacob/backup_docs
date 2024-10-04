CREATE EXTERNAL TABLE if not exists ${db_name}.quantum_enrichment_status
(
    application_type string,
    app_version string,
    message__category                                  string,
    grouping_id                                        int,
    total_devices                                      bigint,
    total_accounts                                     bigint,
    total_devices_with_auth                            bigint,
    total_accounts_with_auth                           bigint,
    total_accounts_with_oath                           bigint,
    total_accounts_with_playback_messages_oauth_tokens bigint,
    total_accounts_without_oauth_tokens                bigint,
    has_billing_id                                     bigint,
    missing_billing_id                                 bigint,
    has_mso                                            bigint,
    missing_mso                                        bigint,
    has_device_id                                      bigint,
    missing_device_id                                  bigint,
    has_oauth                                          bigint,
    missing_oauth                                      bigint
)
    PARTITIONED BY (denver_date STRING)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY");
