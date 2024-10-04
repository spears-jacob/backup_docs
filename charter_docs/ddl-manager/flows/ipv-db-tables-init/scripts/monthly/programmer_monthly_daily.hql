CREATE EXTERNAL TABLE if NOT EXISTS ${db_name}.programmer_monthly_daily
(
    visit__account__bi_account_info__stream_2        string,
    visit__application_details__application_type     string,
    state__content__identifiers__tms_program_id      string,
    state__content__identifiers__provider_asset_id   string,
    visit__account__account_number                   string,
    system__dma                                      string,
    state__content__stream__playback_id              string,
    state__content__details__title                   string,
    state__content__details__season_number           string,
    state__content__details__episode_number          string,
    state__content__programmer__linear__channel_name string,
    state__content__details__episode_title           string,
    visit__connection__network_status                string,
    state__content__stream__playback_type            string,
    state__content__programmer__linear__network_name string,
    message__name                                    string,
    message__sequence_number                         int,
    state__playback__heartbeat__content_elapsed_ms   int,
    visit__account__details__stream_subtype          string
)
    PARTITIONED BY (denver_date string)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY");
