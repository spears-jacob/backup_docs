CREATE EXTERNAL TABLE if not exists ${db_name}.quantum_originals_tms_ids
(
    promo_type                string COMMENT '{"DESC:type of promoted content for example Spectrum Original"}',
    series_id                 string COMMENT '{"DESC:series tms id of the content being promoted"}',
    tms_program_id            string COMMENT '{"DESC:asset id of the content being promoted - for quantum events matching"}',
    asset_id                  string COMMENT '{"DESC:asset id of the content being promoted - for verimatrix vod matching"}',
    promo_description         string COMMENT '{"DESC:description of content being promoted"}',
    format                    string,
    series_number             int,
    episode_number            int,
    asset_type                string,
    tracking_start_date       string,
    tracking_end_date         string,
    include_in_reporting      STRING,
    first_episode_for_reporting    string
)
    PARTITIONED BY (series_name string)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');
