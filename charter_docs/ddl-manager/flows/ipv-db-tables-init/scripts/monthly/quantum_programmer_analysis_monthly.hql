CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.quantum_programmer_analysis_monthly
(
    grouping_id        int,
    asset_id           string,
    network            string,
    provider           string,
    in_home            string,
    title              string,
    episode_title      string,
    episode_number     string,
    season_number      string,
    system__dma        string,
    unique_hh          int,
    total_streams      int,
    average_streams_hh double,
    average_minutes    double,
    total_minutes      double
)
    PARTITIONED BY (year_month_denver string)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY");
