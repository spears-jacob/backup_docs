CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.atom_fiber_nodes
(
    `bi_foreign_key_dim_fiber_node_history_sk`             string,
    `bi_foreign_key_dim_date_sk`                           string,
    `bi_foreign_key_dim_employee_history_sk`               string,
    `bi_foreign_key_dim_source_system_code_sk`             string,
    `key_market_area_description`                          string,
    `node_health_score_value`                              double,
    `cable_modem_termination_system_status_code`           string,
    `cable_modem_termination_system_name`                  string,
    `node_health_score_color_bucket`                       string,
    `node_health_score_color_name`                         string,
    `total_out_of_spec_device_count`                       int,
    `upstream_uncorrectable_fec_percentage`                double,
    `upstream_correctable_fec_percentage`                  double,
    `downstream_uncorrectable_fec_percentage`              double,
    `downstream_correctable_fec_percentage`                double,
    `upstream_average_snr`                                 double,
    `upstream_snr_variance`                                double,
    `downstream_average_snr`                               double,
    `downstream_snr_variance`                              double,
    `downstream_snr_out_of_spec_device_count`              double,
    `upstream_hybrid_fiber_coax_rf_variance`               double,
    `downstream_hybrid_fiber_coax_rf_variance`             double,
    `out_of_spec_device_count`                             int,
    `out_of_spec_device_percentage`                        double,
    `t3_timeout_count`                                     int,
    `upstream_inchannel_frequency_response_percentage`     double,
    `upstream_ripple_percentage`                           double,
    `upstream_echo_percentage`                             double,
    `consecutive_sub_prime_node_health_days`               int,
    `revenue_generating_units_count`                       int,
    `total_subscriber_count`                               int,
    `video_subscriber_count`                               int,
    `internet_subscriber_count`                            int,
    `voice_subscriber_count`                               int,
    `line_problem_service_repair_order_count`              int,
    `trouble_call_repeat_count`                            int,
    `trouble_call_repeat_within_seven_days_count`          int,
    `video_trouble_call_repeat_within_seven_days_count`    int,
    `internet_trouble_call_repeat_within_seven_days_count` int,
    `voice_trouble_call_repeat_within_seven_days_count`    int
)
    PARTITIONED BY (
        `partition_date_denver` string,
        `extract_source` string)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY")
