CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_page_agg_counts
(
    application_name      STRING,
    current_page_name     STRING,
    current_article_name  STRING,
    standardized_name     STRING,
    modal_name            STRING,
    modal_view_count      BIGINT,
    page_view_count       BIGINT,
    select_action_count   BIGINT,
    spinner_success_count BIGINT,
    spinner_failure_count BIGINT,
    toggle_flips_count    BIGINT
)
    PARTITIONED BY (denver_date STRING, unit_type STRING)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY")
;
