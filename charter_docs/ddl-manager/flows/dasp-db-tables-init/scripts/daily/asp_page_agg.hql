CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_page_agg
(
    mso                          STRING,
    application_name             STRING,
    visit_id                     STRING,
    account_number               STRING,
    device_id                    STRING,
    current_page_name            STRING,
    current_article_name         STRING,
    previous_page_name           STRING,
    standardized_name            STRING,
    modal_name                   STRING,
    previous_page_viewed_time_ms INT,
    modal_view_instances         BIGINT,
    page_view_instances          BIGINT,
    select_action_instances      BIGINT,
    spinner_success_instances    BIGINT,
    spinner_failure_instances    BIGINT,
    toggle_flip_instances        BIGINT,
    modal_view_devices           BIGINT,
    page_view_devices            BIGINT,
    select_action_devices        BIGINT,
    spinner_success_devices      BIGINT,
    spinner_failure_devices      BIGINT,
    toggle_flip_devices          BIGINT,
    modal_view_households        BIGINT,
    page_view_households         BIGINT,
    select_action_households     BIGINT,
    spinner_success_households   BIGINT,
    spinner_failure_households   BIGINT,
    toggle_flip_households       BIGINT,
    modal_view_visits            BIGINT,
    page_view_visits             BIGINT,
    select_action_visits         BIGINT,
    spinner_success_visits       BIGINT,
    spinner_failure_visits       BIGINT,
    toggle_flip_visits           BIGINT
)
PARTITIONED BY (denver_date STRING)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY")
;
