USE ${env:ENVIRONMENT};

SELECT "Creating asp_asapp_customer_feedback";
CREATE TABLE IF NOT EXISTS asp_asapp_customer_feedback (
    question STRING,
    last_agent_id INT,
    company_name STRING,
    question_category STRING,
    issue_id BIGINT,
    instance_ts STRING,
    question_type STRING,
    ordering INT,
    answer STRING,
    conversation_id BIGINT,
    last_rep_id INT,
    partition_date STRING,
    partition_hour STRING
)
PARTITIONED BY (instance_date string, instance_hour string)
TBLPROPERTIES ('retention_policy'='Event Level - Without PII (3 Years)');

SELECT "Creating asp_asapp_export_row_counts";
CREATE TABLE IF NOT EXISTS asp_asapp_export_row_counts (
    export_name STRING,
    company_name STRING,
    export_interval STRING,
    exported_rows INT,
    export_date STRING,
    partition_date STRING,
    partition_hour STRING
)
PARTITIONED BY (partition_export_date string)
TBLPROPERTIES ('retention_policy'='Event Level - Without PII (3 Years)');

SELECT "Creating asp_asapp_flow_detail";
CREATE TABLE IF NOT EXISTS asp_asapp_flow_detail (
    link_resolved_pdl STRING,
    link_resolved_pil STRING,
    session_id STRING,
    company_name STRING,
    event_name STRING,
    event_ts STRING,
    issue_id STRING,
    flow_name STRING,
    event_type STRING,
    flow_id STRING,
    conversation_id STRING,
    partition_date STRING,
    partition_hour STRING
)
PARTITIONED BY (event_date string, event_hour string)
TBLPROPERTIES ('retention_policy'='Event Level - Without PII (3 Years)');

SELECT "Creating asp_asapp_intents";
CREATE TABLE IF NOT EXISTS asp_asapp_intents (
    name STRING,
    default_disambiguation BOOLEAN,
    company_name STRING,
    short_description STRING,
    code STRING,
    flow_name STRING,
    intent_type STRING,
    actions STRING
)
PARTITIONED BY (partition_date STRING, partition_hour STRING)
TBLPROPERTIES ('retention_policy'='Event Level - Without PII (3 Years)');

SELECT "Creating asp_asapp_issue_queues";
CREATE TABLE IF NOT EXISTS asp_asapp_issue_queues (
    rep_id INT,
    enter_queue_flow_name STRING,
    abandoned BOOLEAN,
    company_name STRING,
    enter_queue_eventflags INT,
    enqueue_time DOUBLE,
    company_subdivision STRING,
    queue_id STRING,
    issue_id BIGINT,
    enter_queue_eventtype STRING,
    instance_ts STRING,
    enter_queue_message_name STRING,
    exit_queue_eventflags INT,
    enter_queue_ts STRING,
    exit_queue_ts STRING,
    company_segments STRING,
    conversation_id BIGINT,
    exit_queue_eventtype STRING,
    queue_name STRING,
    agent_id INT,
    partition_date STRING,
    partition_hour STRING
)
PARTITIONED BY (instance_date string, instance_hour string)
TBLPROPERTIES ('retention_policy'='Event Level - Without PII (3 Years)');

SELECT "Creating asp_asapp_rep_activity";
CREATE TABLE IF NOT EXISTS asp_asapp_rep_activity (
    status_description STRING,
    rep_id INT,
    company_name STRING,
    company_subdivision STRING,
    status_id STRING,
    in_status_starting_ts STRING,
    rep_name STRING,
    agent_name STRING,
    total_status_time DOUBLE,
    instance_ts STRING,
    cumul_ute_time DOUBLE,
    unutilized_time DOUBLE,
    linear_ute_time DOUBLE,
    window_status_time FLOAT,
    max_slots INT,
    company_segments STRING,
    agent_id INT,
    partition_date STRING,
    partition_hour STRING
)
PARTITIONED BY (instance_date string, instance_hour string)
TBLPROPERTIES ('retention_policy'='Event Level - Without PII (3 Years)');

SELECT "Creating asp_asapp_rep_attributes";
CREATE TABLE IF NOT EXISTS asp_asapp_rep_attributes (
    rep_id INT,
    company_name STRING,
    created_ts STRING,
    rep_attribute_id BIGINT,
    external_rep_id STRING,
    external_agent_id STRING,
    attribute_value STRING,
    attribute_name STRING,
    agent_attribute_id BIGINT,
    agent_id INT,
    partition_date STRING,
    partition_hour STRING
)
PARTITIONED BY (created_date string, created_hour string)
TBLPROPERTIES ('retention_policy'='Event Level - Without PII (3 Years)');

SELECT "Creating asp_asapp_rep_convos";
CREATE TABLE IF NOT EXISTS asp_asapp_rep_convos (
    agent_first_response_ts STRING,
    rep_id INT,
    is_ghost_customer BOOLEAN,
    wrap_up_time_seconds DOUBLE,
    company_name STRING,
    auto_complete_msgs INT,
    rep_response_ct INT,
    avg_rep_response_seconds DOUBLE,
    auto_suggest_msgs INT,
    rep_utterance_count INT,
    company_subdivision STRING,
    handle_time_seconds FLOAT,
    cume_cust_response_seconds DOUBLE,
    custom_auto_complete_msgs INT,
    lead_time_seconds FLOAT,
    cust_response_ct INT,
    issue_id BIGINT,
    first_response_seconds FLOAT,
    instance_ts STRING,
    custom_auto_suggest_msgs INT,
    disposition_event_type STRING,
    cume_rep_response_seconds DOUBLE,
    kb_recommendation_msgs INT,
    customer_end_ts STRING,
    kb_search_msgs INT,
    dispositioned_ts STRING,
    rep_first_response_ts STRING,
    company_segments STRING,
    issue_assigned_ts STRING,
    conversation_id BIGINT,
    max_rep_response_seconds FLOAT,
    drawer_msgs INT,
    agent_id INT,
    cust_utterance_count INT,
    partition_date STRING,
    partition_hour STRING
)
PARTITIONED BY (instance_date string, instance_hour string)
TBLPROPERTIES ('retention_policy'='Event Level - Without PII (3 Years)');

SELECT "Creating asp_asapp_rep_hierarchy";
CREATE TABLE IF NOT EXISTS asp_asapp_rep_hierarchy (
    company_name STRING,
    superior_rep_id INT,
    subordinate_agent_id INT,
    superior_agent_id INT,
    subordinate_rep_id INT,
    reporting_relationship STRING
)
PARTITIONED BY (partition_date STRING, partition_hour STRING)
TBLPROPERTIES ('retention_policy'='Event Level - Without PII (3 Years)');

SELECT "Creating asp_asapp_rep_utilized";
CREATE TABLE IF NOT EXISTS asp_asapp_rep_utilized (
    company_id INT,
    rep_id INT,
    act_ratio DOUBLE,
    lin_ute_avail_min FLOAT,
    company_name STRING,
    cum_ute_avail_min FLOAT,
    lin_ute_busy_min FLOAT,
    company_subdivision STRING,
    ute_ratio DOUBLE,
    lin_avail_min FLOAT,
    cum_ute_prebreak_min FLOAT,
    rep_name STRING,
    lin_prebreak_min FLOAT,
    lin_logged_in_min FLOAT,
    cum_ute_busy_min FLOAT,
    instance_ts STRING,
    busy_clicks_ct INT,
    lin_ute_prebreak_min FLOAT,
    lin_busy_min FLOAT,
    cum_logged_in_min FLOAT,
    lin_ute_total_min FLOAT,
    max_slots INT,
    company_segments STRING,
    cum_prebreak_min FLOAT,
    labor_min FLOAT,
    cum_avail_min FLOAT,
    cum_ute_total_min FLOAT,
    cum_busy_min FLOAT,
    partition_date STRING,
    partition_hour STRING
)
PARTITIONED BY (instance_date string, instance_hour string)
TBLPROPERTIES ('retention_policy'='Event Level - Without PII (3 Years)');

SELECT "Creating asp_asapp_repmetrics";
CREATE TABLE IF NOT EXISTS asp_asapp_repmetrics (
    rep_id INT,
    logged_in_time DOUBLE,
    unassisted_issues INT,
    total_chats INT,
    disposition_count INT,
    web_assign_count INT,
    transfers_accepted INT,
    max_response_time DOUBLE,
    ios_issues_count INT,
    sms_assign_count INT,
    script_count INT,
    company_subdivision STRING,
    total_disposition_time DOUBLE,
    total_issues INT,
    max_handle_time DOUBLE,
    ios_assign_count INT,
    max_first_response_time DOUBLE,
    response_count INT,
    unresolved_issues INT,
    available_time DOUBLE,
    sms_issues_count INT,
    transfer_requests_received INT,
    autosuggest_count INT,
    total_response_time DOUBLE,
    total_first_response_time DOUBLE,
    instance_ts STRING,
    android_assign_count INT,
    unknown_issues_count INT,
    resolved_issues INT,
    first_response_count INT,
    transfers_requested INT,
    max_slots INT,
    timed_out_issues INT,
    company_segments STRING,
    cumulative_utilization_time DOUBLE,
    web_issues_count INT,
    android_issues_count INT,
    autocomplete_count INT,
    active_issues_count INT,
    manual_chats_count INT,
    linear_utilization_time DOUBLE,
    agent_id INT,
    unknown_assign_count INT,
    partition_date STRING,
    partition_hour STRING
)
PARTITIONED BY (instance_date string, instance_hour string)
TBLPROPERTIES ('retention_policy'='Event Level - Without PII (3 Years)');

SELECT "Creating asp_asapp_reps";
CREATE TABLE IF NOT EXISTS asp_asapp_reps (
    rep_id INT,
    name STRING,
    crm_rep_id STRING,
    crm_agent_id STRING,
    company_name STRING,
    agent_status INT,
    max_slot INT,
    disabled_time STRING,
    created_ts STRING,
    rep_status INT,
    agent_id INT,
    partition_date STRING,
    partition_hour STRING
)
PARTITIONED BY (created_date string, created_hour string)
TBLPROPERTIES ('retention_policy'='Event Level - Without PII (3 Years)');

SELECT "Creating asp_asapp_transfers";
CREATE TABLE IF NOT EXISTS asp_asapp_transfers (
    company_id INT,
    rep_id INT,
    actual_rep_transfer INT,
    requested_rep_transfer STRING,
    company_name STRING,
    actual_agent_transfer INT,
    group_transfer_from_name STRING,
    is_auto_transfer BOOLEAN,
    company_subdivision STRING,
    transfer_button_clicks INT,
    requested_agent_transfer STRING,
    accepted BOOLEAN,
    timestamp_req STRING,
    group_transfer_from STRING,
    issue_id BIGINT,
    group_transfer_to STRING,
    exit_transfer_event_type STRING,
    instance_ts STRING,
    timestamp_reply STRING,
    company_segments STRING,
    conversation_id BIGINT,
    group_transfer_to_name STRING,
    agent_id INT,
    partition_date STRING,
    partition_hour STRING
)
PARTITIONED BY (instance_date string, instance_hour string)
TBLPROPERTIES ('retention_policy'='Event Level - Without PII (3 Years)');
