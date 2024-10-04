USE nifi;

CREATE EXTERNAL TABLE IF NOT EXISTS asp_asapp_augmentation_analytics (
    text_sent STRING,
    company_id INT,
    rep_id INT,
    auto_suggestion STRING,
    action_id STRING,
    customer_id BIGINT,
    company_name STRING,
    external_customer_id STRING,
    company_subdivision STRING,
    is_custom INT,
    suggested_msg STRING,
    external_session_id STRING,
    event_create_ts STRING,
    issue_id BIGINT,
    external_rep_id STRING,
    external_agent_id STRING,
    edits STRING,
    auto_completion STRING,
    original STRING,
    edited INT,
    company_segments STRING,
    action_type STRING,
    agent_id INT
)
PARTITIONED BY (partition_date string, partition_hour string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
with serdeproperties (
"separatorChar" = "\n"
)
stored as textfile;

CREATE EXTERNAL TABLE if not exists asp_asapp_convos_intents (
    first_utterance_text STRING,
    customer_id BIGINT,
    company_name STRING,
    is_first_intent_correct BOOLEAN,
    first_agent_id INT,
    first_rep_id INT,
    final_intent_code STRING,
    ftd_visit BOOLEAN,
    faq_id STRING,
    issue_id STRING,
    first_intent_code STRING,
    first_utterance_ts STRING,
    intent_path STRING,
    first_intent_code_alt STRING,
    final_action_destination STRING,
    conversation_id STRING,
    disambig_count INT
)
PARTITIONED BY (partition_date string, partition_hour string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
with serdeproperties (
"separatorChar" = "\n"
)
stored as textfile;

CREATE EXTERNAL TABLE IF NOT EXISTS asp_asapp_convos_intents_ended (
    first_utterance_text STRING,
    customer_id BIGINT,
    company_name STRING,
    is_first_intent_correct BOOLEAN,
    first_agent_id INT,
    first_rep_id INT,
    final_intent_code STRING,
    ftd_visit BOOLEAN,
    faq_id STRING,
    issue_id STRING,
    first_intent_code STRING,
    first_utterance_ts STRING,
    intent_path STRING,
    first_intent_code_alt STRING,
    final_action_destination STRING,
    conversation_id STRING,
    disambig_count INT
)
PARTITIONED BY (partition_date string, partition_hour string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
with serdeproperties (
"separatorChar" = "\n"
)
stored as textfile;

CREATE EXTERNAL TABLE IF NOT EXISTS asp_asapp_convos_metadata (
    first_utterance_text STRING,
    company_id INT,
    issue_created_ts STRING,
    last_event_ts STRING,
    is_review_required STRING,
    session_id STRING,
    customer_id BIGINT,
    last_agent_id INT,
    company_name STRING,
    sentiment_valence STRING,
    app_version_asapp STRING,
    internal_session_id STRING,
    ended_resolved INT,
    last_srs_event_ts STRING,
    company_subdivision STRING,
    escalated_to_chat STRING,
    internal_session_type STRING,
    first_agent_id INT,
    trigger_link STRING,
    external_user_id STRING,
    first_rep_id INT,
    ended_auto INT,
    external_channel STRING,
    external_session_id STRING,
    ended_other INT,
    disposition_notes STRING,
    internal_user_identifier STRING,
    issue_queue_name STRING,
    device_type STRING,
    auth_state STRING,
    conversation_end_ts STRING,
    ended_unresolved INT,
    issue_id BIGINT,
    external_issue_id STRING,
    end_srs_selection STRING,
    external_session_type STRING,
    external_rep_id STRING,
    disposition_ts STRING,
    mid_issue_auth_ts STRING,
    external_agent_id STRING,
    auth_source STRING,
    disposition_event_type STRING,
    first_utterance_ts STRING,
    auth_external_user_type STRING,
    session_event_type STRING,
    external_user_type STRING,
    auth_external_user_id STRING,
    assigned_to_rep_time STRING,
    app_version_client STRING,
    session_metadata STRING,
    company_segments STRING,
    platform STRING,
    csat_rating FLOAT,
    deep_link_queue STRING,
    issue_queue_id STRING,
    ended_timeout INT,
    last_sequence_id SMALLINT,
    last_rep_id INT,
    termination_event_type STRING,
    internal_user_session_type STRING,
    session_type STRING,
    auth_external_token_id STRING
)
PARTITIONED BY (partition_date string, partition_hour string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
with serdeproperties (
"separatorChar" = "\n"
)
stored as textfile;

CREATE EXTERNAL TABLE IF NOT EXISTS asp_asapp_convos_metadata_ended (
    first_utterance_text STRING,
    company_id INT,
    issue_created_ts STRING,
    last_event_ts STRING,
    is_review_required STRING,
    session_id STRING,
    customer_id BIGINT,
    last_agent_id INT,
    company_name STRING,
    sentiment_valence STRING,
    app_version_asapp STRING,
    internal_session_id STRING,
    ended_resolved INT,
    last_srs_event_ts STRING,
    company_subdivision STRING,
    escalated_to_chat STRING,
    internal_session_type STRING,
    first_agent_id INT,
    trigger_link STRING,
    external_user_id STRING,
    first_rep_id INT,
    ended_auto INT,
    external_channel STRING,
    external_session_id STRING,
    ended_other INT,
    disposition_notes STRING,
    internal_user_identifier STRING,
    issue_queue_name STRING,
    device_type STRING,
    auth_state STRING,
    conversation_end_ts STRING,
    ended_unresolved INT,
    issue_id BIGINT,
    external_issue_id STRING,
    end_srs_selection STRING,
    external_session_type STRING,
    external_rep_id STRING,
    disposition_ts STRING,
    mid_issue_auth_ts STRING,
    external_agent_id STRING,
    auth_source STRING,
    disposition_event_type STRING,
    first_utterance_ts STRING,
    auth_external_user_type STRING,
    session_event_type STRING,
    external_user_type STRING,
    auth_external_user_id STRING,
    assigned_to_rep_time STRING,
    app_version_client STRING,
    session_metadata STRING,
    company_segments STRING,
    platform STRING,
    csat_rating FLOAT,
    deep_link_queue STRING,
    issue_queue_id STRING,
    ended_timeout INT,
    last_sequence_id SMALLINT,
    last_rep_id INT,
    termination_event_type STRING,
    internal_user_session_type STRING,
    session_type STRING,
    auth_external_token_id STRING
)
PARTITIONED BY (partition_date string, partition_hour string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
with serdeproperties (
"separatorChar" = "\n"
)
stored as textfile;

CREATE EXTERNAL TABLE IF NOT EXISTS asp_asapp_convos_metrics (
    customer_response_count INT,
    rep_sent_msgs INT,
    out_business_ct INT,
    customer_id BIGINT,
    auto_wait_for_agent_msgs INT,
    company_name STRING,
    auto_complete_msgs INT,
    auto_suggest_msgs INT,
    company_subdivision STRING,
    first_rep_response_count INT,
    total_session_time DOUBLE,
    agent_sent_msgs INT,
    customer_sent_msgs INT,
    agent_response_count INT,
    total_handle_time DOUBLE,
    device_type STRING,
    rep_response_count INT,
    customer_wait_for_agent_msgs INT,
    total_rep_seconds_to_respond DOUBLE,
    auto_wait_for_rep_msgs INT,
    issue_id BIGINT,
    total_wrap_up_time DOUBLE,
    assisted INT,
    first_utterance_ts STRING,
    auto_generated_msgs INT,
    attempted_chat INT,
    company_segments STRING,
    platform STRING,
    total_cust_seconds_to_respond DOUBLE,
    conversation_id BIGINT,
    time_in_queue DOUBLE,
    total_lead_time DOUBLE,
    total_seconds_to_first_rep_response DOUBLE,
    customer_wait_for_rep_msgs INT
)
PARTITIONED BY (partition_date string, partition_hour string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
with serdeproperties (
"separatorChar" = "\n"
)
stored as textfile;

CREATE EXTERNAL TABLE IF NOT EXISTS asp_asapp_convos_metrics_ended (
    customer_response_count INT,
    rep_sent_msgs INT,
    out_business_ct INT,
    customer_id BIGINT,
    auto_wait_for_agent_msgs INT,
    company_name STRING,
    auto_complete_msgs INT,
    auto_suggest_msgs INT,
    company_subdivision STRING,
    first_rep_response_count INT,
    total_session_time DOUBLE,
    agent_sent_msgs INT,
    customer_sent_msgs INT,
    agent_response_count INT,
    total_handle_time DOUBLE,
    device_type STRING,
    rep_response_count INT,
    customer_wait_for_agent_msgs INT,
    total_rep_seconds_to_respond DOUBLE,
    auto_wait_for_rep_msgs INT,
    issue_id BIGINT,
    total_wrap_up_time DOUBLE,
    assisted INT,
    first_utterance_ts STRING,
    auto_generated_msgs INT,
    attempted_chat INT,
    company_segments STRING,
    platform STRING,
    total_cust_seconds_to_respond DOUBLE,
    conversation_id BIGINT,
    time_in_queue DOUBLE,
    total_lead_time DOUBLE,
    total_seconds_to_first_rep_response DOUBLE,
    customer_wait_for_rep_msgs INT
)
PARTITIONED BY (partition_date string, partition_hour string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
with serdeproperties (
"separatorChar" = "\n"
)
stored as textfile;

CREATE EXTERNAL TABLE IF NOT EXISTS asp_asapp_csid_containment (
    company_id INT,
    was_enqueued BOOLEAN,
    first_auth_external_user_id STRING,
    csid STRING,
    customer_id BIGINT,
    first_auth_external_user_type STRING,
    company_name STRING,
    first_auth_external_token_id STRING,
    last_auth_external_user_type STRING,
    agents_involved STRING,
    has_customer_utterance BOOLEAN,
    external_customer_id STRING,
    last_auth_source STRING,
    company_subdivision STRING,
    fgsrs_event_count SMALLINT,
    attempted_escalate BOOLEAN,
    rep_msgs INT,
    last_auth_external_user_id STRING,
    last_device_type STRING,
    is_contained BOOLEAN,
    csid_start_ts STRING,
    instance_ts STRING,
    included_issues STRING,
    last_auth_external_token_id STRING,
    first_auth_source STRING,
    messages_sent INT,
    event_count SMALLINT,
    distinct_auth_source_path STRING,
    company_segments STRING,
    last_platform STRING,
    csid_end_ts STRING,
    reps_involved STRING
)
PARTITIONED BY (partition_date string, partition_hour string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
with serdeproperties (
"separatorChar" = "\n"
)
stored as textfile;

CREATE EXTERNAL TABLE IF NOT EXISTS asp_asapp_customer_feedback (
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
    last_rep_id INT
)
PARTITIONED BY (partition_date string, partition_hour string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
with serdeproperties (
"separatorChar" = "\n"
)
stored as textfile;

CREATE EXTERNAL TABLE IF NOT EXISTS asp_asapp_customer_params (
    company_id INT,
    rep_id INT,
    session_id STRING,
    customer_id BIGINT,
    company_name STRING,
    company_subdivision STRING,
    params STRING,
    param_value STRING,
    event_ts STRING,
    auth_state BOOLEAN,
    issue_id BIGINT,
    instance_ts STRING,
    param_key STRING,
    company_segments STRING,
    platform STRING,
    event_id STRING,
    referring_page_url STRING
)
PARTITIONED BY (partition_date string, partition_hour string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
with serdeproperties (
"separatorChar" = "\n"
)
stored as textfile;

CREATE EXTERNAL TABLE IF NOT EXISTS asp_asapp_export_row_counts (
    export_name STRING,
    company_name STRING,
    export_date STRING,
    export_interval STRING,
    exported_rows INT
)
PARTITIONED BY (partition_date string, partition_hour string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
with serdeproperties (
"separatorChar" = "\n"
)
stored as textfile;

CREATE EXTERNAL TABLE IF NOT EXISTS asp_asapp_flow_completions (
    customer_session_id STRING,
    company_id INT,
    customer_id STRING,
    company_name STRING,
    is_flow_success_issue BOOLEAN,
    company_subdivision STRING,
    is_flow_success_event BOOLEAN,
    success_event_ts STRING,
    external_user_id STRING,
    issue_id STRING,
    negation_event_ts STRING,
    success_rule_id STRING,
    success_event_details STRING,
    company_segments STRING,
    platform STRING,
    negation_rule_id STRING,
    conversation_id STRING
)
PARTITIONED BY (partition_date string, partition_hour string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
with serdeproperties (
"separatorChar" = "\n"
)
stored as textfile;

CREATE EXTERNAL TABLE IF NOT EXISTS asp_asapp_flow_detail (
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
    conversation_id STRING
)
PARTITIONED BY (partition_date string, partition_hour string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
with serdeproperties (
"separatorChar" = "\n"
)
stored as textfile;

CREATE EXTERNAL TABLE IF NOT EXISTS asp_asapp_intents (
    name STRING,
    default_disambiguation BOOLEAN,
    company_name STRING,
    short_description STRING,
    code STRING,
    flow_name STRING,
    intent_type STRING,
    actions STRING
)
PARTITIONED BY (partition_date string, partition_hour string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
with serdeproperties (
"separatorChar" = "\n"
)
stored as textfile;

CREATE EXTERNAL TABLE IF NOT EXISTS asp_asapp_issue_queues (
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
    agent_id INT
)
PARTITIONED BY (partition_date string, partition_hour string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
with serdeproperties (
"separatorChar" = "\n"
)
stored as textfile;

CREATE EXTERNAL TABLE IF NOT EXISTS asp_asapp_rep_activity (
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
    agent_id INT
)
PARTITIONED BY (partition_date string, partition_hour string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
with serdeproperties (
"separatorChar" = "\n"
)
stored as textfile;

CREATE EXTERNAL TABLE IF NOT EXISTS asp_asapp_rep_attributes (
    rep_id INT,
    company_name STRING,
    created_ts STRING,
    rep_attribute_id BIGINT,
    external_rep_id STRING,
    external_agent_id STRING,
    attribute_value STRING,
    attribute_name STRING,
    agent_attribute_id BIGINT,
    agent_id INT
)
PARTITIONED BY (partition_date string, partition_hour string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
with serdeproperties (
"separatorChar" = "\n"
)
stored as textfile;

CREATE EXTERNAL TABLE IF NOT EXISTS asp_asapp_rep_augmentation (
    is_rep_resolved BOOLEAN,
    company_id INT,
    rep_id INT,
    customer_id BIGINT,
    company_name STRING,
    auto_complete_msgs INT,
    auto_suggest_msgs INT,
    external_customer_id STRING,
    company_subdivision STRING,
    custom_auto_complete_msgs INT,
    conversation_end_ts STRING,
    issue_id BIGINT,
    instance_ts STRING,
    custom_auto_suggest_msgs INT,
    kb_recommendation_msgs INT,
    kb_search_msgs INT,
    is_billable BOOLEAN,
    company_segments STRING,
    did_customer_timeout BOOLEAN,
    conversation_id BIGINT,
    drawer_msgs INT,
    agent_id INT
)
PARTITIONED BY (partition_date string, partition_hour string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
with serdeproperties (
"separatorChar" = "\n"
)
stored as textfile;

CREATE EXTERNAL TABLE IF NOT EXISTS asp_asapp_rep_convos (
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
    cust_utterance_count INT
)
PARTITIONED BY (partition_date string, partition_hour string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
with serdeproperties (
"separatorChar" = "\n"
)
stored as textfile;

CREATE EXTERNAL TABLE IF NOT EXISTS asp_asapp_rep_hierarchy (
    company_name STRING,
    superior_rep_id INT,
    subordinate_agent_id INT,
    superior_agent_id INT,
    subordinate_rep_id INT,
    reporting_relationship STRING
)
PARTITIONED BY (partition_date string, partition_hour string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
with serdeproperties (
"separatorChar" = "\n"
)
stored as textfile;

CREATE EXTERNAL TABLE IF NOT EXISTS asp_asapp_rep_utilized (
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
    cum_busy_min FLOAT
)
PARTITIONED BY (partition_date string, partition_hour string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
with serdeproperties (
"separatorChar" = "\n"
)
stored as textfile;

CREATE EXTERNAL TABLE IF NOT EXISTS asp_asapp_repmetrics (
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
    unknown_assign_count INT
)
PARTITIONED BY (partition_date string, partition_hour string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
with serdeproperties (
"separatorChar" = "\n"
)
stored as textfile;

CREATE EXTERNAL TABLE IF NOT EXISTS asp_asapp_reps (
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
    agent_id INT
)
PARTITIONED BY (partition_date string, partition_hour string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
with serdeproperties (
"separatorChar" = "\n"
)
stored as textfile;

CREATE EXTERNAL TABLE IF NOT EXISTS asp_asapp_sdk_events (
    company_id INT,
    stripped_trigger_link STRING,
    customer_id STRING,
    company_name STRING,
    company_subdivision STRING,
    created_ts STRING,
    instance_ts STRING,
    app_id STRING,
    company_segments STRING,
    raw_trigger_link STRING
)
PARTITIONED BY (partition_date string, partition_hour string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
with serdeproperties (
"separatorChar" = "\n"
)
stored as textfile;

CREATE EXTERNAL TABLE IF NOT EXISTS asp_asapp_transfers (
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
    agent_id INT
)
PARTITIONED BY (partition_date string, partition_hour string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
with serdeproperties (
"separatorChar" = "\n"
)
stored as textfile;

CREATE EXTERNAL TABLE IF NOT EXISTS asp_asapp_utterances (
    sent_to_rep BOOLEAN,
    sequence_id STRING,
    company_name STRING,
    sender_id BIGINT,
    company_subdivision STRING,
    sender_type STRING,
    created_ts STRING,
    issue_id BIGINT,
    instance_ts STRING,
    sent_to_agent BOOLEAN,
    utterance STRING,
    company_segments STRING,
    conversation_id BIGINT,
    utterance_type STRING
)
PARTITIONED BY (partition_date string, partition_hour string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
with serdeproperties (
"separatorChar" = "\n"
)
stored as textfile;
