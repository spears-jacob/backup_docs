USE ${env:ENVIRONMENT};

SELECT "Creating asp_asapp_augmentation_analytics";
CREATE TABLE IF NOT EXISTS asp_asapp_augmentation_analytics (
    text_sent STRING,
    company_id INT,
    rep_id INT,
    auto_suggestion STRING,
    action_id STRING,
    customer_id STRING,
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
    agent_id INT,
    partition_date STRING,
    partition_hour STRING
)
PARTITIONED BY (event_create_date string, event_create_hour string)
TBLPROPERTIES ('retention_policy'='Event Level - With PII (1 Year)');

SELECT "Creating asp_asapp_convos_intents";
CREATE TABLE IF NOT EXISTS asp_asapp_convos_intents (
    first_utterance_text STRING,
    customer_id STRING,
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
    disambig_count INT,
    partition_date STRING,
    partition_hour STRING
)
PARTITIONED BY (first_utterance_date string, first_utterance_hour string)
TBLPROPERTIES ('retention_policy'='Event Level - With PII (1 Year)');

SELECT "Creating asp_asapp_convos_intents_ended";
CREATE TABLE IF NOT EXISTS asp_asapp_convos_intents_ended (
    first_utterance_text STRING,
    customer_id STRING,
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
    disambig_count INT,
    partition_date STRING,
    partition_hour STRING
)
PARTITIONED BY (first_utterance_date string, first_utterance_hour string)
TBLPROPERTIES ('retention_policy'='Event Level - With PII (1 Year)');

SELECT "Creating asp_asapp_convos_metadata";
CREATE TABLE IF NOT EXISTS asp_asapp_convos_metadata (
    first_utterance_text STRING,
    company_id INT,
    issue_created_ts STRING,
    last_event_ts STRING,
    is_review_required STRING,
    session_id STRING,
    customer_id STRING,
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
    auth_external_token_id STRING,
    partition_date STRING,
    partition_hour STRING
)
PARTITIONED BY (last_event_date string, last_event_hour string)
TBLPROPERTIES ('retention_policy'='Event Level - With PII (1 Year)');

SELECT "Creating asp_asapp_convos_metadata_ended";
CREATE TABLE IF NOT EXISTS asp_asapp_convos_metadata_ended (
    first_utterance_text STRING,
    company_id INT,
    issue_created_ts STRING,
    last_event_ts STRING,
    is_review_required STRING,
    session_id STRING,
    customer_id STRING,
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
    auth_external_token_id STRING,
    partition_date STRING,
    partition_hour STRING
)
PARTITIONED BY (last_event_date string, last_event_hour string)
TBLPROPERTIES ('retention_policy'='Event Level - With PII (1 Year)');

SELECT "Creating asp_asapp_convos_metrics";
CREATE TABLE IF NOT EXISTS asp_asapp_convos_metrics (
    customer_response_count INT,
    rep_sent_msgs INT,
    out_business_ct INT,
    customer_id STRING,
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
    customer_wait_for_rep_msgs INT,
    partition_date STRING,
    partition_hour STRING
)
PARTITIONED BY (first_utterance_date string, first_utterance_hour string)
TBLPROPERTIES ('retention_policy'='Event Level - With PII (1 Year)');

SELECT "Creating asp_asapp_convos_metrics_ended";
CREATE TABLE IF NOT EXISTS asp_asapp_convos_metrics_ended (
    customer_response_count INT,
    rep_sent_msgs INT,
    out_business_ct INT,
    customer_id STRING,
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
    customer_wait_for_rep_msgs INT,
    partition_date STRING,
    partition_hour STRING
)
PARTITIONED BY (first_utterance_date string, first_utterance_hour string)
TBLPROPERTIES ('retention_policy'='Event Level - With PII (1 Year)');

SELECT "Creating asp_asapp_csid_containment";
CREATE TABLE IF NOT EXISTS asp_asapp_csid_containment (
    company_id INT,
    was_enqueued BOOLEAN,
    first_auth_external_user_id STRING,
    csid STRING,
    customer_id STRING,
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
    reps_involved STRING,
    partition_date STRING,
    partition_hour STRING
)
PARTITIONED BY (instance_date string, instance_hour string)
TBLPROPERTIES ('retention_policy'='Event Level - With PII (1 Year)');

SELECT "Creating asp_asapp_customer_params";
CREATE TABLE IF NOT EXISTS asp_asapp_customer_params (
    company_id INT,
    rep_id INT,
    session_id STRING,
    customer_id STRING,
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
    referring_page_url STRING,
    partition_date STRING,
    partition_hour STRING
)
PARTITIONED BY (instance_date string, instance_hour string)
TBLPROPERTIES ('retention_policy'='Event Level - With PII (1 Year)');

SELECT "Creating asp_asapp_flow_completions";
CREATE TABLE IF NOT EXISTS asp_asapp_flow_completions (
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
    conversation_id STRING,
    partition_date STRING,
    partition_hour STRING
)
PARTITIONED BY (success_event_date string, success_event_hour string)
TBLPROPERTIES ('retention_policy'='Event Level - With PII (1 Year)');

SELECT "Creating asp_asapp_rep_augmentation";
CREATE TABLE IF NOT EXISTS asp_asapp_rep_augmentation (
    is_rep_resolved BOOLEAN,
    company_id INT,
    rep_id INT,
    customer_id STRING,
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
    agent_id INT,
    partition_date STRING,
    partition_hour STRING
)
PARTITIONED BY (instance_date string, instance_hour string)
TBLPROPERTIES ('retention_policy'='Event Level - With PII (1 Year)');

SELECT "Creating asp_asapp_sdk_events";
CREATE TABLE IF NOT EXISTS asp_asapp_sdk_events (
    company_id INT,
    stripped_trigger_link STRING,
    customer_id STRING,
    company_name STRING,
    company_subdivision STRING,
    created_ts STRING,
    instance_ts STRING,
    app_id STRING,
    company_segments STRING,
    raw_trigger_link STRING,
    partition_date STRING,
    partition_hour STRING
)
PARTITIONED BY (instance_date string, instance_hour string)
TBLPROPERTIES ('retention_policy'='Event Level - With PII (1 Year)');

SELECT "Creating asp_asapp_utterances";
CREATE TABLE IF NOT EXISTS asp_asapp_utterances (
    sent_to_rep BOOLEAN,
    sequence_id STRING,
    company_name STRING,
    sender_id STRING,
    company_subdivision STRING,
    sender_type STRING,
    created_ts STRING,
    issue_id BIGINT,
    instance_ts STRING,
    sent_to_agent BOOLEAN,
    utterance STRING,
    company_segments STRING,
    conversation_id BIGINT,
    utterance_type STRING,
    partition_date STRING,
    partition_hour STRING
)
PARTITIONED BY (instance_date string, instance_hour string)
TBLPROPERTIES ('retention_policy'='Event Level - With PII (1 Year)');
