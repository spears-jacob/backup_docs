USE ${env:ENVIRONMENT};

SELECT "Creating view asp_v_asapp_customer_feedback";
CREATE VIEW IF NOT EXISTS asp_v_asapp_customer_feedback
AS
SELECT DISTINCT
    question,
    last_agent_id,
    company_name,
    question_category,
    issue_id,
    instance_ts,
    question_type,
    ordering,
    answer,
    conversation_id,
    last_rep_id,
    instance_date,
    instance_hour
FROM asp_asapp_customer_feedback;

SELECT "Creating view asp_v_asapp_export_row_counts";
CREATE VIEW IF NOT EXISTS asp_v_asapp_export_row_counts AS
SELECT DISTINCT
    export_name,
    company_name,
    export_interval,
    exported_rows,
    export_date,
    partition_export_date
FROM asp_asapp_export_row_counts
;

SELECT "Creating view asp_v_asapp_flow_detail";
CREATE VIEW IF NOT EXISTS asp_v_asapp_flow_detail AS
SELECT DISTINCT
    link_resolved_pdl,
    link_resolved_pil,
    session_id,
    company_name,
    event_name,
    event_ts,
    issue_id,
    flow_name,
    event_type,
    flow_id,
    conversation_id,
    event_date,
    event_hour
FROM asp_asapp_flow_detail
;

SELECT "Creating view asp_v_asapp_intents";
CREATE VIEW IF NOT EXISTS asp_v_asapp_intents AS
SELECT DISTINCT
    name,
    default_disambiguation,
    company_name,
    short_description,
    code,
    flow_name,
    intent_type,
    actions,
    partition_date,
    partition_hour
FROM asp_asapp_intents
;

SELECT "Creating view asp_v_asapp_issue_queues";
CREATE VIEW IF NOT EXISTS asp_v_asapp_issue_queues AS
SELECT DISTINCT
    rep_id,
    enter_queue_flow_name,
    abandoned,
    company_name,
    enter_queue_eventflags,
    enqueue_time,
    company_subdivision,
    queue_id,
    issue_id,
    enter_queue_eventtype,
    instance_ts,
    enter_queue_message_name,
    exit_queue_eventflags,
    enter_queue_ts,
    exit_queue_ts,
    company_segments,
    conversation_id,
    exit_queue_eventtype,
    queue_name,
    agent_id,
    instance_date,
    instance_hour
FROM asp_asapp_issue_queues
;

SELECT "Creating view asp_v_asapp_rep_activity";
CREATE VIEW IF NOT EXISTS asp_v_asapp_rep_activity AS
SELECT DISTINCT
    status_description,
    rep_id,
    company_name,
    company_subdivision,
    status_id,
    in_status_starting_ts,
    rep_name,
    agent_name,
    total_status_time,
    instance_ts,
    cumul_ute_time,
    unutilized_time,
    linear_ute_time,
    window_status_time,
    max_slots,
    company_segments,
    agent_id,
    instance_date,
    instance_hour
FROM asp_asapp_rep_activity
;

SELECT "Creating view asp_v_asapp_rep_attributes";
CREATE VIEW IF NOT EXISTS asp_v_asapp_rep_attributes AS
SELECT DISTINCT
    rep_id,
    company_name,
    created_ts,
    rep_attribute_id,
    external_rep_id,
    external_agent_id,
    attribute_value,
    attribute_name,
    agent_attribute_id,
    agent_id,
    created_date,
    created_hour
FROM asp_asapp_rep_attributes
;

SELECT "Creating view asp_v_asapp_rep_convos";
CREATE VIEW IF NOT EXISTS asp_v_asapp_rep_convos AS
SELECT DISTINCT
    agent_first_response_ts,
    rep_id,
    is_ghost_customer,
    wrap_up_time_seconds,
    company_name,
    auto_complete_msgs,
    rep_response_ct,
    avg_rep_response_seconds,
    auto_suggest_msgs,
    rep_utterance_count,
    company_subdivision,
    handle_time_seconds,
    cume_cust_response_seconds,
    custom_auto_complete_msgs,
    lead_time_seconds,
    cust_response_ct,
    issue_id,
    first_response_seconds,
    instance_ts,
    custom_auto_suggest_msgs,
    disposition_event_type,
    cume_rep_response_seconds,
    kb_recommendation_msgs,
    customer_end_ts,
    kb_search_msgs,
    dispositioned_ts,
    rep_first_response_ts,
    company_segments,
    issue_assigned_ts,
    conversation_id,
    max_rep_response_seconds,
    drawer_msgs,
    agent_id,
    cust_utterance_count,
    instance_date,
    instance_hour
FROM asp_asapp_rep_convos
;

SELECT "Creating view asp_v_asapp_rep_hierarchy";
CREATE VIEW IF NOT EXISTS asp_v_asapp_rep_hierarchy AS
SELECT DISTINCT
    company_name,
    superior_rep_id,
    subordinate_agent_id,
    superior_agent_id,
    subordinate_rep_id,
    reporting_relationship,
    partition_date,
    partition_hour
FROM asp_asapp_rep_hierarchy
;

SELECT "Creating view asp_v_asapp_rep_utilized";
CREATE VIEW IF NOT EXISTS asp_v_asapp_rep_utilized AS
SELECT DISTINCT
    company_id,
    rep_id,
    act_ratio,
    lin_ute_avail_min,
    company_name,
    cum_ute_avail_min,
    lin_ute_busy_min,
    company_subdivision,
    ute_ratio,
    lin_avail_min,
    cum_ute_prebreak_min,
    rep_name,
    lin_prebreak_min,
    lin_logged_in_min,
    cum_ute_busy_min,
    instance_ts,
    busy_clicks_ct,
    lin_ute_prebreak_min,
    lin_busy_min,
    cum_logged_in_min,
    lin_ute_total_min,
    max_slots,
    company_segments,
    cum_prebreak_min,
    labor_min,
    cum_avail_min,
    cum_ute_total_min,
    cum_busy_min,
    instance_date,
    instance_hour
FROM asp_asapp_rep_utilized
;

SELECT "Creating view asp_v_asapp_repmetrics";
CREATE VIEW IF NOT EXISTS asp_v_asapp_repmetrics AS
SELECT DISTINCT
    rep_id,
    logged_in_time,
    unassisted_issues,
    total_chats,
    disposition_count,
    web_assign_count,
    transfers_accepted,
    max_response_time,
    ios_issues_count,
    sms_assign_count,
    script_count,
    company_subdivision,
    total_disposition_time,
    total_issues,
    max_handle_time,
    ios_assign_count,
    max_first_response_time,
    response_count,
    unresolved_issues,
    available_time,
    sms_issues_count,
    transfer_requests_received,
    autosuggest_count,
    total_response_time,
    total_first_response_time,
    instance_ts,
    android_assign_count,
    unknown_issues_count,
    resolved_issues,
    first_response_count,
    transfers_requested,
    max_slots,
    timed_out_issues,
    company_segments,
    cumulative_utilization_time,
    web_issues_count,
    android_issues_count,
    autocomplete_count,
    active_issues_count,
    manual_chats_count,
    linear_utilization_time,
    agent_id,
    unknown_assign_count,
    instance_date,
    instance_hour
FROM asp_asapp_repmetrics
;

SELECT "Creating view asp_v_asapp_reps";
CREATE VIEW IF NOT EXISTS asp_v_asapp_reps AS
SELECT DISTINCT
    rep_id,
    name,
    crm_rep_id,
    crm_agent_id,
    company_name,
    agent_status,
    max_slot,
    disabled_time,
    created_ts,
    rep_status,
    agent_id,
    created_date,
    created_hour
FROM asp_asapp_reps
;

SELECT "Creating view asp_v_asapp_transfers";
CREATE VIEW IF NOT EXISTS asp_v_asapp_transfers AS
SELECT DISTINCT
    company_id,
    rep_id,
    actual_rep_transfer,
    requested_rep_transfer,
    company_name,
    actual_agent_transfer,
    group_transfer_from_name,
    is_auto_transfer,
    company_subdivision,
    transfer_button_clicks,
    requested_agent_transfer,
    accepted,
    timestamp_req,
    group_transfer_from,
    issue_id,
    group_transfer_to,
    exit_transfer_event_type,
    instance_ts,
    timestamp_reply,
    company_segments,
    conversation_id,
    group_transfer_to_name,
    agent_id,
    instance_date,
    instance_hour
FROM asp_asapp_transfers
;

SELECT "Creating view asp_v_asapp_augmentation_analytics";
CREATE VIEW IF NOT EXISTS asp_v_asapp_augmentation_analytics AS
SELECT DISTINCT
    text_sent,
    company_id,
    rep_id,
    auto_suggestion,
    action_id,
    customer_id,
    company_name,
    external_customer_id,
    company_subdivision,
    is_custom,
    suggested_msg,
    external_session_id,
    event_create_ts,
    issue_id,
    external_rep_id,
    external_agent_id,
    edits,
    auto_completion,
    original,
    edited,
    company_segments,
    action_type,
    agent_id,
    event_create_date,
    event_create_hour
FROM asp_asapp_augmentation_analytics
;

SELECT "Creating view asp_v_asapp_convos_intents";
CREATE VIEW IF NOT EXISTS asp_v_asapp_convos_intents AS
SELECT DISTINCT
    first_utterance_text,
    customer_id,
    company_name,
    is_first_intent_correct,
    first_agent_id,
    first_rep_id,
    final_intent_code,
    ftd_visit,
    faq_id,
    issue_id,
    first_intent_code,
    first_utterance_ts,
    intent_path,
    first_intent_code_alt,
    final_action_destination,
    conversation_id,
    disambig_count,
    first_utterance_date,
    first_utterance_hour
FROM asp_asapp_convos_intents
;

SELECT "Creating view asp_v_asapp_convos_intents_ended";
CREATE VIEW IF NOT EXISTS asp_v_asapp_convos_intents_ended AS
SELECT DISTINCT
    first_utterance_text,
    customer_id,
    company_name,
    is_first_intent_correct,
    first_agent_id,
    first_rep_id,
    final_intent_code,
    ftd_visit,
    faq_id,
    issue_id,
    first_intent_code,
    first_utterance_ts,
    intent_path,
    first_intent_code_alt,
    final_action_destination,
    conversation_id,
    disambig_count,
    first_utterance_date,
    first_utterance_hour
FROM asp_asapp_convos_intents_ended
;

SELECT "Creating view asp_v_asapp_convos_metadata";
CREATE VIEW IF NOT EXISTS asp_v_asapp_convos_metadata AS
SELECT DISTINCT
    first_utterance_text,
    company_id,
    issue_created_ts,
    last_event_ts,
    is_review_required,
    session_id,
    customer_id,
    last_agent_id,
    company_name,
    sentiment_valence,
    app_version_asapp,
    internal_session_id,
    ended_resolved,
    last_srs_event_ts,
    company_subdivision,
    escalated_to_chat,
    internal_session_type,
    first_agent_id,
    trigger_link,
    external_user_id,
    first_rep_id,
    ended_auto,
    external_channel,
    external_session_id,
    ended_other,
    disposition_notes,
    internal_user_identifier,
    issue_queue_name,
    device_type,
    auth_state,
    conversation_end_ts,
    ended_unresolved,
    issue_id,
    external_issue_id,
    end_srs_selection,
    external_session_type,
    external_rep_id,
    disposition_ts,
    mid_issue_auth_ts,
    external_agent_id,
    auth_source,
    disposition_event_type,
    first_utterance_ts,
    auth_external_user_type,
    session_event_type,
    external_user_type,
    auth_external_user_id,
    assigned_to_rep_time,
    app_version_client,
    session_metadata,
    company_segments,
    platform,
    csat_rating,
    deep_link_queue,
    issue_queue_id,
    ended_timeout,
    last_sequence_id,
    last_rep_id,
    termination_event_type,
    internal_user_session_type,
    session_type,
    auth_external_token_id,
    last_event_date,
    last_event_hour
FROM asp_asapp_convos_metadata
;

SELECT "Creating view asp_v_asapp_convos_metadata_ended";
CREATE VIEW IF NOT EXISTS asp_v_asapp_convos_metadata_ended AS
SELECT DISTINCT
    first_utterance_text,
    company_id,
    issue_created_ts,
    last_event_ts,
    is_review_required,
    session_id,
    customer_id,
    last_agent_id,
    company_name,
    sentiment_valence,
    app_version_asapp,
    internal_session_id,
    ended_resolved,
    last_srs_event_ts,
    company_subdivision,
    escalated_to_chat,
    internal_session_type,
    first_agent_id,
    trigger_link,
    external_user_id,
    first_rep_id,
    ended_auto,
    external_channel,
    external_session_id,
    ended_other,
    disposition_notes,
    internal_user_identifier,
    issue_queue_name,
    device_type,
    auth_state,
    conversation_end_ts,
    ended_unresolved,
    issue_id,
    external_issue_id,
    end_srs_selection,
    external_session_type,
    external_rep_id,
    disposition_ts,
    mid_issue_auth_ts,
    external_agent_id,
    auth_source,
    disposition_event_type,
    first_utterance_ts,
    auth_external_user_type,
    session_event_type,
    external_user_type,
    auth_external_user_id,
    assigned_to_rep_time,
    app_version_client,
    session_metadata,
    company_segments,
    platform,
    csat_rating,
    deep_link_queue,
    issue_queue_id,
    ended_timeout,
    last_sequence_id,
    last_rep_id,
    termination_event_type,
    internal_user_session_type,
    session_type,
    auth_external_token_id,
    last_event_date,
    last_event_hour
FROM asp_asapp_convos_metadata_ended
;

SELECT "Creating view asp_v_asapp_convos_metrics";
CREATE VIEW IF NOT EXISTS asp_v_asapp_convos_metrics AS
SELECT DISTINCT
    customer_response_count,
    rep_sent_msgs,
    out_business_ct,
    customer_id,
    auto_wait_for_agent_msgs,
    company_name,
    auto_complete_msgs,
    auto_suggest_msgs,
    company_subdivision,
    first_rep_response_count,
    total_session_time,
    agent_sent_msgs,
    customer_sent_msgs,
    agent_response_count,
    total_handle_time,
    device_type,
    rep_response_count,
    customer_wait_for_agent_msgs,
    total_rep_seconds_to_respond,
    auto_wait_for_rep_msgs,
    issue_id,
    total_wrap_up_time,
    assisted,
    first_utterance_ts,
    auto_generated_msgs,
    attempted_chat,
    company_segments,
    platform,
    total_cust_seconds_to_respond,
    conversation_id,
    time_in_queue,
    total_lead_time,
    total_seconds_to_first_rep_response,
    customer_wait_for_rep_msgs,
    first_utterance_date,
    first_utterance_hour
FROM asp_asapp_convos_metrics
;

SELECT "Creating view asp_v_asapp_convos_metrics_ended";
CREATE VIEW IF NOT EXISTS asp_v_asapp_convos_metrics_ended AS
SELECT DISTINCT
    customer_response_count,
    rep_sent_msgs,
    out_business_ct,
    customer_id,
    auto_wait_for_agent_msgs,
    company_name,
    auto_complete_msgs,
    auto_suggest_msgs,
    company_subdivision,
    first_rep_response_count,
    total_session_time,
    agent_sent_msgs,
    customer_sent_msgs,
    agent_response_count,
    total_handle_time,
    device_type,
    rep_response_count,
    customer_wait_for_agent_msgs,
    total_rep_seconds_to_respond,
    auto_wait_for_rep_msgs,
    issue_id,
    total_wrap_up_time,
    assisted,
    first_utterance_ts,
    auto_generated_msgs,
    attempted_chat,
    company_segments,
    platform,
    total_cust_seconds_to_respond,
    conversation_id,
    time_in_queue,
    total_lead_time,
    total_seconds_to_first_rep_response,
    customer_wait_for_rep_msgs,
    first_utterance_date,
    first_utterance_hour
FROM asp_asapp_convos_metrics_ended
;

SELECT "Creating view asp_v_asapp_csid_containment";
CREATE VIEW IF NOT EXISTS asp_v_asapp_csid_containment AS
SELECT DISTINCT
    company_id,
    was_enqueued,
    first_auth_external_user_id,
    csid,
    customer_id,
    first_auth_external_user_type,
    company_name,
    first_auth_external_token_id,
    last_auth_external_user_type,
    agents_involved,
    has_customer_utterance,
    external_customer_id,
    last_auth_source,
    company_subdivision,
    fgsrs_event_count,
    attempted_escalate,
    rep_msgs,
    last_auth_external_user_id,
    last_device_type,
    is_contained,
    csid_start_ts,
    instance_ts,
    included_issues,
    last_auth_external_token_id,
    first_auth_source,
    messages_sent,
    event_count,
    distinct_auth_source_path,
    company_segments,
    last_platform,
    csid_end_ts,
    reps_involved,
    instance_date,
    instance_hour
FROM asp_asapp_csid_containment
;

SELECT "Creating view asp_v_asapp_customer_params";
CREATE VIEW IF NOT EXISTS asp_v_asapp_customer_params AS
SELECT DISTINCT
    company_id,
    rep_id,
    session_id,
    customer_id,
    company_name,
    company_subdivision,
    params,
    param_value,
    event_ts,
    auth_state,
    issue_id,
    instance_ts,
    param_key,
    company_segments,
    platform,
    event_id,
    referring_page_url,
    instance_date,
    instance_hour
FROM asp_asapp_customer_params
;

SELECT "Creating view asp_v_asapp_flow_completions";
CREATE VIEW IF NOT EXISTS asp_v_asapp_flow_completions AS
SELECT DISTINCT
    customer_session_id,
    company_id,
    customer_id,
    company_name,
    is_flow_success_issue,
    company_subdivision,
    is_flow_success_event,
    success_event_ts,
    external_user_id,
    issue_id,
    negation_event_ts,
    success_rule_id,
    success_event_details,
    company_segments,
    platform,
    negation_rule_id,
    conversation_id,
    success_event_date,
    success_event_hour
FROM asp_asapp_flow_completions
;

SELECT "Creating view asp_v_asapp_rep_augmentation";
CREATE VIEW IF NOT EXISTS asp_v_asapp_rep_augmentation AS
SELECT DISTINCT
    is_rep_resolved,
    company_id,
    rep_id,
    customer_id,
    company_name,
    auto_complete_msgs,
    auto_suggest_msgs,
    external_customer_id,
    company_subdivision,
    custom_auto_complete_msgs,
    conversation_end_ts,
    issue_id,
    instance_ts,
    custom_auto_suggest_msgs,
    kb_recommendation_msgs,
    kb_search_msgs,
    is_billable,
    company_segments,
    did_customer_timeout,
    conversation_id,
    drawer_msgs,
    agent_id,
    instance_date,
    instance_hour
FROM asp_asapp_rep_augmentation
;

SELECT "Creating view asp_v_asapp_sdk_events";
CREATE VIEW IF NOT EXISTS asp_v_asapp_sdk_events AS
SELECT DISTINCT
    company_id,
    stripped_trigger_link,
    customer_id,
    company_name,
    company_subdivision,
    created_ts,
    instance_ts,
    app_id,
    company_segments,
    raw_trigger_link,
    instance_date,
    instance_hour
FROM asp_asapp_sdk_events
;

SELECT "Creating view asp_v_asapp_utterances";
CREATE VIEW IF NOT EXISTS asp_v_asapp_utterances AS
SELECT DISTINCT
    sent_to_rep,
    sequence_id,
    company_name,
    sender_id,
    company_subdivision,
    sender_type,
    created_ts,
    issue_id,
    instance_ts,
    sent_to_agent,
    utterance,
    company_segments,
    conversation_id,
    utterance_type,
    instance_date,
    instance_hour
FROM asp_asapp_utterances
;
