USE ${env:DB_TEMP};

add jar hdfs:///udf/json-serde-1.3.9-SNAPSHOT-jar-with-dependencies.jar;


SELECT "Truncating temp asp_asapp_augmentation_analytics - 1";
MSCK REPAIR TABLE ${env:DB_NIFI}.asp_asapp_augmentation_analytics;
TRUNCATE TABLE ${env:DB_TEMP}.asp_asapp_augmentation_analytics;
TRUNCATE TABLE ${env:DB_TEMP}.asp_asapp_augmentation_analytics_new;

SELECT "Inserting temp asp_asapp_augmentation_analytics - 1";
INSERT INTO ${env:DB_TEMP}.asp_asapp_augmentation_analytics partition(partition_date, partition_hour)
SELECT
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
        partition_date,
        partition_hour
   FROM ${env:DB_NIFI}.asp_asapp_augmentation_analytics
  WHERE partition_date = DATE_SUB(TO_DATE('${env:RUN_DATE}'), 1)
;


SELECT "Truncating temp asp_asapp_convos_intents - 2";
MSCK REPAIR TABLE ${env:DB_NIFI}.asp_asapp_convos_intents;
TRUNCATE TABLE ${env:DB_TEMP}.asp_asapp_convos_intents;
TRUNCATE TABLE ${env:DB_TEMP}.asp_asapp_convos_intents_new;

SELECT "Inserting temp asp_asapp_convos_intents - 2";
INSERT INTO ${env:DB_TEMP}.asp_asapp_convos_intents partition(partition_date, partition_hour)
SELECT
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
        partition_date,
        partition_hour
   FROM ${env:DB_NIFI}.asp_asapp_convos_intents
  WHERE partition_date = DATE_SUB(TO_DATE('${env:RUN_DATE}'), 1)
;


SELECT "Truncating temp asp_asapp_convos_intents_ended - 3";
MSCK REPAIR TABLE ${env:DB_NIFI}.asp_asapp_convos_intents_ended;
TRUNCATE TABLE ${env:DB_TEMP}.asp_asapp_convos_intents_ended;
TRUNCATE TABLE ${env:DB_TEMP}.asp_asapp_convos_intents_ended_new;

SELECT "Inserting temp asp_asapp_convos_intents_ended - 3";
INSERT INTO ${env:DB_TEMP}.asp_asapp_convos_intents_ended partition(partition_date, partition_hour)
SELECT
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
        partition_date,
        partition_hour
   FROM ${env:DB_NIFI}.asp_asapp_convos_intents_ended
  WHERE partition_date = DATE_SUB(TO_DATE('${env:RUN_DATE}'), 1)
;


SELECT "Truncating temp asp_asapp_convos_metadata - 4";
MSCK REPAIR TABLE ${env:DB_NIFI}.asp_asapp_convos_metadata;
TRUNCATE TABLE ${env:DB_TEMP}.asp_asapp_convos_metadata;
TRUNCATE TABLE ${env:DB_TEMP}.asp_asapp_convos_metadata_new;

SELECT "Inserting temp asp_asapp_convos_metadata - 4";
INSERT INTO ${env:DB_TEMP}.asp_asapp_convos_metadata partition(partition_date, partition_hour)
SELECT
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
        partition_date,
        partition_hour
   FROM ${env:DB_NIFI}.asp_asapp_convos_metadata
  WHERE partition_date = DATE_SUB(TO_DATE('${env:RUN_DATE}'), 1)
;


SELECT "Truncating temp asp_asapp_convos_metadata_ended - 5";
MSCK REPAIR TABLE ${env:DB_NIFI}.asp_asapp_convos_metadata_ended;
TRUNCATE TABLE ${env:DB_TEMP}.asp_asapp_convos_metadata_ended;
TRUNCATE TABLE ${env:DB_TEMP}.asp_asapp_convos_metadata_ended_new;

SELECT "Inserting temp asp_asapp_convos_metadata_ended - 5";
INSERT INTO ${env:DB_TEMP}.asp_asapp_convos_metadata_ended partition(partition_date, partition_hour)
SELECT
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
        partition_date,
        partition_hour
   FROM ${env:DB_NIFI}.asp_asapp_convos_metadata_ended
  WHERE partition_date = DATE_SUB(TO_DATE('${env:RUN_DATE}'), 1)
;


SELECT "Truncating temp asp_asapp_convos_metrics - 6";
MSCK REPAIR TABLE ${env:DB_NIFI}.asp_asapp_convos_metrics;
TRUNCATE TABLE ${env:DB_TEMP}.asp_asapp_convos_metrics;
TRUNCATE TABLE ${env:DB_TEMP}.asp_asapp_convos_metrics_new;

SELECT "Inserting temp asp_asapp_convos_metrics - 6";
INSERT INTO ${env:DB_TEMP}.asp_asapp_convos_metrics partition(partition_date, partition_hour)
SELECT
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
        partition_date,
        partition_hour
   FROM ${env:DB_NIFI}.asp_asapp_convos_metrics
  WHERE partition_date = DATE_SUB(TO_DATE('${env:RUN_DATE}'), 1)
;


SELECT "Truncating temp asp_asapp_convos_metrics_ended - 7";
MSCK REPAIR TABLE ${env:DB_NIFI}.asp_asapp_convos_metrics_ended;
TRUNCATE TABLE ${env:DB_TEMP}.asp_asapp_convos_metrics_ended;
TRUNCATE TABLE ${env:DB_TEMP}.asp_asapp_convos_metrics_ended_new;

SELECT "Inserting temp asp_asapp_convos_metrics_ended - 7";
INSERT INTO ${env:DB_TEMP}.asp_asapp_convos_metrics_ended partition(partition_date, partition_hour)
SELECT
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
        partition_date,
        partition_hour
   FROM ${env:DB_NIFI}.asp_asapp_convos_metrics_ended
  WHERE partition_date = DATE_SUB(TO_DATE('${env:RUN_DATE}'), 1)
;


SELECT "Truncating temp asp_asapp_csid_containment - 8";
MSCK REPAIR TABLE ${env:DB_NIFI}.asp_asapp_csid_containment;
TRUNCATE TABLE ${env:DB_TEMP}.asp_asapp_csid_containment;
TRUNCATE TABLE ${env:DB_TEMP}.asp_asapp_csid_containment_new;

SELECT "Inserting temp asp_asapp_csid_containment - 8";
INSERT INTO ${env:DB_TEMP}.asp_asapp_csid_containment partition(partition_date, partition_hour)
SELECT
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
        partition_date,
        partition_hour
   FROM ${env:DB_NIFI}.asp_asapp_csid_containment
  WHERE partition_date = DATE_SUB(TO_DATE('${env:RUN_DATE}'), 1)
;


SELECT "Truncating temp asp_asapp_customer_params - 9";
MSCK REPAIR TABLE ${env:DB_NIFI}.asp_asapp_customer_params;
TRUNCATE TABLE ${env:DB_TEMP}.asp_asapp_customer_params;
TRUNCATE TABLE ${env:DB_TEMP}.asp_asapp_customer_params_new;

SELECT "Inserting temp asp_asapp_customer_params - 9";
INSERT INTO ${env:DB_TEMP}.asp_asapp_customer_params partition(partition_date, partition_hour)
SELECT
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
        partition_date,
        partition_hour
   FROM ${env:DB_NIFI}.asp_asapp_customer_params
  WHERE partition_date = DATE_SUB(TO_DATE('${env:RUN_DATE}'), 1)
;


SELECT "Truncating temp asp_asapp_flow_completions - 10";
MSCK REPAIR TABLE ${env:DB_NIFI}.asp_asapp_flow_completions;
TRUNCATE TABLE ${env:DB_TEMP}.asp_asapp_flow_completions;
TRUNCATE TABLE ${env:DB_TEMP}.asp_asapp_flow_completions_new;

SELECT "Inserting temp asp_asapp_flow_completions - 10";
INSERT INTO ${env:DB_TEMP}.asp_asapp_flow_completions partition(partition_date, partition_hour)
SELECT
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
        partition_date,
        partition_hour
   FROM ${env:DB_NIFI}.asp_asapp_flow_completions
  WHERE partition_date = DATE_SUB(TO_DATE('${env:RUN_DATE}'), 1)
;


SELECT "Truncating temp asp_asapp_rep_augmentation - 11";
MSCK REPAIR TABLE ${env:DB_NIFI}.asp_asapp_rep_augmentation;
TRUNCATE TABLE ${env:DB_TEMP}.asp_asapp_rep_augmentation;
TRUNCATE TABLE ${env:DB_TEMP}.asp_asapp_rep_augmentation_new;

SELECT "Inserting temp asp_asapp_rep_augmentation - 11";
INSERT INTO ${env:DB_TEMP}.asp_asapp_rep_augmentation partition(partition_date, partition_hour)
SELECT
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
        partition_date,
        partition_hour
   FROM ${env:DB_NIFI}.asp_asapp_rep_augmentation
  WHERE partition_date = DATE_SUB(TO_DATE('${env:RUN_DATE}'), 1)
;


SELECT "Truncating temp asp_asapp_sdk_events - 12";
MSCK REPAIR TABLE ${env:DB_NIFI}.asp_asapp_sdk_events;
TRUNCATE TABLE ${env:DB_TEMP}.asp_asapp_sdk_events;
TRUNCATE TABLE ${env:DB_TEMP}.asp_asapp_sdk_events_new;

SELECT "Inserting temp asp_asapp_sdk_events - 12";
INSERT INTO ${env:DB_TEMP}.asp_asapp_sdk_events partition(partition_date, partition_hour)
SELECT
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
        partition_date,
        partition_hour
   FROM ${env:DB_NIFI}.asp_asapp_sdk_events
  WHERE partition_date = DATE_SUB(TO_DATE('${env:RUN_DATE}'), 1)
;


SELECT "Truncating temp asp_asapp_utterances - 13";
MSCK REPAIR TABLE ${env:DB_NIFI}.asp_asapp_utterances;
TRUNCATE TABLE ${env:DB_TEMP}.asp_asapp_utterances;
TRUNCATE TABLE ${env:DB_TEMP}.asp_asapp_utterances_new;

SELECT "Inserting temp asp_asapp_utterances - 13";
INSERT INTO ${env:DB_TEMP}.asp_asapp_utterances partition(partition_date, partition_hour)
SELECT
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
        partition_date,
        partition_hour
   FROM ${env:DB_NIFI}.asp_asapp_utterances
  WHERE partition_date = DATE_SUB(TO_DATE('${env:RUN_DATE}'), 1)
;
