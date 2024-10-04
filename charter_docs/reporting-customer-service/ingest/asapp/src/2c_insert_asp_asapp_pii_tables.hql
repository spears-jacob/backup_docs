USE ${env:ENVIRONMENT};


SELECT "Inserting Data For asp_asapp_augmentation_analytics - 1";
drop table if exists tmp_asapp_augmentation_analytics;
CREATE TEMPORARY table tmp_asapp_augmentation_analytics as
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
        partition_hour,
        regexp_replace(event_create_ts,'T.*','') as event_create_date,
        regexp_replace(regexp_replace(event_create_ts,'^.*T',''),':.*','') as event_create_hour,
        MAX(regexp_replace(event_create_ts,'T.*','')) OVER (PARTITION BY partition_date, partition_hour) AS hourly_max_event_create_date,
        MAX(regexp_replace(event_create_ts,'T.*','')) OVER (PARTITION BY partition_date) AS daily_max_event_create_date,
        MAX(regexp_replace(regexp_replace(event_create_ts,'^.*T',''),':.*','')) OVER (PARTITION BY partition_date, partition_hour) AS hourly_max_event_create_hour,
        MAX(regexp_replace(regexp_replace(event_create_ts,'^.*T',''),':.*','')) OVER (PARTITION BY partition_date) AS daily_max_event_create_hour
   FROM ${env:DB_TEMP}.asp_asapp_augmentation_analytics_new
;

INSERT INTO TABLE asp_asapp_augmentation_analytics PARTITION (event_create_date,event_create_hour)
SELECT  DISTINCT
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
        partition_hour,
        CASE WHEN event_create_date is not null THEN event_create_date
             WHEN (event_create_date is null and hourly_max_event_create_date is not null) then hourly_max_event_create_date
             WHEN (event_create_date is null and hourly_max_event_create_date is null and daily_max_event_create_date is not null) then daily_max_event_create_date
             ELSE partition_date
        END AS event_create_date,
        CASE WHEN event_create_hour is not null THEN event_create_hour
             WHEN (event_create_hour is null and hourly_max_event_create_hour is not null) then hourly_max_event_create_hour
             WHEN (event_create_hour is null and hourly_max_event_create_hour is null and daily_max_event_create_hour is not null) then daily_max_event_create_hour
             ELSE partition_hour
        END AS event_create_hour
   FROM tmp_asapp_augmentation_analytics
;


SELECT "Inserting Data For asp_asapp_convos_intents - 2";
DROP TABLE IF EXISTS tmp_asapp_convos_intents;
CREATE TEMPORARY TABLE tmp_asapp_convos_intents AS
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
        partition_hour,
        regexp_replace(first_utterance_ts,'T.*','') as first_utterance_date,
        regexp_replace(regexp_replace(first_utterance_ts,'^.*T',''),':.*','') as first_utterance_hour,
        MAX(regexp_replace(first_utterance_ts,'T.*','')) OVER (PARTITION BY partition_date, partition_hour) AS hourly_max_first_utterance_date,
        MAX(regexp_replace(first_utterance_ts,'T.*','')) OVER (PARTITION BY partition_date) AS daily_max_first_utterance_date,
        MAX(regexp_replace(regexp_replace(first_utterance_ts,'^.*T',''),':.*','')) OVER (PARTITION BY partition_date, partition_hour) AS hourly_max_first_utterance_hour,
        MAX(regexp_replace(regexp_replace(first_utterance_ts,'^.*T',''),':.*','')) OVER (PARTITION BY partition_date) AS daily_max_first_utterance_hour
   FROM ${env:DB_TEMP}.asp_asapp_convos_intents_new
;

INSERT INTO TABLE asp_asapp_convos_intents PARTITION (first_utterance_date,first_utterance_hour)
SELECT  DISTINCT
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
        partition_hour,
        CASE WHEN first_utterance_date is not null THEN first_utterance_date
             WHEN (first_utterance_date is null and hourly_max_first_utterance_date is not null) then hourly_max_first_utterance_date
             WHEN (first_utterance_date is null and hourly_max_first_utterance_date is null and daily_max_first_utterance_date is not null) then daily_max_first_utterance_date
             ELSE partition_date
        END AS first_utterance_date,
        CASE WHEN first_utterance_hour is not null THEN first_utterance_hour
             WHEN (first_utterance_hour is null and hourly_max_first_utterance_hour is not null) then hourly_max_first_utterance_hour
             WHEN (first_utterance_hour is null and hourly_max_first_utterance_hour is null and daily_max_first_utterance_hour is not null) then daily_max_first_utterance_hour
             ELSE partition_hour
        END AS first_utterance_hour
   FROM tmp_asapp_convos_intents
;


SELECT "Inserting Data For asp_asapp_convos_intents_ended - 3";
DROP TABLE IF EXISTS tmp_asapp_convos_intents_ended;
CREATE TEMPORARY TABLE tmp_asapp_convos_intents_ended AS
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
        partition_hour,
        regexp_replace(first_utterance_ts,'T.*','') as first_utterance_date,
        regexp_replace(regexp_replace(first_utterance_ts,'^.*T',''),':.*','') as first_utterance_hour,
        MAX(regexp_replace(first_utterance_ts,'T.*','')) OVER (PARTITION BY partition_date, partition_hour) AS hourly_max_first_utterance_date,
        MAX(regexp_replace(first_utterance_ts,'T.*','')) OVER (PARTITION BY partition_date) AS daily_max_first_utterance_date,
        MAX(regexp_replace(regexp_replace(first_utterance_ts,'^.*T',''),':.*','')) OVER (PARTITION BY partition_date, partition_hour) AS hourly_max_first_utterance_hour,
        MAX(regexp_replace(regexp_replace(first_utterance_ts,'^.*T',''),':.*','')) OVER (PARTITION BY partition_date) AS daily_max_first_utterance_hour
   FROM ${env:DB_TEMP}.asp_asapp_convos_intents_ended_new
;

INSERT INTO TABLE asp_asapp_convos_intents_ended PARTITION (first_utterance_date,first_utterance_hour)
SELECT  DISTINCT
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
        partition_hour,
        CASE WHEN first_utterance_date is not null THEN first_utterance_date
             WHEN (first_utterance_date is null and hourly_max_first_utterance_date is not null) then hourly_max_first_utterance_date
             WHEN (first_utterance_date is null and hourly_max_first_utterance_date is null and daily_max_first_utterance_date is not null) then daily_max_first_utterance_date
             ELSE partition_date
        END AS first_utterance_date,
        CASE WHEN first_utterance_hour is not null THEN first_utterance_hour
             WHEN (first_utterance_hour is null and hourly_max_first_utterance_hour is not null) then hourly_max_first_utterance_hour
             WHEN (first_utterance_hour is null and hourly_max_first_utterance_hour is null and daily_max_first_utterance_hour is not null) then daily_max_first_utterance_hour
             ELSE partition_hour
        END AS first_utterance_hour
   FROM tmp_asapp_convos_intents_ended
;


SELECT "Inserting Data For asp_asapp_convos_metadata- 4";
DROP TABLE IF EXISTS tmp_asapp_convos_metadata;
CREATE TEMPORARY TABLE tmp_asapp_convos_metadata AS
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
        partition_hour,
        regexp_replace(last_event_ts,'T.*','') as last_event_date,
        regexp_replace(regexp_replace(last_event_ts,'^.*T',''),':.*','') as last_event_hour,
        MAX(regexp_replace(last_event_ts,'T.*','')) OVER (PARTITION BY partition_date, partition_hour) AS hourly_max_last_event_date,
        MAX(regexp_replace(last_event_ts,'T.*','')) OVER (PARTITION BY partition_date) AS daily_max_last_event_date,
        MAX(regexp_replace(regexp_replace(last_event_ts,'^.*T',''),':.*','')) OVER (PARTITION BY partition_date, partition_hour) AS hourly_max_last_event_hour,
        MAX(regexp_replace(regexp_replace(last_event_ts,'^.*T',''),':.*','')) OVER (PARTITION BY partition_date) AS daily_max_last_event_hour
   FROM ${env:DB_TEMP}.asp_asapp_convos_metadata_new
;

INSERT INTO TABLE asp_asapp_convos_metadata PARTITION (last_event_date,last_event_hour)
SELECT  DISTINCT
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
        partition_hour,
        CASE WHEN last_event_date is not null THEN last_event_date
             WHEN (last_event_date is null and hourly_max_last_event_date is not null) then hourly_max_last_event_date
             WHEN (last_event_date is null and hourly_max_last_event_date is null and daily_max_last_event_date is not null) then daily_max_last_event_date
             ELSE partition_date
        END AS last_event_date,
        CASE WHEN last_event_hour is not null THEN last_event_hour
             WHEN (last_event_hour is null and hourly_max_last_event_hour is not null) then hourly_max_last_event_hour
             WHEN (last_event_hour is null and hourly_max_last_event_hour is null and daily_max_last_event_hour is not null) then daily_max_last_event_hour
             ELSE partition_hour
        END AS last_event_hour
   FROM tmp_asapp_convos_metadata
;


SELECT "Inserting Data For asp_asapp_convos_metadata_ended- 5";
DROP TABLE IF EXISTS tmp_asapp_convos_metadata_ended;
CREATE TEMPORARY TABLE tmp_asapp_convos_metadata_ended AS
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
        partition_hour,
        regexp_replace(last_event_ts,'T.*','') as last_event_date,
        regexp_replace(regexp_replace(last_event_ts,'^.*T',''),':.*','') as last_event_hour,
        MAX(regexp_replace(last_event_ts,'T.*','')) OVER (PARTITION BY partition_date, partition_hour) AS hourly_max_last_event_date,
        MAX(regexp_replace(last_event_ts,'T.*','')) OVER (PARTITION BY partition_date) AS daily_max_last_event_date,
        MAX(regexp_replace(regexp_replace(last_event_ts,'^.*T',''),':.*','')) OVER (PARTITION BY partition_date, partition_hour) AS hourly_max_last_event_hour,
        MAX(regexp_replace(regexp_replace(last_event_ts,'^.*T',''),':.*','')) OVER (PARTITION BY partition_date) AS daily_max_last_event_hour
   FROM ${env:DB_TEMP}.asp_asapp_convos_metadata_ended_new
;

INSERT INTO TABLE asp_asapp_convos_metadata_ended PARTITION (last_event_date,last_event_hour)
SELECT  DISTINCT
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
        partition_hour,
        CASE WHEN last_event_date is not null THEN last_event_date
             WHEN (last_event_date is null and hourly_max_last_event_date is not null) then hourly_max_last_event_date
             WHEN (last_event_date is null and hourly_max_last_event_date is null and daily_max_last_event_date is not null) then daily_max_last_event_date
             ELSE partition_date
        END AS last_event_date,
        CASE WHEN last_event_hour is not null THEN last_event_hour
             WHEN (last_event_hour is null and hourly_max_last_event_hour is not null) then hourly_max_last_event_hour
             WHEN (last_event_hour is null and hourly_max_last_event_hour is null and daily_max_last_event_hour is not null) then daily_max_last_event_hour
             ELSE partition_hour
        END AS last_event_hour
   FROM tmp_asapp_convos_metadata_ended
;


SELECT "Inserting Data For asp_asapp_convos_metrics- 6";
DROP TABLE IF EXISTS tmp_asapp_convos_metrics;
CREATE TEMPORARY TABLE tmp_asapp_convos_metrics AS
SELECT  customer_response_count,
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
        partition_hour,
        regexp_replace(first_utterance_ts,'T.*','') as first_utterance_date,
        regexp_replace(regexp_replace(first_utterance_ts,'^.*T',''),':.*','') as first_utterance_hour,
        MAX(regexp_replace(first_utterance_ts,'T.*','')) OVER (PARTITION BY partition_date, partition_hour) AS hourly_max_first_utterance_date,
        MAX(regexp_replace(first_utterance_ts,'T.*','')) OVER (PARTITION BY partition_date) AS daily_max_first_utterance_date,
        MAX(regexp_replace(regexp_replace(first_utterance_ts,'^.*T',''),':.*','')) OVER (PARTITION BY partition_date, partition_hour) AS hourly_max_first_utterance_hour,
        MAX(regexp_replace(regexp_replace(first_utterance_ts,'^.*T',''),':.*','')) OVER (PARTITION BY partition_date) AS daily_max_first_utterance_hour
   FROM ${env:DB_TEMP}.asp_asapp_convos_metrics_new
;

INSERT INTO TABLE asp_asapp_convos_metrics PARTITION (first_utterance_date,first_utterance_hour)
SELECT  DISTINCT
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
        partition_hour,
        CASE WHEN first_utterance_date is not null THEN first_utterance_date
             WHEN (first_utterance_date is null and hourly_max_first_utterance_date is not null) then hourly_max_first_utterance_date
             WHEN (first_utterance_date is null and hourly_max_first_utterance_date is null and daily_max_first_utterance_date is not null) then daily_max_first_utterance_date
             ELSE partition_date
        END AS first_utterance_date,
        CASE WHEN first_utterance_hour is not null THEN first_utterance_hour
             WHEN (first_utterance_hour is null and hourly_max_first_utterance_hour is not null) then hourly_max_first_utterance_hour
             WHEN (first_utterance_hour is null and hourly_max_first_utterance_hour is null and daily_max_first_utterance_hour is not null) then daily_max_first_utterance_hour
             ELSE partition_hour
        END AS first_utterance_hour
   FROM tmp_asapp_convos_metrics
;


SELECT "Inserting Data For asp_asapp_convos_metrics_ended- 7";
DROP TABLE IF EXISTS tmp_asapp_convos_metrics_ended;
CREATE TEMPORARY TABLE tmp_asapp_convos_metrics_ended AS
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
        partition_hour,
        regexp_replace(first_utterance_ts,'T.*','') as first_utterance_date,
        regexp_replace(regexp_replace(first_utterance_ts,'^.*T',''),':.*','') as first_utterance_hour,
        MAX(regexp_replace(first_utterance_ts,'T.*','')) OVER (PARTITION BY partition_date, partition_hour) AS hourly_max_first_utterance_date,
        MAX(regexp_replace(first_utterance_ts,'T.*','')) OVER (PARTITION BY partition_date) AS daily_max_first_utterance_date,
        MAX(regexp_replace(regexp_replace(first_utterance_ts,'^.*T',''),':.*','')) OVER (PARTITION BY partition_date, partition_hour) AS hourly_max_first_utterance_hour,
        MAX(regexp_replace(regexp_replace(first_utterance_ts,'^.*T',''),':.*','')) OVER (PARTITION BY partition_date) AS daily_max_first_utterance_hour
   FROM ${env:DB_TEMP}.asp_asapp_convos_metrics_ended_new
;

INSERT INTO TABLE asp_asapp_convos_metrics_ended PARTITION (first_utterance_date,first_utterance_hour)
SELECT  DISTINCT
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
        partition_hour,
        CASE WHEN first_utterance_date is not null THEN first_utterance_date
             WHEN (first_utterance_date is null and hourly_max_first_utterance_date is not null) then hourly_max_first_utterance_date
             WHEN (first_utterance_date is null and hourly_max_first_utterance_date is null and daily_max_first_utterance_date is not null) then daily_max_first_utterance_date
             ELSE partition_date
        END AS first_utterance_date,
        CASE WHEN first_utterance_hour is not null THEN first_utterance_hour
             WHEN (first_utterance_hour is null and hourly_max_first_utterance_hour is not null) then hourly_max_first_utterance_hour
             WHEN (first_utterance_hour is null and hourly_max_first_utterance_hour is null and daily_max_first_utterance_hour is not null) then daily_max_first_utterance_hour
             ELSE partition_hour
        END AS first_utterance_hour
   FROM tmp_asapp_convos_metrics_ended
;


SELECT "Inserting Data For asp_asapp_csid_containment - 8";
DROP TABLE IF EXISTS tmp_asapp_csid_containment;
CREATE TEMPORARY TABLE tmp_asapp_csid_containment AS
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
        partition_hour,
        regexp_replace(instance_ts,'T.*','') as instance_date,
        regexp_replace(regexp_replace(instance_ts,'^.*T',''),':.*','') as instance_hour,
        MAX(regexp_replace(instance_ts,'T.*','')) OVER (PARTITION BY partition_date, partition_hour) AS hourly_max_instance_date,
        MAX(regexp_replace(instance_ts,'T.*','')) OVER (PARTITION BY partition_date) AS daily_max_instance_date,
        MAX(regexp_replace(regexp_replace(instance_ts,'^.*T',''),':.*','')) OVER (PARTITION BY partition_date, partition_hour) AS hourly_max_instance_hour,
        MAX(regexp_replace(regexp_replace(instance_ts,'^.*T',''),':.*','')) OVER (PARTITION BY partition_date) AS daily_max_instance_hour
   FROM ${env:DB_TEMP}.asp_asapp_csid_containment_new
;

INSERT INTO TABLE asp_asapp_csid_containment PARTITION (instance_date,instance_hour)
SELECT  DISTINCT
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
        partition_hour,
        CASE WHEN instance_date is not null THEN instance_date
             WHEN (instance_date is null and hourly_max_instance_date is not null) then hourly_max_instance_date
             WHEN (instance_date is null and hourly_max_instance_date is null and daily_max_instance_date is not null) then daily_max_instance_date
             ELSE partition_date
        END AS instance_date,
        CASE WHEN instance_hour is not null THEN instance_hour
             WHEN (instance_hour is null and hourly_max_instance_hour is not null) then hourly_max_instance_hour
             WHEN (instance_hour is null and hourly_max_instance_hour is null and daily_max_instance_hour is not null) then daily_max_instance_hour
             ELSE partition_hour
        END AS instance_hour
   FROM tmp_asapp_csid_containment
;


SELECT "Inserting Data For asp_asapp_customer_params - 9";
DROP TABLE IF EXISTS tmp_asapp_customer_params;
CREATE TEMPORARY TABLE tmp_asapp_customer_params AS
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
        partition_hour,
        regexp_replace(instance_ts,'T.*','') as instance_date,
        regexp_replace(regexp_replace(instance_ts,'^.*T',''),':.*','') as instance_hour,
        MAX(regexp_replace(instance_ts,'T.*','')) OVER (PARTITION BY partition_date, partition_hour) AS hourly_max_instance_date,
        MAX(regexp_replace(instance_ts,'T.*','')) OVER (PARTITION BY partition_date) AS daily_max_instance_date,
        MAX(regexp_replace(regexp_replace(instance_ts,'^.*T',''),':.*','')) OVER (PARTITION BY partition_date, partition_hour) AS hourly_max_instance_hour,
        MAX(regexp_replace(regexp_replace(instance_ts,'^.*T',''),':.*','')) OVER (PARTITION BY partition_date) AS daily_max_instance_hour
   FROM ${env:DB_TEMP}.asp_asapp_customer_params_new
;

INSERT INTO TABLE asp_asapp_customer_params PARTITION (instance_date,instance_hour)
SELECT  DISTINCT
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
        partition_hour,
        CASE WHEN instance_date is not null THEN instance_date
             WHEN (instance_date is null and hourly_max_instance_date is not null) then hourly_max_instance_date
             WHEN (instance_date is null and hourly_max_instance_date is null and daily_max_instance_date is not null) then daily_max_instance_date
             ELSE partition_date
        END AS instance_date,
        CASE WHEN instance_hour is not null THEN instance_hour
             WHEN (instance_hour is null and hourly_max_instance_hour is not null) then hourly_max_instance_hour
             WHEN (instance_hour is null and hourly_max_instance_hour is null and daily_max_instance_hour is not null) then daily_max_instance_hour
             ELSE partition_hour
        END AS instance_hour
   FROM tmp_asapp_customer_params
;


SELECT "Inserting Data For asp_asapp_flow_completions - 10";
DROP TABLE IF EXISTS tmp_asapp_flow_completions;
CREATE TEMPORARY TABLE tmp_asapp_flow_completions AS
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
        partition_hour,
        regexp_replace(success_event_ts,'T.*','') as success_event_date,
        regexp_replace(regexp_replace(success_event_ts,'^.*T',''),':.*','') as success_event_hour,
        MAX(regexp_replace(success_event_ts,'T.*','')) OVER (PARTITION BY partition_date, partition_hour) AS hourly_max_success_event_date,
        MAX(regexp_replace(success_event_ts,'T.*','')) OVER (PARTITION BY partition_date) AS daily_max_success_event_date,
        MAX(regexp_replace(regexp_replace(success_event_ts,'^.*T',''),':.*','')) OVER (PARTITION BY partition_date, partition_hour) AS hourly_max_success_event_hour,
        MAX(regexp_replace(regexp_replace(success_event_ts,'^.*T',''),':.*','')) OVER (PARTITION BY partition_date) AS daily_max_success_event_hour
   FROM ${env:DB_TEMP}.asp_asapp_flow_completions_new
;

INSERT INTO TABLE asp_asapp_flow_completions PARTITION (success_event_date,success_event_hour)
SELECT  DISTINCT
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
        partition_hour,
        CASE WHEN success_event_date is not null THEN success_event_date
             WHEN (success_event_date is null and hourly_max_success_event_date is not null) then hourly_max_success_event_date
             WHEN (success_event_date is null and hourly_max_success_event_date is null and daily_max_success_event_date is not null) then daily_max_success_event_date
             ELSE partition_date
        END AS success_event_date,
        CASE WHEN success_event_hour is not null THEN success_event_hour
             WHEN (success_event_hour is null and hourly_max_success_event_hour is not null) then hourly_max_success_event_hour
             WHEN (success_event_hour is null and hourly_max_success_event_hour is null and daily_max_success_event_hour is not null) then daily_max_success_event_hour
             ELSE partition_hour
        END AS success_event_hour
   FROM tmp_asapp_flow_completions
;


SELECT "Inserting Data For asp_asapp_rep_augmentation - 11";
DROP TABLE IF EXISTS tmp_asapp_rep_augmentation;
CREATE TEMPORARY TABLE tmp_asapp_rep_augmentation AS
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
        partition_hour,
        regexp_replace(instance_ts,'T.*','') as instance_date,
        regexp_replace(regexp_replace(instance_ts,'^.*T',''),':.*','') as instance_hour,
        MAX(regexp_replace(instance_ts,'T.*','')) OVER (PARTITION BY partition_date, partition_hour) AS hourly_max_instance_date,
        MAX(regexp_replace(instance_ts,'T.*','')) OVER (PARTITION BY partition_date) AS daily_max_instance_date,
        MAX(regexp_replace(regexp_replace(instance_ts,'^.*T',''),':.*','')) OVER (PARTITION BY partition_date, partition_hour) AS hourly_max_instance_hour,
        MAX(regexp_replace(regexp_replace(instance_ts,'^.*T',''),':.*','')) OVER (PARTITION BY partition_date) AS daily_max_instance_hour
   FROM ${env:DB_TEMP}.asp_asapp_rep_augmentation_new
;

INSERT INTO TABLE asp_asapp_rep_augmentation PARTITION (instance_date,instance_hour)
SELECT  DISTINCT
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
        partition_hour,
        CASE WHEN instance_date is not null THEN instance_date
             WHEN (instance_date is null and hourly_max_instance_date is not null) then hourly_max_instance_date
             WHEN (instance_date is null and hourly_max_instance_date is null and daily_max_instance_date is not null) then daily_max_instance_date
             ELSE partition_date
        END AS instance_date,
        CASE WHEN instance_hour is not null THEN instance_hour
             WHEN (instance_hour is null and hourly_max_instance_hour is not null) then hourly_max_instance_hour
             WHEN (instance_hour is null and hourly_max_instance_hour is null and daily_max_instance_hour is not null) then daily_max_instance_hour
             ELSE partition_hour
        END AS instance_hour
   FROM tmp_asapp_rep_augmentation
;


SELECT "Inserting Data For asp_asapp_sdk_events - 12";
DROP TABLE IF EXISTS tmp_asapp_sdk_events;
CREATE TEMPORARY TABLE tmp_asapp_sdk_events AS
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
        partition_hour,
        regexp_replace(instance_ts,'T.*','') as instance_date,
        regexp_replace(regexp_replace(instance_ts,'^.*T',''),':.*','') as instance_hour,
        MAX(regexp_replace(instance_ts,'T.*','')) OVER (PARTITION BY partition_date, partition_hour) AS hourly_max_instance_date,
        MAX(regexp_replace(instance_ts,'T.*','')) OVER (PARTITION BY partition_date) AS daily_max_instance_date,
        MAX(regexp_replace(regexp_replace(instance_ts,'^.*T',''),':.*','')) OVER (PARTITION BY partition_date, partition_hour) AS hourly_max_instance_hour,
        MAX(regexp_replace(regexp_replace(instance_ts,'^.*T',''),':.*','')) OVER (PARTITION BY partition_date) AS daily_max_instance_hour
   FROM ${env:DB_TEMP}.asp_asapp_sdk_events_new
;

INSERT INTO TABLE asp_asapp_sdk_events PARTITION (instance_date,instance_hour)
SELECT  DISTINCT
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
        partition_hour,
        CASE WHEN instance_date is not null THEN instance_date
             WHEN (instance_date is null and hourly_max_instance_date is not null) then hourly_max_instance_date
             WHEN (instance_date is null and hourly_max_instance_date is null and daily_max_instance_date is not null) then daily_max_instance_date
             ELSE partition_date
        END AS instance_date,
        CASE WHEN instance_hour is not null THEN instance_hour
             WHEN (instance_hour is null and hourly_max_instance_hour is not null) then hourly_max_instance_hour
             WHEN (instance_hour is null and hourly_max_instance_hour is null and daily_max_instance_hour is not null) then daily_max_instance_hour
             ELSE partition_hour
        END AS instance_hour
   FROM tmp_asapp_sdk_events
;


SELECT "Inserting Data For asp_asapp_utterances - 13";
DROP TABLE IF EXISTS tmp_asapp_utterances;
CREATE TEMPORARY TABLE tmp_asapp_utterances AS
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
        partition_hour,
        regexp_replace(instance_ts,'T.*','') as instance_date,
        regexp_replace(regexp_replace(instance_ts,'^.*T',''),':.*','') as instance_hour,
        MAX(regexp_replace(instance_ts,'T.*','')) OVER (PARTITION BY partition_date, partition_hour) AS hourly_max_instance_date,
        MAX(regexp_replace(instance_ts,'T.*','')) OVER (PARTITION BY partition_date) AS daily_max_instance_date,
        MAX(regexp_replace(regexp_replace(instance_ts,'^.*T',''),':.*','')) OVER (PARTITION BY partition_date, partition_hour) AS hourly_max_instance_hour,
        MAX(regexp_replace(regexp_replace(instance_ts,'^.*T',''),':.*','')) OVER (PARTITION BY partition_date) AS daily_max_instance_hour
   FROM ${env:DB_TEMP}.asp_asapp_utterances_new
;

INSERT INTO TABLE asp_asapp_utterances PARTITION (instance_date,instance_hour)
SELECT  DISTINCT
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
        partition_hour,
        CASE WHEN instance_date is not null THEN instance_date
             WHEN (instance_date is null and hourly_max_instance_date is not null) then hourly_max_instance_date
             WHEN (instance_date is null and hourly_max_instance_date is null and daily_max_instance_date is not null) then daily_max_instance_date
             ELSE partition_date
        END AS instance_date,
        CASE WHEN instance_hour is not null THEN instance_hour
             WHEN (instance_hour is null and hourly_max_instance_hour is not null) then hourly_max_instance_hour
             WHEN (instance_hour is null and hourly_max_instance_hour is null and daily_max_instance_hour is not null) then daily_max_instance_hour
             ELSE partition_hour
        END AS instance_hour
   FROM tmp_asapp_utterances
;
