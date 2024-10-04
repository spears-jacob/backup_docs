USE ${env:ENVIRONMENT};

add jar hdfs:///udf/json-serde-1.3.9-SNAPSHOT-jar-with-dependencies.jar;


SELECT "Inserting Data For asp_asapp_customer_feedback - 1";
MSCK REPAIR TABLE ${env:DB_NIFI}.asp_asapp_customer_feedback;

DROP TABLE IF EXISTS tmp_asapp_customer_feedback;
CREATE TEMPORARY TABLE tmp_asapp_customer_feedback AS
SELECT  question,
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
        partition_date,
        partition_hour,
        regexp_replace(instance_ts,'T.*','') as instance_date,
        regexp_replace(regexp_replace(instance_ts,'^.*T',''),':.*','') as instance_hour,
        MAX(regexp_replace(instance_ts,'T.*','')) OVER (PARTITION BY partition_date, partition_hour) AS hourly_max_instance_date,
        MAX(regexp_replace(instance_ts,'T.*','')) OVER (PARTITION BY partition_date) AS daily_max_instance_date,
        MAX(regexp_replace(regexp_replace(instance_ts,'^.*T',''),':.*','')) OVER (PARTITION BY partition_date, partition_hour) AS hourly_max_instance_hour,
        MAX(regexp_replace(regexp_replace(instance_ts,'^.*T',''),':.*','')) OVER (PARTITION BY partition_date) AS daily_max_instance_hour
  FROM ${env:DB_NIFI}.asp_asapp_customer_feedback
 WHERE partition_date = DATE_SUB(TO_DATE('${env:RUN_DATE}'), 1)
 ;

INSERT INTO TABLE asp_asapp_customer_feedback PARTITION (instance_date,instance_hour)
SELECT  DISTINCT
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
   FROM tmp_asapp_customer_feedback
;


SELECT "Inserting Data For asp_asapp_export_row_counts - 2";
MSCK REPAIR TABLE ${env:DB_NIFI}.asp_asapp_export_row_counts;

DROP TABLE IF EXISTS tmp_asapp_export_row_counts;
CREATE TEMPORARY TABLE tmp_asapp_export_row_counts AS
SELECT
        export_name,
        company_name,
        export_interval,
        exported_rows,
        export_date,
        partition_date,
        partition_hour,
        regexp_replace(export_date,'T.*','') as partition_export_date,
        MAX(regexp_replace(export_date,'T.*','')) OVER (PARTITION BY partition_date, partition_hour) AS hourly_max_export_date,
        MAX(regexp_replace(export_date,'T.*','')) OVER (PARTITION BY partition_date) AS daily_max_export_date
   FROM ${env:DB_NIFI}.asp_asapp_export_row_counts
  WHERE partition_date = DATE_SUB(TO_DATE('${env:RUN_DATE}'), 1)
;

INSERT INTO TABLE asp_asapp_export_row_counts PARTITION (partition_export_date)
SELECT  DISTINCT
        export_name,
        company_name,
        export_interval,
        exported_rows,
        export_date,
        partition_date,
        partition_hour,
        CASE WHEN partition_export_date is not null THEN partition_export_date
             WHEN (partition_export_date is null and hourly_max_export_date is not null) then hourly_max_export_date
             WHEN (partition_export_date is null and hourly_max_export_date is null and daily_max_export_date is not null) then daily_max_export_date
             ELSE partition_date
        END AS partition_export_date
   FROM tmp_asapp_export_row_counts
;


SELECT "Inserting Data For asp_asapp_flow_detail - 3";
MSCK REPAIR TABLE ${env:DB_NIFI}.asp_asapp_flow_detail;

DROP TABLE IF EXISTS tmp_asapp_flow_detail;
CREATE TEMPORARY TABLE tmp_asapp_flow_detail AS
SELECT
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
        partition_date,
        partition_hour,
        regexp_replace(event_ts,'T.*','') as event_date,
        regexp_replace(regexp_replace(event_ts,'^.*T',''),':.*','') as event_hour,
        MAX(regexp_replace(event_ts,'T.*','')) OVER (PARTITION BY partition_date, partition_hour) AS hourly_max_event_date,
        MAX(regexp_replace(event_ts,'T.*','')) OVER (PARTITION BY partition_date) AS daily_max_event_date,
        MAX(regexp_replace(regexp_replace(event_ts,'^.*T',''),':.*','')) OVER (PARTITION BY partition_date, partition_hour) AS hourly_max_event_hour,
        MAX(regexp_replace(regexp_replace(event_ts,'^.*T',''),':.*','')) OVER (PARTITION BY partition_date) AS daily_max_event_hour
   FROM ${env:DB_NIFI}.asp_asapp_flow_detail
  WHERE partition_date = DATE_SUB(TO_DATE('${env:RUN_DATE}'), 1)
;

INSERT INTO TABLE asp_asapp_flow_detail PARTITION (event_date,event_hour)
SELECT  DISTINCT
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
        partition_date,
        partition_hour,
        CASE WHEN event_date is not null THEN event_date
             WHEN (event_date is null and hourly_max_event_date is not null) then hourly_max_event_date
             WHEN (event_date is null and hourly_max_event_date is null and daily_max_event_date is not null) then daily_max_event_date
             ELSE partition_date
        END AS event_date,
        CASE WHEN event_hour is not null THEN event_hour
             WHEN (event_hour is null and hourly_max_event_hour is not null) then hourly_max_event_hour
             WHEN (event_hour is null and hourly_max_event_hour is null and daily_max_event_hour is not null) then daily_max_event_hour
             ELSE partition_hour
        END AS event_hour
   FROM tmp_asapp_flow_detail
;


SELECT "Inserting Data For asp_asapp_intents - 4";
MSCK REPAIR TABLE ${env:DB_NIFI}.asp_asapp_intents;

INSERT INTO TABLE asp_asapp_intents PARTITION (partition_date,partition_hour)
SELECT  DISTINCT
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
   FROM ${env:DB_NIFI}.asp_asapp_intents
  WHERE partition_date = DATE_SUB(TO_DATE('${env:RUN_DATE}'), 1)
;


SELECT "Inserting Data For asp_asapp_issue_queues - 5";
MSCK REPAIR TABLE ${env:DB_NIFI}.asp_asapp_issue_queues;

DROP TABLE IF EXISTS tmp_asapp_issue_queues;
CREATE TEMPORARY TABLE tmp_asapp_issue_queues AS
SELECT
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
        partition_date,
        partition_hour,
        regexp_replace(instance_ts,'T.*','') as instance_date,
        regexp_replace(regexp_replace(instance_ts,'^.*T',''),':.*','') as instance_hour,
        MAX(regexp_replace(instance_ts,'T.*','')) OVER (PARTITION BY partition_date, partition_hour) AS hourly_max_instance_date,
        MAX(regexp_replace(instance_ts,'T.*','')) OVER (PARTITION BY partition_date) AS daily_max_instance_date,
        MAX(regexp_replace(regexp_replace(instance_ts,'^.*T',''),':.*','')) OVER (PARTITION BY partition_date, partition_hour) AS hourly_max_instance_hour,
        MAX(regexp_replace(regexp_replace(instance_ts,'^.*T',''),':.*','')) OVER (PARTITION BY partition_date) AS daily_max_instance_hour
   FROM ${env:DB_NIFI}.asp_asapp_issue_queues
  WHERE partition_date = DATE_SUB(TO_DATE('${env:RUN_DATE}'), 1)
;

INSERT INTO TABLE asp_asapp_issue_queues PARTITION (instance_date,instance_hour)
SELECT  DISTINCT
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
   FROM tmp_asapp_issue_queues
;


SELECT "Inserting Data For asp_asapp_rep_activity - 6";
MSCK REPAIR TABLE ${env:DB_NIFI}.asp_asapp_rep_activity;

DROP TABLE IF EXISTS tmp_asapp_rep_activity;
CREATE TEMPORARY TABLE tmp_asapp_rep_activity AS
SELECT
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
        partition_date,
        partition_hour,
        regexp_replace(instance_ts,'T.*','') as instance_date,
        regexp_replace(regexp_replace(instance_ts,'^.*T',''),':.*','') as instance_hour,
        MAX(regexp_replace(instance_ts,'T.*','')) OVER (PARTITION BY partition_date, partition_hour) AS hourly_max_instance_date,
        MAX(regexp_replace(instance_ts,'T.*','')) OVER (PARTITION BY partition_date) AS daily_max_instance_date,
        MAX(regexp_replace(regexp_replace(instance_ts,'^.*T',''),':.*','')) OVER (PARTITION BY partition_date, partition_hour) AS hourly_max_instance_hour,
        MAX(regexp_replace(regexp_replace(instance_ts,'^.*T',''),':.*','')) OVER (PARTITION BY partition_date) AS daily_max_instance_hour
   FROM ${env:DB_NIFI}.asp_asapp_rep_activity
  WHERE partition_date = DATE_SUB(TO_DATE('${env:RUN_DATE}'), 1)
;

INSERT INTO TABLE asp_asapp_rep_activity PARTITION (instance_date,instance_hour)
SELECT  DISTINCT
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
   FROM tmp_asapp_rep_activity
;


SELECT "Inserting Data For asp_asapp_rep_attributes - 7";
MSCK REPAIR TABLE ${env:DB_NIFI}.asp_asapp_rep_attributes;

DROP TABLE IF EXISTS tmp_asapp_rep_attributes;
CREATE TEMPORARY TABLE tmp_asapp_rep_attributes AS
SELECT
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
        partition_date,
        partition_hour,
        regexp_replace(created_ts,'T.*','') as created_date,
        regexp_replace(regexp_replace(created_ts,'^.*T',''),':.*','') as created_hour,
        MAX(regexp_replace(created_ts,'T.*','')) OVER (PARTITION BY partition_date, partition_hour) AS hourly_max_created_date,
        MAX(regexp_replace(created_ts,'T.*','')) OVER (PARTITION BY partition_date) AS daily_max_created_date,
        MAX(regexp_replace(regexp_replace(created_ts,'^.*T',''),':.*','')) OVER (PARTITION BY partition_date, partition_hour) AS hourly_max_created_hour,
        MAX(regexp_replace(regexp_replace(created_ts,'^.*T',''),':.*','')) OVER (PARTITION BY partition_date) AS daily_max_created_hour
   FROM ${env:DB_NIFI}.asp_asapp_rep_attributes
  WHERE partition_date = DATE_SUB(TO_DATE('${env:RUN_DATE}'), 1)
;

INSERT INTO TABLE asp_asapp_rep_attributes PARTITION (created_date,created_hour)
SELECT  DISTINCT
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
        partition_date,
        partition_hour,
        CASE WHEN created_date is not null THEN created_date
             WHEN (created_date is null and hourly_max_created_date is not null) then hourly_max_created_date
             WHEN (created_date is null and hourly_max_created_date is null and daily_max_created_date is not null) then daily_max_created_date
             ELSE partition_date
        END AS created_date,
        CASE WHEN created_hour is not null THEN created_hour
             WHEN (created_hour is null and hourly_max_created_hour is not null) then hourly_max_created_hour
             WHEN (created_hour is null and hourly_max_created_hour is null and daily_max_created_hour is not null) then daily_max_created_hour
             ELSE partition_hour
        END AS created_hour
   FROM tmp_asapp_rep_attributes
;


SELECT "Inserting Data For asp_asapp_rep_convos- 8";
MSCK REPAIR TABLE ${env:DB_NIFI}.asp_asapp_rep_convos;

DROP TABLE IF EXISTS tmp_asapp_rep_convos;
CREATE TEMPORARY TABLE tmp_asapp_rep_convos AS
SELECT
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
        partition_date,
        partition_hour,
        regexp_replace(instance_ts,'T.*','') as instance_date,
        regexp_replace(regexp_replace(instance_ts,'^.*T',''),':.*','') as instance_hour,
        MAX(regexp_replace(instance_ts,'T.*','')) OVER (PARTITION BY partition_date, partition_hour) AS hourly_max_instance_date,
        MAX(regexp_replace(instance_ts,'T.*','')) OVER (PARTITION BY partition_date) AS daily_max_instance_date,
        MAX(regexp_replace(regexp_replace(instance_ts,'^.*T',''),':.*','')) OVER (PARTITION BY partition_date, partition_hour) AS hourly_max_instance_hour,
        MAX(regexp_replace(regexp_replace(instance_ts,'^.*T',''),':.*','')) OVER (PARTITION BY partition_date) AS daily_max_instance_hour
   FROM ${env:DB_NIFI}.asp_asapp_rep_convos
  WHERE partition_date = DATE_SUB(TO_DATE('${env:RUN_DATE}'), 1)
;

INSERT INTO TABLE asp_asapp_rep_convos PARTITION (instance_date,instance_hour)
SELECT  DISTINCT
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
   FROM tmp_asapp_rep_convos
;


SELECT "Inserting Data For asp_asapp_rep_hierarchy - 9";
MSCK REPAIR TABLE ${env:DB_NIFI}.asp_asapp_rep_hierarchy;

INSERT INTO TABLE asp_asapp_rep_hierarchy PARTITION (partition_date, partition_hour)
SELECT  DISTINCT
        company_name,
        superior_rep_id,
        subordinate_agent_id,
        superior_agent_id,
        subordinate_rep_id,
        reporting_relationship,
        partition_date,
        partition_hour
   FROM ${env:DB_NIFI}.asp_asapp_rep_hierarchy
  WHERE partition_date = DATE_SUB(TO_DATE('${env:RUN_DATE}'), 1)
;


SELECT "Inserting Data For asp_asapp_rep_utilized - 10";
MSCK REPAIR TABLE ${env:DB_NIFI}.asp_asapp_rep_utilized;

DROP TABLE IF EXISTS tmp_asapp_rep_utilized;
CREATE TEMPORARY TABLE tmp_asapp_rep_utilized AS
SELECT
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
        partition_date,
        partition_hour,
        regexp_replace(instance_ts,'T.*','') as instance_date,
        regexp_replace(regexp_replace(instance_ts,'^.*T',''),':.*','') as instance_hour,
        MAX(regexp_replace(instance_ts,'T.*','')) OVER (PARTITION BY partition_date, partition_hour) AS hourly_max_instance_date,
        MAX(regexp_replace(instance_ts,'T.*','')) OVER (PARTITION BY partition_date) AS daily_max_instance_date,
        MAX(regexp_replace(regexp_replace(instance_ts,'^.*T',''),':.*','')) OVER (PARTITION BY partition_date, partition_hour) AS hourly_max_instance_hour,
        MAX(regexp_replace(regexp_replace(instance_ts,'^.*T',''),':.*','')) OVER (PARTITION BY partition_date) AS daily_max_instance_hour
   FROM ${env:DB_NIFI}.asp_asapp_rep_utilized
  WHERE partition_date = DATE_SUB(TO_DATE('${env:RUN_DATE}'), 1)
;

INSERT INTO TABLE asp_asapp_rep_utilized PARTITION (instance_date,instance_hour)
SELECT  DISTINCT
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
   FROM tmp_asapp_rep_utilized
;


SELECT "Inserting Data For asp_asapp_repmetrics - 11";
MSCK REPAIR TABLE ${env:DB_NIFI}.asp_asapp_repmetrics;

DROP TABLE IF EXISTS tmp_asapp_repmetrics;
CREATE TEMPORARY TABLE tmp_asapp_repmetrics AS
SELECT
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
        partition_date,
        partition_hour,
        regexp_replace(instance_ts,'T.*','') as instance_date,
        regexp_replace(regexp_replace(instance_ts,'^.*T',''),':.*','') as instance_hour,
        MAX(regexp_replace(instance_ts,'T.*','')) OVER (PARTITION BY partition_date, partition_hour) AS hourly_max_instance_date,
        MAX(regexp_replace(instance_ts,'T.*','')) OVER (PARTITION BY partition_date) AS daily_max_instance_date,
        MAX(regexp_replace(regexp_replace(instance_ts,'^.*T',''),':.*','')) OVER (PARTITION BY partition_date, partition_hour) AS hourly_max_instance_hour,
        MAX(regexp_replace(regexp_replace(instance_ts,'^.*T',''),':.*','')) OVER (PARTITION BY partition_date) AS daily_max_instance_hour
   FROM ${env:DB_NIFI}.asp_asapp_repmetrics
  WHERE partition_date = DATE_SUB(TO_DATE('${env:RUN_DATE}'), 1)
;

INSERT INTO TABLE asp_asapp_repmetrics PARTITION (instance_date,instance_hour)
SELECT  DISTINCT
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
   FROM tmp_asapp_repmetrics
;


SELECT "Inserting Data For asp_asapp_reps - 12";
MSCK REPAIR TABLE ${env:DB_NIFI}.asp_asapp_reps;

DROP TABLE IF EXISTS tmp_asapp_reps;
CREATE TEMPORARY TABLE tmp_asapp_reps AS
SELECT
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
        partition_date,
        partition_hour,
        regexp_replace(created_ts,'T.*','') as created_date,
        regexp_replace(regexp_replace(created_ts,'^.*T',''),':.*','') as created_hour,
        MAX(regexp_replace(created_ts,'T.*','')) OVER (PARTITION BY partition_date, partition_hour) AS hourly_max_created_date,
        MAX(regexp_replace(created_ts,'T.*','')) OVER (PARTITION BY partition_date) AS daily_max_created_date,
        MAX(regexp_replace(regexp_replace(created_ts,'^.*T',''),':.*','')) OVER (PARTITION BY partition_date, partition_hour) AS hourly_max_created_hour,
        MAX(regexp_replace(regexp_replace(created_ts,'^.*T',''),':.*','')) OVER (PARTITION BY partition_date) AS daily_max_created_hour
   FROM ${env:DB_NIFI}.asp_asapp_reps
  WHERE partition_date = DATE_SUB(TO_DATE('${env:RUN_DATE}'), 1)
;

INSERT INTO TABLE asp_asapp_reps PARTITION (created_date,created_hour)
SELECT  DISTINCT
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
        partition_date,
        partition_hour,
        CASE WHEN created_date is not null THEN created_date
             WHEN (created_date is null and hourly_max_created_date is not null) then hourly_max_created_date
             WHEN (created_date is null and hourly_max_created_date is null and daily_max_created_date is not null) then daily_max_created_date
             ELSE partition_date
        END AS created_date,
        CASE WHEN created_hour is not null THEN created_hour
             WHEN (created_hour is null and hourly_max_created_hour is not null) then hourly_max_created_hour
             WHEN (created_hour is null and hourly_max_created_hour is null and daily_max_created_hour is not null) then daily_max_created_hour
             ELSE partition_hour
        END AS created_hour
   FROM tmp_asapp_reps
;


SELECT "Inserting Data For asp_asapp_transfers - 13";
MSCK REPAIR TABLE ${env:DB_NIFI}.asp_asapp_transfers;

DROP TABLE IF EXISTS tmp_asapp_transfers;
CREATE TEMPORARY TABLE tmp_asapp_transfers AS
SELECT
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
        partition_date,
        partition_hour,
        regexp_replace(instance_ts,'T.*','') as instance_date,
        regexp_replace(regexp_replace(instance_ts,'^.*T',''),':.*','') as instance_hour,
        MAX(regexp_replace(instance_ts,'T.*','')) OVER (PARTITION BY partition_date, partition_hour) AS hourly_max_instance_date,
        MAX(regexp_replace(instance_ts,'T.*','')) OVER (PARTITION BY partition_date) AS daily_max_instance_date,
        MAX(regexp_replace(regexp_replace(instance_ts,'^.*T',''),':.*','')) OVER (PARTITION BY partition_date, partition_hour) AS hourly_max_instance_hour,
        MAX(regexp_replace(regexp_replace(instance_ts,'^.*T',''),':.*','')) OVER (PARTITION BY partition_date) AS daily_max_instance_hour
   FROM ${env:DB_NIFI}.asp_asapp_transfers
  WHERE partition_date = DATE_SUB(TO_DATE('${env:RUN_DATE}'), 1)
;

INSERT INTO TABLE asp_asapp_transfers PARTITION (instance_date,instance_hour)
SELECT  DISTINCT
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
   FROM tmp_asapp_transfers
;
