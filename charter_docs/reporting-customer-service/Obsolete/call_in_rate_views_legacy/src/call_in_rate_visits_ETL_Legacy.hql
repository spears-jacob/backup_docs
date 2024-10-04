--The goal of this process is to support the Call-In Rate Dashboard on Tableau Server which requires linking visits with calls

set mapreduce.input.fileinputformat.split.maxsize=100000000;
set mapreduce.input.fileinputformat.split.minsize=100000000;

--Step 1: Get legacy data for BHN and TWC legacy accounts
DROP TABLE IF EXISTS ${env:TMP_db}.cs_page_visits_Legacy;
CREATE TABLE IF NOT EXISTS ${env:TMP_db}.cs_page_visits_Legacy
(
  account_number string,
  visit_id string,
  customer_type string,
  message__category string,
  message__name string,
  message__timestamp bigint,
  visit_type string,
  mso string
)
partitioned by
(
  partition_date_utc string
)
;
INSERT INTO TABLE ${env:TMP_db}.cs_page_visits_Legacy PARTITION (partition_date_utc)
SELECT distinct
        a.visit__account__account_number as account_number,     -- Customer account number encrypted with 128-bit key length
        a.visit__visit_id AS visit_id,                                          -- Visit ID is the unique identifier for each customer visit and carries through page views
        CASE WHEN b.customer__type is null then 'UNMAPPED'
          ELSE UPPER(b.customer__type) END as customer_type,
        a.message__category,
        a.message__name_array,
        CAST(a.message__timestamp as BIGINT) as message__timestamp,
        'L-TWC - SMB' as visit_type,                                            -- Create Visit_Type field to identify these visits as Spectrum.net
        'TWC' AS mso,
        a.partition_date_utc
FROM (SELECT PROD.TWCDecrypt(a.visit__account__account_number) as visit__account__account_number,
       visit__visit_id,
       message__category,
       message__name_array,
       message__timestamp,
       visit__application_details__application_name,
       partition_date_utc
FROM PROD.asp_v_twc_bus_global_events a LATERAL VIEW explode(message__name)  explode_table AS message__name_array
WHERE (a.partition_date_utc between '${env:START_DATE}' AND '${env:END_DATE}')     --These dates reflect events activity within 24 hours of the latest call extract
        AND visit__account__account_number is not null                                -- Pull only authenticated visits
        AND visit__account__account_number != 'GSWNkZXIfDPD6x25Na3i8g==' -- remove pending account numbers
        AND visit__account__account_number != '7FbKtybuOWU4/Q0SRInbHA==' -- remove empty string account numbers
        AND visit__visit_id is not null) a
LEFT JOIN prod.account_current b 
on PROD.TWCDecrypt(a.visit__account__account_number) = PROD.aes_decrypt256(b.account__number_aes256)
;

INSERT INTO TABLE ${env:TMP_db}.cs_page_visits_Legacy PARTITION (partition_date_utc)
SELECT distinct
        PROD.TWCDecrypt(a.visit__account__account_number) as account_number,     -- Customer account number encrypted with 128-bit key length
        a.visit__visit_id AS visit_id,                                          -- Visit ID is the unique identifier for each customer visit and carries through page views
        CASE WHEN b.customer__type is null then 'UNMAPPED'
          ELSE UPPER(b.customer__type) END as customer_type,
        a.message__category,
        a.message__name,
        CAST(a.message__timestamp as BIGINT) as message__timestamp,
        'L-TWC - RESI' as visit_type,                                            -- Create Visit_Type field to identify these visits as Spectrum.net
        'TWC' AS mso,
        a.partition_date_utc
FROM PROD.asp_v_twc_residential_global_events a                                                    -- This table contains visits for Spectrum.net exclusively
  LEFT JOIN PROD.account_current b on PROD.TWCDecrypt(a.visit__account__account_number) = ${env:ENVIRONMENT}.aes_decrypt256(b.account__number_aes256)
WHERE partition_date_utc between '${env:START_DATE}' AND '${env:END_DATE}'    --These dates reflect events activity within 24 hours of the latest call extract
  AND visit__account__account_number is not null                                -- Pull only authenticated visits
  AND visit__account__account_number != 'GSWNkZXIfDPD6x25Na3i8g==' -- remove pending account numbers
  AND visit__account__account_number != '7FbKtybuOWU4/Q0SRInbHA==' -- remove empty string account numbers
  AND visit__visit_id is not null
  ;

INSERT INTO ${env:TMP_db}.cs_page_visits_Legacy PARTITION (partition_date_utc)
SELECT distinct
        SUBSTR(a.visit__account__account_number, 2) as account_number,     -- Customer account number encrypted with 128-bit key length
        a.visit__visit_id AS visit_id,                                          -- Visit ID is the unique identifier for each customer visit and carries through page views
        CASE WHEN b.customer__type is null then 'UNMAPPED'
          ELSE UPPER(b.customer__type) END as customer_type,
        a.message__category,
        a.message__name_array,
        CAST(a.message__timestamp as BIGINT) as message__timestamp,
        'L-BHN - SMB' as visit_type,                                            -- Create Visit_Type field to identify these visits as Spectrum.net
        'BHN' AS mso,
        a.partition_date_utc
FROM (SELECT ${env:ENVIRONMENT}.aes_decrypt256(a.visit__account__account_number) as visit__account__account_number,
       visit__visit_id,
       message__category,
       message__name_array,
       message__timestamp,
       visit__application_details__application_name,
       partition_date_utc
FROM PROD.asp_v_bhn_my_services_events a LATERAL VIEW explode(message__name)  explode_table AS message__name_array
WHERE (a.partition_date_utc between '${env:START_DATE}' AND '${env:END_DATE}') --These dates reflect events activity within 24 hours of the latest call extract
AND visit__account__account_number is not null                                -- Pull only authenticated visits
  AND visit__account__account_number != 'GSWNkZXIfDPD6x25Na3i8g==' -- remove pending account numbers
  AND visit__account__account_number != '7FbKtybuOWU4/Q0SRInbHA==' -- remove empty string account numbers
  AND visit__visit_id is not null) a
LEFT JOIN PROD.account_current b 
on ${env:ENVIRONMENT}.aes_decrypt256(a.visit__account__account_number) = ${env:ENVIRONMENT}.aes_decrypt256(b.account__number_aes256)  
;


INSERT INTO ${env:TMP_db}.cs_page_visits_Legacy PARTITION (partition_date_utc)
SELECT distinct
        ${env:ENVIRONMENT}.aes_decrypt256(a.visit__account__account_number) as account_number,     -- Customer account number encrypted with 128-bit key length
        a.visit__visit_id AS visit_id,                                          -- Visit ID is the unique identifier for each customer visit and carries through page views
        CASE WHEN b.customer__type is null then 'UNMAPPED'
          ELSE UPPER(b.customer__type) END as customer_type,
        a.message__category,
        a.message__name,
        CAST(a.message__timestamp as BIGINT) as message__timestamp,
        'L-BHN - RESI' as visit_type,                                            -- Create Visit_Type field to identify these visits as Spectrum.net
        'BHN' AS mso,
        a.partition_date_utc
FROM PROD.asp_v_bhn_residential_events a                                                    -- This table contains visits for Spectrum.net exclusively
  LEFT JOIN PROD.account_current b on ${env:ENVIRONMENT}.aes_decrypt256(a.visit__account__account_number) = ${env:ENVIRONMENT}.aes_decrypt256(b.account__number_aes256)
WHERE a.partition_date_utc between '${env:START_DATE}' AND '${env:END_DATE}'    --These dates reflect events activity within 24 hours of the latest call extract
  AND visit__account__account_number is not null                                -- Pull only authenticated visits
  AND visit__account__account_number != 'GSWNkZXIfDPD6x25Na3i8g==' -- remove pending account numbers
  AND visit__account__account_number != '7FbKtybuOWU4/Q0SRInbHA==' -- remove empty string account numbers
  AND visit__visit_id is not null 
;



--Step 2: Add new events into current events table which is used for reporting
INSERT OVERWRITE TABLE ${env:TMP_db}.cs_care_events_Legacy PARTITION (partition_date_utc)
SELECT account_number, visit_id, customer_type as visit_customer_type, message__category, message__name, message__timestamp, visit_type, mso, partition_date_utc
FROM ${env:TMP_db}.cs_page_visits_Legacy
WHERE account_number NOT rlike '[A-Z]'
;


set hive.auto.convert.join=false;
--Step 4: Combine call AND event at page level
DROP TABLE IF EXISTS ${env:TMP_db}.cs_calls_and_events_Legacy;
CREATE TABLE ${env:TMP_db}.cs_calls_and_events_Legacy
  AS
  SELECT
         a.account_number as visit_account_number,
         a.visit_id,
         a.customer_type as visit_customer_type,
         a.message__category,
         a.message__name,
         a.message__timestamp,
         a.mso,
         a.partition_date_utc,
         a.visit_type,
         b.account_number,
         b.agent_effective_hire_date,
         b.agent_job_role_name,
         b.agent_mso,
         b.call_end_date_utc,
         b.call_end_datetime_utc,
         b.call_end_time_utc,
         b.call_end_timestamp_utc,
         b.call_id,
         b.call_inbound_key,
         b.call_start_date_utc,
         b.call_start_datetime_utc,
         b.call_start_time_utc,
         b.call_start_timestamp_utc,
         b.call_type,
         b.care_center_management_name,
         b.cause_description,
         b.company_code,
         b.created_by,
         b.created_on,
         b.customer_account_number,
         b.segment_handled_flag,
         b.customer_subtype,
         b.customer_type,
         b.eduid,
         b.issue_description,
         b.last_handled_segment_flag,
         b.location_name,
         b.notes_txt,
         b.phone_number_from_tracker,
         b.previous_call_time_utc,
         b.product,
         b.record_update_timestamp,
         b.resolution_description,
         b.segment_duration_minutes,
         b.segment_duration_seconds,
         b.segment_end_datetime_utc,
         b.segment_end_time_utc,
         b.segment_end_timestamp_utc,
         b.segment_number,
         b.segment_start_datetime_utc,
         b.segment_start_time_utc,
         b.segment_start_timestamp_utc,
         b.service_call_tracker_id,
         b.split_sum_desc,
         b.truck_roll_flag
  FROM ${env:TMP_db}.cs_page_visits_Legacy a                                                 -- Table containing the spectrum.net, SB.net, and myspectrum app visits for the latest extract timeframe
    INNER JOIN ${env:ENVIRONMENT}.cs_call_data b ON a.account_number = ${env:ENVIRONMENT}.aes_decrypt256(b.account_number) --Calls and visits are linked together based on customer account information
  WHERE b.call_end_date_utc >= '${env:START_DATE}' --limiting calls to cover the new extract dates
  AND b.enhanced_account_number = 0
  AND a.account_number NOT rlike '[A-Z]';



set hive.auto.convert.join=true;
--Step 5: Limit linked events/calls to only see those occurring within 24 hours of each other
DROP TABLE IF EXISTS ${env:TMP_db}.cs_calls_with_events24hr_new_Legacy;
CREATE TABLE ${env:TMP_db}.cs_calls_with_events24hr_new_Legacy AS
SELECT distinct *
FROM ${env:TMP_db}.cs_calls_and_events_Legacy
WHERE abs(call_start_timestamp_utc/1000 - message__timestamp) <= '86400'    -- Limit data to visits that occurred within 24 hours of call start time
;




--Step 6: Create look up table for visit and call within 2 days interval, at visit level and limit to handled calls
INSERT OVERWRITE TABLE ${env:ENVIRONMENT}.cs_calls_with_prior_visit_Legacy PARTITION (call_end_date_utc)
SELECT  distinct
        a.account_number,                                                       -- Customer account number in 256-bit encryption
        a.call_inbound_key,                                                     -- This is a unique call identifier
        a.customer_type,
        a.customer_subtype,
        a.agent_mso,
        a.product,
        a.issue_description,
        a.cause_description,
        a.resolution_description,
        a.issue_category,
        a.cause_category,
        a.resolution_category,
        a.resolution_type,
        (a.call_start_timestamp_utc)/1000 as call_start_div,  -- updated from 1000 to 1000000
        visit_type,
        a.visitStart,
        a.mso,
        a.call_end_date_utc
FROM
    (SELECT cd.account_number,
            cd.call_inbound_key,
            cd.customer_type,
            cd.customer_subtype,
            cd.agent_mso,
            cd.product,
            cd.issue_description,
            cd.cause_description,
            cd.resolution_description,
            icl.issue_category,
            icl.cause_category,
            l.resolution_category,
            l.resolution_type,
            cd.call_start_timestamp_utc,
            cd.call_end_date_utc,
            cd.visit_type,
            min(cd.message__timestamp) AS visitStart,
            cd.mso,
            max(cd.message__timestamp) AS visitEnd,
            cd.previous_call_time_utc
      FROM ${env:TMP_db}.cs_calls_with_events24hr_new_Legacy cd
        LEFT JOIN ${env:ENVIRONMENT}.cs_issue_cause_lookup icl
          ON UPPER(cd.issue_description) = icl.issue_description                    --Grabbing CII's categories for issue/cause
          AND UPPER(cd.cause_description) = icl.cause_description
          AND UPPER(cd.call_type) = icl.call_group
        LEFT JOIN ${env:ENVIRONMENT}.cs_resolution_lookup l
          ON UPPER (cd.resolution_description) = l.resolution_description              --Grabbing CII's categories for resolution
      WHERE segment_handled_flag = true
     GROUP BY cd.account_number,
              cd.visit_type,
              cd.call_inbound_key,
              cd.customer_type,
              cd.customer_subtype,
              cd.agent_mso,
              cd.mso,
              cd.product,
              cd.issue_description,
              cd.cause_description,
              cd.resolution_description,
              icl.issue_category,
              icl.cause_category,
              l.resolution_category,
              l.resolution_type,
              cd.call_start_timestamp_utc,
              cd.call_end_date_utc,
              cd.previous_call_time_utc) a
WHERE a.visitStart < (a.call_start_timestamp_utc/1000)          -- limits visits to ones that occurred before the call
      AND (a.visitStart > (a.previous_call_time_utc/1000) OR a.previous_call_time_utc is null)
;