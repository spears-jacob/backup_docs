--Step 9: Create view to allow minutes-to-call reporting
--DROP VIEW IF EXISTS ${env:ENVIRONMENT}.cs_v_calls_with_visits_Legacy;
CREATE VIEW IF NOT EXISTS ${env:ENVIRONMENT}.cs_v_calls_with_visits_Legacy
AS
SELECT  call_end_date_utc as call_date,                            -- Call date is set based on the call end date
        account_number,
        customer_type,
        customer_subtype,
        call_inbound_key,
        product,
        agent_mso,
        visit_type,
        issue_description,
        issue_category,
        cause_description,
        cause_category,
        resolution_description,
        resolution_category,
        resolution_type,
        (MIN(call_start_div)-MAX(visitstart))/60 as minutes_to_call,             -- Calculates the time between visit start and call start time in minutes
        mso
FROM ${env:ENVIRONMENT}.cs_calls_with_prior_visit_Legacy a
GROUP BY  call_inbound_key,
          account_number,
          customer_type,
          customer_subtype,
          call_end_date_utc,
          product,
          agent_mso,
          visit_type,
          issue_description,
          issue_category,
          cause_description,
          cause_category,
          resolution_category,
          resolution_type,
          resolution_description,
          mso
;

--Step 10: Create rate view to support care reporting
--DROP VIEW IF EXISTS ${env:ENVIRONMENT}.cs_v_visit_rate_4calls_Legacy;
CREATE VIEW IF NOT EXISTS ${env:ENVIRONMENT}.cs_v_visit_rate_4calls_Legacy
AS
    SELECT * FROM 
    ( 
        SELECT COALESCE(c.partition_date_utc, b.call_date, a.call_date) as call_date, 
            UPPER(COALESCE(b.agent_mso, a.agent_mso, 'UNK')) as agent_mso, 
            COALESCE(c.visit_type, a.visit_type, 'unknown') as visit_type, 
            COALESCE(b.customer_type, a.customer_type, c.visit_customer_type,'UNMAPPED') as customer_type, 
            COALESCE(a.calls_with_visit, 0) as calls_with_visit, 
            COALESCE(b.handled_acct_calls, 0) as handled_acct_calls, 
            COALESCE(b.total_acct_calls, 0) as total_acct_calls, 
            COALESCE(b.total_calls, 0) as total_calls, 
            COALESCE(c.total_acct_visits, 0) as total_acct_visits, 
            COALESCE(c.total_visits, 0) as total_visits, 
            c.mso 
        FROM (
            SELECT 
                partition_date_utc, 
                CASE WHEN visit_type in ('L-BHN - RESI','L-TWC - RESI') THEN 'RESIDENTIAL' 
                    ELSE 'COMMERCIAL' 
                END as visit_customer_type, 
                visit_type, -- Query pulls all visits to Spectrum.net and SB.net by day 
                count(distinct account_number) as total_acct_visits, 
                count(distinct visit_id) as total_visits, mso 
            FROM ${env:TMP_db}.cs_care_events_Legacy 
            WHERE account_number is not null -- Pull only authenticated visits 
            AND visit_id is not null 
            GROUP BY partition_date_utc, visit_type, visit_customer_type, mso) c 
            FULL JOIN (
                SELECT call_end_date_utc as call_date, -- Query pulls all handled calls and summarizes by date 
                    agent_mso, 
                    customer_type, 
                    count(distinct (CASE WHEN enhanced_account_number = 0 THEN account_number END)) as total_acct_calls, 
                    count(distinct call_inbound_key) as total_calls, 
                    count(distinct (case when lower(${env:ENVIRONMENT}.aes_decrypt256(account_number)) !='unknown' AND enhanced_account_number = 0 then call_inbound_key end)) as handled_acct_calls 
                FROM ${env:ENVIRONMENT}.cs_call_data 
                WHERE (call_end_date_utc >= '2019-01-01')  -- hardcoded the date to avoid limiting the rows to the job execution dates
                AND segment_handled_flag = true and agent_mso in ('TWC','BHN') 
                GROUP BY call_end_date_utc, agent_mso, customer_type) b 
            on b.call_date = c.partition_date_utc and b.customer_type = c.visit_customer_type and b.agent_mso = c.mso 
            FULL JOIN (
                SELECT call_end_date_utc as call_date, 
                    agent_mso, 
                    customer_type, 
                    visit_type, 
                    count(distinct call_inbound_key) as calls_with_visit -- Query pulls all handled calls that had a visit start before call and after previous call 
                FROM ${env:ENVIRONMENT}.cs_calls_with_prior_visit_Legacy -- Uses view that limited to handled calls with visits before call 
                WHERE ((agent_mso = 'BHN' and visit_type rlike '.*BHN.*') 
                OR (agent_mso = 'TWC' and visit_type rlike '.*TWC.*')) 
                GROUP BY call_end_date_utc, agent_mso, customer_type,visit_type) a 
            on c.partition_date_utc = a.call_date and a.visit_type = c.visit_type and a.agent_mso = b.agent_mso and a.customer_type = b.customer_type ) d ;