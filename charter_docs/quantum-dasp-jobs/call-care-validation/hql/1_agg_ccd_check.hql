-- set call_table=387455165365/prod.atom_cs_call_care_data_3;
-- SEt call_agg_table=387455165365/prod_dasp.cs_call_care_data_agg;
-- set TEST_DATE=2021-11-01;

--- Please evaluate this when we convert the call care scala job to spark-sql

SELECT count(raw.call_end_date_utc)
     , "This should not be 0" as requirement
  FROM (SELECT
              account_agent_mso
              ,agent_mso
              ,call_end_date_utc
              ,customer_type
              ,issue_description
              ,cause_description
              ,resolution_description
              ,segment_handled_flag
              ,truck_roll_flag
              ,COUNT(segment_id) as segments
              ,COUNT(distinct call_inbound_key) as calls
              ,COUNT(distinct encrypted_account_number_256) as accounts
              ,SUM(case when segment_duration_minutes>0 then segment_duration_minutes else 0 end) as segment_duration_minutes
       FROM `${hiveconf:call_table}`
      WHERE call_end_date_utc='${hiveconf:TEST_DATE}'
      GROUP BY
               account_agent_mso
              ,agent_mso
              ,call_end_date_utc
              ,customer_type
              ,issue_description
              ,cause_description
              ,resolution_description
              ,segment_handled_flag
              ,truck_roll_flag
      ) raw
 JOIN `${hiveconf:call_agg_table}` agg
   ON raw.account_agent_mso=agg.account_agent_mso
  AND raw.agent_mso=agg.agent_mso
  AND raw.call_end_date_utc=agg.call_end_date_utc
  AND agg.customer_type=raw.customer_type
  AND raw.issue_description=agg.issue_description
  AND raw.cause_description=agg.cause_description
  AND raw.resolution_description=agg.resolution_description
  AND raw.segment_handled_flag=agg.segment_handled_flag
  AND raw.truck_roll_flag=agg.truck_roll_flag
;

-------------------------------------------


SELECT
      raw.agent_mso
      ,raw.call_end_date_utc
      ,raw.customer_type
      ,raw.issue_description
      ,raw.cause_description
      ,raw.resolution_description
      ,raw.segment_handled_flag
      ,raw.truck_roll_flag
      ,raw.segments
      ,agg.segments
      ,raw.calls
      ,agg.calls
      ,raw.accounts
      ,agg.accounts
      ,raw.segment_duration_minutes
      ,agg.segment_duration_minutes
      , "Not Empty" as should_be_empty
FROM (SELECT
            account_agent_mso
            ,agent_mso
            ,call_end_date_utc
            ,customer_type
            ,issue_description
            ,cause_description
            ,resolution_description
            ,segment_handled_flag
            ,truck_roll_flag
            ,COUNT(segment_id) as segments
            ,COUNT(distinct call_inbound_key) as calls
            ,COUNT(distinct encrypted_account_number_256) as accounts
            ,SUM(case when segment_duration_minutes>0 then segment_duration_minutes else 0 end) as segment_duration_minutes
      FROM `${hiveconf:call_table}`
      WHERE call_end_date_utc='${hiveconf:TEST_DATE}'
      GROUP BY
             account_agent_mso
            ,agent_mso
            ,call_end_date_utc
            ,customer_type
            ,issue_description
            ,cause_description
            ,resolution_description
            ,segment_handled_flag
            ,truck_roll_flag
      ) raw
 JOIN `${hiveconf:call_agg_table}` agg
   ON raw.account_agent_mso=agg.account_agent_mso
  AND raw.agent_mso=agg.agent_mso
  AND raw.call_end_date_utc=agg.call_end_date_utc
  AND agg.customer_type=raw.customer_type
  AND raw.issue_description=agg.issue_description
  AND raw.cause_description=agg.cause_description
  AND raw.resolution_description=agg.resolution_description
  AND raw.segment_handled_flag=agg.segment_handled_flag
  AND raw.truck_roll_flag=agg.truck_roll_flag
WHERE
     raw.segments<>agg.segments
  OR raw.calls<>agg.calls
  OR raw.accounts<>agg.accounts
  OR abs(raw.segment_duration_minutes-agg.segment_duration_minutes)>1

LIMIT 10
;
