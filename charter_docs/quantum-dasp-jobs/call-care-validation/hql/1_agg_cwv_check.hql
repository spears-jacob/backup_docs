--The aggregate data tables should be consistent with the tables they're aggregated from
--
-- SET visits_table=387455165365/prod_dasp.cs_calls_with_prior_visits;
-- SET visits_agg_table=387455165365/prod_dasp.cs_calls_with_prior_visits_agg;
--  SET visits_table=stg_dasp.cs_calls_with_prior_visits;
--  SET visits_agg_table=stg_dasp.cs_calls_with_prior_visits_agg;
--  set TEST_DATE=2021-11-24;

SELECT
      raw.customer_type
      , raw.agent_mso
      , raw.account_agent_mso
      , raw.visit_type
      , raw.primary_auto_disposition_issue
      , raw.primary_auto_disposition_cause
      , raw.primary_manual_disposition_issue
      , raw.primary_manual_disposition_cause
      , raw.primary_manual_disposition_resolution
      , raw.issue_description
      , raw.cause_description
      , raw.resolution_description
      , raw.call_date
      , raw.generated_segment_count
      , agg.segments
      , raw.generated_call_count
      , agg.calls
      , raw.generated_account_count
      , agg.accounts
      , "discrepancy" as should_return_0_results
FROM
      (SELECT customer_type,agent_mso
              ,account_agent_mso
              ,visit_type
              ,primary_auto_disposition_issue
              ,primary_auto_disposition_cause
              ,primary_manual_disposition_issue
              ,primary_manual_disposition_cause
              ,primary_manual_disposition_resolution
              ,issue_description
              ,cause_description
              ,resolution_description
              ,call_date
              ,count(1) as generated_segment_count
              ,count(distinct call_inbound_key) as generated_call_count
              ,count(distinct account_key) as generated_account_count
       FROM `${hiveconf:visits_table}`
      WHERE call_date='${hiveconf:TEST_DATE}'
      GROUP BY
               customer_type
               , agent_mso
               , account_agent_mso
               , visit_type
               , primary_auto_disposition_issue
               , primary_auto_disposition_cause
               , primary_manual_disposition_issue
               , primary_manual_disposition_cause
               , primary_manual_disposition_resolution
               , issue_description
               , cause_description
               , resolution_description
               , call_date
       ) raw
 JOIN `${hiveconf:visits_agg_table}` agg
   ON agg.customer_type=raw.customer_type
  AND agg.agent_mso=raw.agent_mso
  AND agg.account_agent_mso=raw.account_agent_mso
  AND agg.visit_type=raw.visit_type
  AND agg.primary_auto_disposition_issue =raw.primary_auto_disposition_issue
  AND agg.primary_auto_disposition_cause =raw.primary_auto_disposition_cause
  AND agg.primary_manual_disposition_issue =raw.primary_manual_disposition_issue
  AND agg.primary_manual_disposition_cause =raw.primary_manual_disposition_cause
  AND agg.primary_manual_disposition_resolution =raw.primary_manual_disposition_resolution
  AND agg.issue_description=raw.issue_description
  AND agg.cause_description=raw.cause_description
  AND agg.resolution_description=raw.resolution_description
  AND agg.call_date=raw.call_date
WHERE generated_segment_count<>segments
   OR generated_account_count<>accounts
   OR generated_call_count<>calls
LIMIT 10
;


SELECT count(raw.call_date) as count
,"should not be 0" as requirement
FROM
    (SELECT
           customer_type
          , agent_mso
          , account_agent_mso
          , visit_type
          , primary_auto_disposition_issue
          , primary_auto_disposition_cause
          , primary_manual_disposition_issue
          , primary_manual_disposition_cause
          , primary_manual_disposition_resolution
          , issue_description
          , cause_description
          , resolution_description
          , call_date
          , count(1) as generated_segment_count
          , count(distinct call_inbound_key) as generated_call_count
          , count(distinct account_key) as generated_account_count
     FROM `${hiveconf:visits_table}`
    WHERE call_date='${hiveconf:TEST_DATE}'
    GROUP BY
          customer_type
          , agent_mso
          , account_agent_mso
          , visit_type
          , primary_auto_disposition_issue
          , primary_auto_disposition_cause
          , primary_manual_disposition_issue
          , primary_manual_disposition_cause
          , primary_manual_disposition_resolution
          , issue_description
          , cause_description
          , resolution_description
          , call_date
) raw
 JOIN `${hiveconf:visits_agg_table}` agg
   ON agg.customer_type=raw.customer_type
  AND agg.agent_mso=raw.agent_mso
  AND agg.account_agent_mso=raw.account_agent_mso
  AND agg.visit_type=raw.visit_type
  AND agg.primary_auto_disposition_issue =raw.primary_auto_disposition_issue
  AND agg.primary_auto_disposition_cause =raw.primary_auto_disposition_cause
  AND agg.primary_manual_disposition_issue =raw.primary_manual_disposition_issue
  AND agg.primary_manual_disposition_cause =raw.primary_manual_disposition_cause
  AND agg.primary_manual_disposition_resolution =raw.primary_manual_disposition_resolution
  AND agg.issue_description=raw.issue_description
  AND agg.cause_description=raw.cause_description
  AND agg.resolution_description=raw.resolution_description
  AND agg.call_date=raw.call_date
;
