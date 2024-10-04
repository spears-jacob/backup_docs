SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.resultset.use.unique.column.names=false;
set hive.cli.print.header=true;

-- SET new_call_table=387455165365/prod.atom_cs_call_care_data_3;
-- SET new_cir_table=387455165365/prod_dasp.cs_call_in_rate;
-- set new_visits_table=387455165365/prod_dasp.cs_calls_with_prior_visits;


--Assumption: Regardless of what else we've changed, call-in rate should make sense
-- given the data in the other tables
--Call-in Rate (cir)
------ Consistent data between tables (cir_cd)

SELECT cwv.*
     , cir.calls_with_visit
     , "Discrepancy" as should_return_5_records
FROM (SELECT
             call_date,
             account_agent_mso,
             customer_type,
             visit_type,
             count(distinct call_inbound_key) as generated_calls_with_visit
         FROM `${hiveconf:new_visits_table}` cwv
         WHERE call_date >='${hiveconf:start_date}'
         AND call_date <='${hiveconf:end_date}'
        GROUP BY call_date,account_agent_mso,customer_type,visit_type
) cwv
 join `${hiveconf:new_cir_table}` cir
   on cir.call_date=cwv.call_date
  AND cir.acct_agent_mso=cwv.account_agent_mso
  AND cir.customer_type=cwv.customer_type
  AND UPPER(cir.visit_type)=UPPER(cwv.visit_type)
WHERE cir.call_date >='${hiveconf:start_date}'
  AND cir.call_date <='${hiveconf:end_date}'
LIMIT 5
;


SELECT cwv.*
     , cir.calls_with_visit
     , "Discrepancy" as should_return_0_records
FROM (SELECT
             call_date,
             account_agent_mso,
             customer_type,
             visit_type,
             count(distinct call_inbound_key) as generated_calls_with_visit
         FROM `${hiveconf:new_visits_table}` cwv
         WHERE call_date >='${hiveconf:start_date}'
         AND call_date <='${hiveconf:end_date}'
        GROUP BY call_date,account_agent_mso,customer_type,visit_type
) cwv
 join `${hiveconf:new_cir_table}` cir
   on cir.call_date=cwv.call_date
  AND cir.acct_agent_mso=cwv.account_agent_mso
  AND cir.customer_type=cwv.customer_type
  AND UPPER(cir.visit_type)=UPPER(cwv.visit_type)
WHERE generated_calls_with_visit<>calls_with_visit
  AND cir.call_date >='${hiveconf:start_date}'
  AND cir.call_date <='${hiveconf:end_date}'
--AND cwv.call_date<'2020-01-24'
LIMIT 10
;

SELECT cd.*
      ,cir.distinct_call_accts
      ,cir.handled_calls
      ,cir.validated_calls
      , "Discrepancy" as should_return_5_records
FROM (SELECT call_end_date_utc as call_date,
             account_agent_mso,
             customer_type,
             count(distinct (CASE WHEN enhanced_account_number = 0 THEN encrypted_padded_account_number_256 END)) as generated_distinct_call_accts,
             count(distinct call_inbound_key) as generated_handled_calls,
             count(distinct (case when lower(${hiveconf:daspdb}.aes_decrypt256(encrypted_padded_account_number_256)) NOT LIKE '%unknown%' AND enhanced_account_number = 0 then call_inbound_key end)) as generated_validated_calls
       FROM `${hiveconf:new_call_table}`
      WHERE segment_handled_flag = true
       AND call_end_date_utc >='${hiveconf:start_date}'
       AND call_end_date_utc <='${hiveconf:end_date}'
      GROUP BY call_end_date_utc, account_agent_mso, customer_type) cd
 join `${hiveconf:new_cir_table}` cir
   on cir.call_date=cd.call_date
  AND cir.acct_agent_mso=cd.account_agent_mso
  AND cir.customer_type=cd.customer_type
WHERE
     cir.call_date >='${hiveconf:start_date}'
      AND cir.call_date <='${hiveconf:end_date}'
LIMIT 5
;

SELECT cd.*
      ,cir.distinct_call_accts
      ,cir.handled_calls
      ,cir.validated_calls
      , "Discrepancy" as should_return_0_records
FROM (SELECT call_end_date_utc as call_date,
             account_agent_mso,
             customer_type,
             count(distinct (CASE WHEN enhanced_account_number = 0 THEN encrypted_padded_account_number_256 END)) as generated_distinct_call_accts,
             count(distinct call_inbound_key) as generated_handled_calls,
             count(distinct (case when lower(${hiveconf:daspdb}.aes_decrypt256(encrypted_padded_account_number_256)) NOT LIKE '%unknown%' AND enhanced_account_number = 0 then call_inbound_key end)) as generated_validated_calls
       FROM `${hiveconf:new_call_table}`
      WHERE segment_handled_flag = true
       AND call_end_date_utc >='${hiveconf:start_date}'
       AND call_end_date_utc <='${hiveconf:end_date}'
      GROUP BY call_end_date_utc, account_agent_mso, customer_type) cd
 join `${hiveconf:new_cir_table}` cir
   on cir.call_date=cd.call_date
  AND cir.acct_agent_mso=cd.account_agent_mso
  AND cir.customer_type=cd.customer_type
WHERE
      (abs(generated_distinct_call_accts-distinct_call_accts) >0
      OR abs(generated_handled_calls-handled_calls) >0
      OR abs(generated_validated_calls-validated_calls) >0)
      AND cir.call_date >='${hiveconf:start_date}'
      AND cir.call_date <='${hiveconf:end_date}'
LIMIT 10
;
