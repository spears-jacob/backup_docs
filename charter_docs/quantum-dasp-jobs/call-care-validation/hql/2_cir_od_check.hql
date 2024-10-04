SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.cli.print.header=true;

 -- set old_cir_table=387455165365/prod_dasp.cs_call_in_rate;
-- SET new_cir_table=387455165365/prod_dasp.cs_call_in_rate;
-- set TEST_DATE=2021-03-10;

--Assumption: the call-in rate and digital-first-contact rates should not change
--- much from what they were before
--Call-in Rate (cir)
------ Matches old data (cir_od)

-- it pulls anything where they're not exactly equal; you have to exercise
--- your judgement about whether that's an OK outcome or if more research needs
--- to be done

SELECT avg(discrepancy) as is_this_acceptable_CIR_percentage_point_deviance FROM (
SELECT
      old.*
      , new.CIR as new_cir
      , new.CIR-old.CIR as discrepancy
 FROM (SELECT
              call_date
            , acct_agent_mso
            , visit_type
            , customer_type
            , 100*(calls_with_visit/authenticated_visits) as CIR
 FROM `${hiveconf:old_cir_table}`) old
 JOIN (SELECT
             call_date
             , acct_agent_mso
             , visit_type
             , customer_type
             , 100*(calls_with_visit/authenticated_visits) as CIR
        FROM
            `${hiveconf:new_cir_table}`) new
   ON new.call_date=old.call_date
  AND old.acct_agent_mso=new.acct_agent_mso
  AND old.visit_type=new.visit_type
 WHERE old.CIR<>new.CIR
   AND old.call_date='${hiveconf:test_date}'
   AND new.call_date='${hiveconf:test_date}'
) av
limit 10
;

SELECT --* -- uncomment this to make it research
      avg(discrepancy) as is_this_acceptable_dfcr_percentage_point_deviance
FROM (SELECT
            old.*
            --,new.*
            , new.dfcr as new_dfcr
            , new.dfcr-old.dfcr as discrepancy
        FROM (
             SELECT
                    call_date
                    , acct_agent_mso
                    , visit_type
                    , customer_type
                    , calls_with_visit
                    , handled_calls
                    , 100*(calls_with_visit/handled_calls) as dfcr
               FROM
                    `${hiveconf:old_cir_table}`) old
       JOIN (SELECT
                   call_date
                   , acct_agent_mso
                   , visit_type
                   , customer_type
                   , calls_with_visit
                   , validated_calls
                   , 100*(calls_with_visit/validated_calls) as dfcr
              FROM
                  `${hiveconf:new_cir_table}`) new
         ON new.call_date=old.call_date
        AND old.acct_agent_mso=new.acct_agent_mso
        AND old.visit_type=new.visit_type
      WHERE 1=1 -- AND old.dfcr<>new.dfcr
        AND old.call_date='${hiveconf:test_date}'
        AND new.call_date='${hiveconf:test_date}'
    ) av
limit 10
;
