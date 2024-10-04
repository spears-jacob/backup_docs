USE ${env:ENVIRONMENT};

set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager;
set hive.support.concurrency=false;
set hive.tez.container.size=16000;
set hive.auto.convert.join=false;
set hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set mapreduce.input.fileinputformat.split.maxsize=268435456;
set mapreduce.input.fileinputformat.split.minsize=268435456;
set hive.optimize.sort.dynamic.partition = false;

SELECT '
--------------------------------------------------------------------------------
---- *** 01: Call Data Portion *** ---------------------------------------------
--------------------------------------------------------------------------------
';

INSERT OVERWRITE TABLE msa_adoption_testing_ijg PARTITION (label_date)
select distinct
  call_inbound_key,
  call_start_date_utc,
  call_start_datetime_utc,
  call_end_datetime_utc,
  location_name as agent_location,
  PROD.AES_DECRYPT256(encrypted_created_by_256) as decrypted_agent_id,
  call_start_timestamp_utc,
  call_end_timestamp_utc,
  encrypted_account_number_256 as call_account_number,
  product as line_of_business, --adding as dimension,
  cause_description, --adding as dimension,
  prod.epoch_converter(call_start_date_utc, 'UTC') AS label_date
from prod.red_cs_call_care_data_v
where call_start_date_utc <= '${env:END_DATE}' AND call_start_date_utc >= DATE_ADD('${env:END_DATE}',-8)
  and segment_handled_flag=1
  and last_handled_segment_flag=true
  and call_inbound_key is not null
  and truck_roll_flag=false
  and encrypted_account_number_256 != '7TTmD9UkKwdYsmcWS5tPUQ=='
  and encrypted_account_number_256 is not null
;

SELECT '
--------------------------------------------------------------------------------
---- *** 02: MSA Post-Call Portion *** -----------------------------------------
--------------------------------------------------------------------------------
';

INSERT OVERWRITE TABLE msa_use_table_ijg PARTITION (label_date)
select
  visit__account__enc_account_number as portals_account_number,
  visit__visit_id,
  min(received__timestamp) as min_received__timestamp,
  prod.epoch_converter(min(received__timestamp), 'UTC') AS label_date
from prod.core_quantum_events_portals_v
  where (partition_date_utc <= '${env:END_DATE}' AND partition_date_utc >= DATE_ADD('${env:END_DATE}',-15))
  and visit__application_details__application_name in('myspectrum', 'MySpectrum')
  and visit__visit_id is not null
  and visit__account__enc_account_number != 'dZJho5z9MUSD35BuytdIhg=='
  and visit__account__enc_account_number != 'Qn+T/sa8AB7Gnxhi4Wx2Xg=='
  and visit__account__enc_account_number is not null
group by
visit__account__enc_account_number,
visit__visit_id
;

SELECT '
--------------------------------------------------------------------------------
---- *** 03: Calls + MSA Prior Join *** ----------------------------------------
--------------------------------------------------------------------------------
';

CREATE TEMPORARY TABLE ${env:TMP_db}.calls_and_prior_msa_test_v1_${env:execid} AS
select
  a.call_inbound_key,
  a.call_start_date_utc,
  a.call_start_datetime_utc,
  a.call_end_datetime_utc,
  a.agent_location,
  a.decrypted_agent_id,
  a.call_start_timestamp_utc,
  a.call_end_timestamp_utc,
  a.call_account_number,
  a.line_of_business,
  a.cause_description,
  a.label_date,
  b.visit__visit_id,
  b.min_received__timestamp
from
  (select
    *
    from msa_adoption_testing_ijg
    where call_start_date_utc = DATE_ADD('${env:END_DATE}',-3)) a
left join (select * from msa_use_table_ijg where prod.epoch_converter(min_received__timestamp, 'UTC') <=DATE_ADD('${env:END_DATE}',-3) and prod.epoch_converter(min_received__timestamp, 'UTC') >=DATE_ADD('${env:END_DATE}',-18)) b
on a.call_account_number = b.portals_account_number
;

SELECT '
--------------------------------------------------------------------------------
---- *** 04: Pruning Calls + MSA Prior Data Before Joining MSA Post Data *** ---
--------------------------------------------------------------------------------
';

CREATE TEMPORARY TABLE ${env:TMP_db}.pruned_prior_complete_${env:execid} AS
select subquery1.*
from(
  select
  call_inbound_key,
  call_start_date_utc,
  call_start_datetime_utc,
  call_end_datetime_utc,
  agent_location,
  decrypted_agent_id,
  call_start_timestamp_utc,
  call_end_timestamp_utc,
  call_account_number,
  line_of_business,
  cause_description,
  label_date,
  visit__visit_id,
  min_received__timestamp,
  row_number() over(partition by call_inbound_key order by min_received__timestamp desc) as rownum,
  prod.epoch_timestamp(min_received__timestamp, 'UTC') portals_end_timestamp_utc,
  ((min_received__timestamp - call_start_timestamp_utc)/1000)/60 as timediff_mins_before,  --changed to start time for prior. Should not have been end time
  (((min_received__timestamp - call_start_timestamp_utc)/1000)/60)/60 as timediff_hours_before,
  ((((min_received__timestamp - call_start_timestamp_utc)/1000)/60)/60)/24 as timediff_days_before
  from ${env:TMP_db}.calls_and_prior_msa_test_v1_${env:execid}
  where (((min_received__timestamp - call_start_timestamp_utc)/1000)/60 < 0 or min_received__timestamp is null)    --Filtered to only those used MSA prior to the call or did not use at all
  ) subquery1
where subquery1.rownum=1
;

SELECT '
--------------------------------------------------------------------------------
---- *** 05: Calls + MSA Post Join *** -----------------------------------------
--------------------------------------------------------------------------------
';

CREATE TEMPORARY TABLE ${env:TMP_db}.calls_and_msa_after_join_${env:execid} AS
select
  a.call_inbound_key,
  a.call_start_date_utc,
  a.call_start_datetime_utc,
  a.call_end_datetime_utc,
  a.agent_location,
  a.decrypted_agent_id,
  a.call_start_timestamp_utc,
  a.call_end_timestamp_utc,
  a.call_account_number,
  a.line_of_business,
  a.cause_description,
  a.label_date,
  a.visit__visit_id,
  a.min_received__timestamp,
  a.rownum,
  a.portals_end_timestamp_utc,
  a.timediff_mins_before,
  a.timediff_hours_before,
  a.timediff_days_before,
  b.visit__visit_id as post_visit_id,
  b.min_received__timestamp as min_received__timestamp_after
from ${env:TMP_db}.pruned_prior_complete_${env:execid} a
LEFT JOIN (select * from msa_use_table_ijg
            where prod.epoch_converter(min_received__timestamp, 'UTC') <= '${env:END_DATE}'
            and prod.epoch_converter(min_received__timestamp, 'UTC') >= DATE_ADD('${env:END_DATE}',-3)) b
  ON a.call_account_number = b.portals_account_number
;

SELECT '
--------------------------------------------------------------------------------
---- *** 06: Final Insert *** --------------------------------------------------
--------------------------------------------------------------------------------
';

INSERT OVERWRITE TABLE msa_adoption_final PARTITION (label_date)
select
  final.call_start_date_utc,
  final.decrypted_agent_id,
  final.agent_location,
  final.line_of_business,
  final.cause_description,
  final.identified_opportunities,
  final.identified_successes_and_fails,
  count(*) as counts,
  '${env:END_DATE}' AS label_date
from(
  select
    subquery1.*,
    case
      WHEN subquery1.timediff_days_before is null or subquery1.timediff_days_before <= -15 then "OPPORTUNITY"
      WHEN subquery1.timediff_days_before >-15 and subquery1.timediff_days_before < 0 then "NOT AN OPPORTUNITY"
      when subquery1.timediff_days_before >=0 then "ERROR"
      else null
    end as identified_opportunities,
    case
      WHEN (subquery1.timediff_days_before is null or subquery1.timediff_days_before <=-15) and subquery1.timediff_days_after is null then "FAILED OPPORTUNITY"
      WHEN (subquery1.timediff_days_before is null or subquery1.timediff_days_before <=-15) and subquery1.timediff_days_after > 3 then "FAILED OPPORTUNITY"
      WHEN (subquery1.timediff_days_before is null or subquery1.timediff_days_before <=-15) and subquery1.timediff_days_after <= 3 and subquery1.timediff_days_after >=0 then "SUCCESSFUL OPPORTUNITY"
      WHEN subquery1.timediff_days_before >-15 and subquery1.timediff_days_before <0 then null
      WHEN subquery1.timediff_days_after < 0 then "ERROR"
      else null
    END as identified_successes_and_fails
      from(
        select
        *,
        row_number() over(partition by call_inbound_key order by min_received__timestamp_after asc) as rownum_post,
        ((min_received__timestamp_after - call_start_timestamp_utc)/1000)/60 as timediff_mins_after,
        (((min_received__timestamp_after - call_start_timestamp_utc)/1000)/60)/60 as timediff_hours_after,
        ((((min_received__timestamp_after - call_start_timestamp_utc)/1000)/60)/60)/24 as timediff_days_after
          from ${env:TMP_db}.calls_and_msa_after_join_${env:execid}
            where ((min_received__timestamp_after - call_start_timestamp_utc) >=0 or min_received__timestamp_after is null)
          ) subquery1
    where rownum_post = 1
  ) final
group by
final.call_start_date_utc,
final.decrypted_agent_id,
final.agent_location,
final.line_of_business,
final.cause_description,
final.identified_opportunities,
final.identified_successes_and_fails
;
