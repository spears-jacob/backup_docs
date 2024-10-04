USE ${env:DASP_db};

SET hive.auto.convert.join=false;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions.pernode=1000;
SET hive.exec.max.dynamic.partitions=1000;
SET hive.merge.size.per.task=2048000000;
SET hive.merge.smallfiles.avgsize=2048000000;
SET hive.merge.tezfiles=true;
SET hive.optimize.sort.dynamic.partition = false;
SET hive.stats.fetch.column.stats=false;
SET hive.support.concurrency=false;
SET hive.tez.container.size=8000;
SET hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager;
SET mapreduce.input.fileinputformat.split.maxsize=536870912;
SET mapreduce.input.fileinputformat.split.minsize=536870912;
SET orc.force.positional.evolution=true;
SET tez.grouping.split-count=400;

ADD JAR ${env:JARS_S3_LOCATION}/epoch-1.0-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION epoch_converter AS 'Epoch_To_Denver';

ADD JAR ${env:JARS_S3_LOCATION}/epochtotimestamp-1.0-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION epoch_timestamp AS 'Epoch_Timestamp';

ADD JAR ${env:JARS_S3_LOCATION}/jdatehour-1.0-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION epoch_datehour AS 'Epoch_Datehour';

msck repair table ${env:GLOBAL_DB}.atom_cs_call_care_data_3;

ADD JAR ${env:JARS_S3_LOCATION}/hadoop-libs-hive-1.0.10.jar;
CREATE TEMPORARY FUNCTION aes_decrypt256 AS 'com.spectrum.v1_0_10.hive.udfs.AesDecrypt256';

--------------------------------------------------------------------------------
---- *** 01: Call Data Portion *** ---------------------------------------------
--------------------------------------------------------------------------------


INSERT OVERWRITE TABLE cs_msa_adoption_testing PARTITION (label_date)
select distinct
  call_inbound_key,
  call_start_date_utc,
  call_start_datetime_utc,
  call_end_datetime_utc,
  location_name as agent_location,
  aes_decrypt256(encrypted_created_by_256, 'aes256') as decrypted_agent_id, --leave commented for stg. Use decrypted field for running in prod
  --encrypted_created_by_256 as decrypted_agent_id, --leave commented for prod. Use encrypted field for testing in stg
  call_start_timestamp_utc,
  call_end_timestamp_utc,
  encrypted_account_number_256 as call_account_number,
  product as line_of_business,
  cause_description,
  epoch_converter(CAST(call_start_timestamp_utc AS STRING), 'UTC') AS label_date
  --epoch_datehour(call_start_timestamp_utc, 'UTC') AS label_date
  --'2020-09-19'
from ${env:GLOBAL_DB}.atom_cs_call_care_data_3
--where  call_start_date_utc >= DATE_ADD('${env:END_DATE}',-7) AND call_start_date_utc <= '${env:END_DATE}'
WHERE (call_end_date_utc >= '${env:END_DATE_LESS_SEVEN}' AND call_end_date_utc <= '${env:END_DATE}')
  and segment_handled_flag=1
  and last_handled_segment_flag=true
  and call_inbound_key is not null
  and truck_roll_flag=false
  and encrypted_account_number_256 != '7TTmD9UkKwdYsmcWS5tPUQ=='
  and encrypted_account_number_256 is not null
;


--------------------------------------------------------------------------------
---- *** 02: MSA Post-Call Portion *** -----------------------------------------
--------------------------------------------------------------------------------


INSERT OVERWRITE TABLE cs_msa_use_table PARTITION (label_date)
select
  visit__account__enc_account_number as portals_account_number,
  visit__visit_id,
  min(received__timestamp) as min_received__timestamp,
  epoch_converter(CAST(received__timestamp AS STRING), 'UTC') AS label_date
  --epoch_datehour(min(received__timestamp), 'UTC') AS label_date
from ${env:GLOBAL_DB}.core_quantum_events_sspp
where (partition_date_utc >= '${env:END_DATE_LESS_FIFTEEN}' AND partition_date_utc <= '${env:END_DATE}')
  and visit__application_details__application_name in('myspectrum', 'MySpectrum')
  and visit__visit_id is not null
  and visit__account__enc_account_number != 'dZJho5z9MUSD35BuytdIhg=='
  and visit__account__enc_account_number != 'Qn+T/sa8AB7Gnxhi4Wx2Xg=='
  and visit__account__enc_account_number is not null
group by
visit__account__enc_account_number,
visit__visit_id,
epoch_converter(CAST(received__timestamp AS STRING), 'UTC')
;


--------------------------------------------------------------------------------
---- *** 03: Calls + MSA Prior Join *** ----------------------------------------
--------------------------------------------------------------------------------
SET tez.grouping.split-count=400;

CREATE TEMPORARY TABLE ${env:TMP_db}.cs_calls_and_prior_msa_test_v01_${env:CLUSTER} AS
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
  label_date
from cs_msa_adoption_testing
where call_start_date_utc = '${env:END_DATE_LESS_THREE}'
;

CREATE TEMPORARY TABLE ${env:TMP_db}.cs_calls_and_prior_msa_test_v02_${env:CLUSTER} AS
select
  visit__visit_id,
  portals_account_number,
  min_received__timestamp,
  label_date
from cs_msa_use_table
where (label_date >= '${env:END_DATE_LESS_EIGHTEEN}' AND label_date <= '${env:END_DATE_LESS_THREE}')
;

CREATE TEMPORARY TABLE ${env:TMP_db}.cs_calls_and_prior_msa_test_v1_${env:CLUSTER} AS
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
${env:TMP_db}.cs_calls_and_prior_msa_test_v01_${env:CLUSTER} a
LEFT Join ${env:TMP_db}.cs_calls_and_prior_msa_test_v02_${env:CLUSTER} b
ON a.call_account_number = b.portals_account_number
;

--------------------------------------------------------------------------------
---- *** 04: Pruning Calls + MSA Prior Data Before Joining MSA Post Data *** ---
--------------------------------------------------------------------------------

CREATE TEMPORARY TABLE ${env:TMP_db}.cs_pruned_prior_complete_${env:CLUSTER} AS
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
  row_number() over(partition by call_inbound_key order by min_received__timestamp ASC) as rownum,
  epoch_datehour(min_received__timestamp, 'UTC') portals_end_timestamp_utc,
  ((min_received__timestamp - call_start_timestamp_utc)/1000)/60 as timediff_mins_before,
  (((min_received__timestamp - call_start_timestamp_utc)/1000)/60)/60 as timediff_hours_before,
  ((((min_received__timestamp - call_start_timestamp_utc)/1000)/60)/60)/24 as timediff_days_before
  from ${env:TMP_db}.cs_calls_and_prior_msa_test_v1_${env:CLUSTER}
  ) subquery1
where subquery1.rownum=1
;

--------------------------------------------------------------------------------
---- *** 05: Calls + MSA Post Join *** -----------------------------------------
--------------------------------------------------------------------------------

CREATE TEMPORARY TABLE ${env:TMP_db}.cs_calls_and_msa_after_join_${env:CLUSTER} AS
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
from ${env:TMP_db}.cs_pruned_prior_complete_${env:CLUSTER} a
--LEFT JOIN (select * from cs_msa_use_table
--            where epoch_datehour(min_received__timestamp, 'UTC') <= '${env:END_DATE}'
--            and epoch_datehour(min_received__timestamp, 'UTC') >= DATE_ADD('${env:END_DATE}',-3)) b
LEFT JOIN (select *
           from cs_msa_use_table
           where (label_date >= '${env:END_DATE_LESS_THREE}' and label_date <= '${env:END_DATE}')
          ) b
  ON a.call_account_number = b.portals_account_number
;

--------------------------------------------------------------------------------
---- *** 06: Final Insert *** --------------------------------------------------
--------------------------------------------------------------------------------

INSERT OVERWRITE TABLE cs_msa_adoption_final PARTITION (label_date)
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
      WHEN (subquery1.timediff_days_before >= 0 AND subquery1.timediff_days_before < 4) OR subquery1.timediff_days_before IS NULL or subquery1.timediff_days_before <=-15 then "OPPORTUNITY"
      WHEN subquery1.timediff_days_before >-15 AND subquery1.timediff_days_before < 0 THEN "NOT AN OPPORTUNITY"
      WHEN subquery1.timediff_days_before >=3 THEN "ERROR"
      else null
    end as identified_opportunities,
    case
      WHEN ((subquery1.timediff_days_before >= 0 AND subquery1.timediff_days_before < 4) OR subquery1.timediff_days_before IS NULL or subquery1.timediff_days_before <=-15) and subquery1.timediff_days_after is null then "FAILED OPPORTUNITY"
      WHEN ((subquery1.timediff_days_before >= 0 AND subquery1.timediff_days_before < 4) OR subquery1.timediff_days_before IS NULL or subquery1.timediff_days_before <=-15) and subquery1.timediff_days_after > 3 then "FAILED OPPORTUNITY"
      WHEN ((subquery1.timediff_days_before >= 0 AND subquery1.timediff_days_before < 4) OR subquery1.timediff_days_before IS NULL or subquery1.timediff_days_before <=-15) and subquery1.timediff_days_after <= 3 and subquery1.timediff_days_after >=0 then "SUCCESSFUL OPPORTUNITY"
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
          from ${env:TMP_db}.cs_calls_and_msa_after_join_${env:CLUSTER}
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

DROP TABLE IF EXISTS ${env:TMP_db}.cs_calls_and_prior_msa_test_v01_${env:CLUSTER} PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.cs_calls_and_prior_msa_test_v02_${env:CLUSTER} PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.cs_calls_and_prior_msa_test_v1_${env:CLUSTER} PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.cs_pruned_prior_complete_${env:CLUSTER} PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.cs_calls_and_msa_after_join_${env:CLUSTER} PURGE;
