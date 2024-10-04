USE ${env:DASP_db};

set mapreduce.input.fileinputformat.split.maxsize=5368709120;
set mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.optimize.sort.dynamic.partition=false;
set hive.exec.dynamic.partition.mode=nonstrict;
set orc.force.positional.evolution=true;
SET hive.merge.tezfiles=true;
set hive.merge.smallfiles.avgsize=2048000000;
set hive.merge.size.per.task=2048000000;

ADD JAR s3://pi-qtm-global-${env:ENVIRONMENT}-artifacts/outgoing/login-data/jars/epoch-1.0-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION epoch_converter AS 'Epoch_To_Denver';

ADD JAR ${env:SCALA_LIB};
ADD JAR s3://pi-global-${env:ENVIRONMENT}-udf-jars/daspscalaudf_2.11-0.1.jar;
CREATE TEMPORARY FUNCTION udf_counter AS 'com.charter.dasp.udf.Counter';
CREATE TEMPORARY FUNCTION udf_group_counter AS 'com.charter.dasp.udf.GroupCounter';
CREATE TEMPORARY FUNCTION udf_aggregator AS 'com.charter.dasp.udf.Aggregator';
CREATE TEMPORARY FUNCTION udf_dig1st_segment_counter AS 'com.charter.dasp.udf.Dig1stSegmentCounter';
CREATE TEMPORARY FUNCTION udf_dig1st_call_counter AS 'com.charter.dasp.udf.Dig1stCallCounter';

drop table if exists asp_digital_call_step_1_${hiveconf:execid}_${hiveconf:stepid} PURGE;
create table asp_digital_call_step_1_${hiveconf:execid}_${hiveconf:stepid}
AS
SELECT encrypted_account_key_256,
       encrypted_account_number_256,
       call_inbound_key,
       segment_id,
       truck_roll_flag,
       call_handled_flag,
       call_owner,
       call_id,
       segment_status_disposition,
       epoch_converter(call_start_timestamp_utc, 'America/Denver') AS call_start_date_denver,
       epoch_converter(call_end_timestamp_utc, 'America/Denver') AS call_end_date_denver,
       lower(trim(cause_description)) as cause_description,
       lower(trim(issue_description)) as issue_description,
       agent_mso AS agent_mso,
       segment_start_timestamp_utc,
       segment_duration_seconds,
       call_start_timestamp_utc,
       CASE
           WHEN call_handled_flag=1 and call_owner='Customer Operations' AND segment_status_disposition='Answered by an agent'
           THEN 1
           ELSE 0
       END AS call_answered_flag
  from ${env:ENVIRONMENT}.atom_cs_call_care_data_3
  where call_end_date_utc between '${hiveconf:START_DATE_DENVER}' and DATE_ADD('${hiveconf:END_DATE_DENVER}',1)
;

drop table if exists asp_digital_call_step_2_${hiveconf:execid}_${hiveconf:stepid} PURGE;
create table asp_digital_call_step_2_${hiveconf:execid}_${hiveconf:stepid}
AS
SELECT *,
       CAST(NULL as STRING) AS cause_group,
       CAST(NULL as STRING) AS customer_journey
 from asp_digital_call_step_1_${hiveconf:execid}_${hiveconf:stepid}
WHERE call_end_date_denver between '${hiveconf:START_DATE_DENVER}' and DATE_ADD('${hiveconf:END_DATE_DENVER}',1)
  AND call_start_date_denver between '${hiveconf:START_DATE_DENVER}' and '${hiveconf:END_DATE_DENVER}'
  AND encrypted_account_number_256 is not NULL
  and encrypted_account_number_256 not in ('7TTmD9UkKwdYsmcWS5tPUQ==')
;

drop table if exists asp_digital_call_step_3_${hiveconf:execid}_${hiveconf:stepid} PURGE;
create table asp_digital_call_step_3_${hiveconf:execid}_${hiveconf:stepid}
AS
SELECT a.*,
       b.legacy_company as call_mso
from asp_digital_call_step_2_${hiveconf:execid}_${hiveconf:stepid} a
LEFT join asp_digital_atom_account_${hiveconf:execid}_${hiveconf:stepid} b
on a.encrypted_account_key_256 = b.encrypted_account_key_256
and a.call_end_date_denver = b.partition_date_denver;

drop table if exists asp_digital_call_step_4_${hiveconf:execid}_${hiveconf:stepid} PURGE;
create table asp_digital_call_step_4_${hiveconf:execid}_${hiveconf:stepid}
AS
SELECT c.*,
       IF (c.call_start_timestamp_utc-p.received_timestamp between 0 AND 86400000 AND
          p.account_number_aes_256 is not NULL, 1, 0) AS had_digital_visit,
       CASE
          WHEN (c.call_start_timestamp_utc-p.received_timestamp between 0 AND 86400000 AND
            c.customer_journey=p.customer_journey AND p.account_number_aes_256 is not NULL)
            THEN p.customer_journey
            ELSE NULL
       END AS journey_visit
from asp_digital_call_step_3_${hiveconf:execid}_${hiveconf:stepid} c
LEFT JOIN asp_digital_portals_for_call_${hiveconf:execid}_${hiveconf:stepid} p
ON (p.account_number_aes_256=c.encrypted_account_number_256
AND p.visit_mso=c.call_mso)
;

drop table if exists asp_digital_call_step_5_${hiveconf:execid}_${hiveconf:stepid} PURGE;
create table asp_digital_call_step_5_${hiveconf:execid}_${hiveconf:stepid}
AS
SELECT segment_id,
       call_inbound_key,
       call_id,
       encrypted_account_key_256,
       MAX(truck_roll_flag) as truck_roll_flag,
       MAX(call_end_date_denver) as call_end_date_denver,
       MAX(cause_group) as cause_group,
       MAX(cause_description) as cause_description,
       MAX(customer_journey) as customer_journey,
       max(call_mso) as call_mso,
       MAX(segment_start_timestamp_utc) as segment_start_timestamp_utc,
       max(segment_duration_seconds) as segment_duration_seconds,
       MAX(had_digital_visit) as had_digital_visit,
       MAX(call_start_timestamp_utc) as call_start_timestamp_utc,
       udf_counter(collect_list(journey_visit)) as digital_first_journeys
FROM asp_digital_call_step_4_${hiveconf:execid}_${hiveconf:stepid}
group by segment_id,
         call_inbound_key,
         call_id,
         encrypted_account_key_256
;

drop table if exists asp_digital_call_step_6_${hiveconf:execid}_${hiveconf:stepid} PURGE;
create table asp_digital_call_step_6_${hiveconf:execid}_${hiveconf:stepid}
AS
SELECT encrypted_account_key_256,
       call_end_date_denver,
       count(distinct call_inbound_key) as call_counts,
       count(distinct (case when had_digital_visit=1 then call_inbound_key end)) as digital_first_call_counts,
       max(truck_roll_flag) as truck_roll_flag,
       max(call_mso) as call_mso,
       max(cause_group) as cause_group,
       collect_list(cause_group) as cause_list,
       collect_list(cause_description) as description_list,
       count(1) as segment_counts,
       COLLECT_LIST(customer_journey) as customer_journey_list,
       COLLECT_LIST(digital_first_journeys) as digital_first_journeys_list,
       COLLECT_LIST(key_journey_map) as key_journey_map_list,
       COLLECT_LIST(key_dig1st_journey_map) as key_dig1st_journey_map_list
from
(select *,
        concat(customer_journey, ':', call_inbound_key) key_journey_map,
        concat(call_inbound_key, ':', digital_first_journeys) key_dig1st_journey_map
from asp_digital_call_step_5_${hiveconf:execid}_${hiveconf:stepid}) a
group by encrypted_account_key_256, call_end_date_denver;

drop table if exists asp_digital_call_${hiveconf:execid}_${hiveconf:stepid} PURGE;
create table asp_digital_call_${hiveconf:execid}_${hiveconf:stepid}
AS
select
   encrypted_account_key_256 as account_key_callfeatures,
   call_end_date_denver as call_end_date_denver_callfeatures,
   call_counts,
   digital_first_call_counts,
   truck_roll_flag,
   call_mso,
   cause_group,
   cause_list,
   description_list,
   segment_counts,
   customer_journey_list,
   udf_counter(customer_journey_list) as customer_journey_segments_map,
   udf_group_counter(key_journey_map_list) as customer_journey_calls_map,
   udf_aggregator(digital_first_journeys_list) as digital_first_journey_action_map,
   udf_dig1st_segment_counter(digital_first_journeys_list) as digital_first_journey_segment_map,
   udf_dig1st_call_counter(key_dig1st_journey_map_list) AS digital_first_journey_call_map
from asp_digital_call_step_6_${hiveconf:execid}_${hiveconf:stepid}
;
