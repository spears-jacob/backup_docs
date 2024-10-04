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

----------------------- Pull raw data from CQE for time range ------------------------------
    ------------------------pull buyflow data from main CQE ------------------------
drop table if exists asp_digital_portals_step_0_${hiveconf:execid}_${hiveconf:stepid} PURGE;
create table asp_digital_portals_step_0_${hiveconf:execid}_${hiveconf:stepid}
AS
select
      visit__application_details__application_name as application_name,
      max(visit__account__details__mso) OVER (PARTITION BY partition_date_utc, visit__application_details__application_name, visit__visit_id) as max_visit_mso,
      visit__account__enc_account_billing_id as acct_number_aes_256,
      visit__visit_id as visit_id,
      concat(visit__visit_id,'-',visit__account__enc_account_billing_id) as unique_visit_key,
      message__sequence_number as message_sequence_number,
      received__timestamp as received_timestamp,
      state__view__current_page__page_name as state_view_current_page_page_name,
      visit__application_statistics__days_since_first_use as visit_application_statistics_days_since_first_use,
      visit__application_statistics__days_since_last_use as visit_application_statistics_days_since_last_use,
      concat_ws(',', state__view__current_page__user_journey) as customer_journey,
      concat_ws(',', state__view__current_page__user_sub_journey) as customer_sub_journey,
      epoch_converter(received__timestamp, 'America/Denver') as received_date_denver,
      partition_date_utc,
      max(visit__account__enc_account_billing_id) OVER (PARTITION BY partition_date_utc, visit__application_details__application_name, visit__visit_id) as account_number_aes_256,
      0 as flag_enriched,
      visit__application_details__application_type,
      visit__device__model
 from ${env:ENVIRONMENT}.core_quantum_events
where partition_date_utc between DATE_ADD('${hiveconf:START_DATE_DENVER}',-2) and DATE_ADD('${hiveconf:END_DATE_DENVER}',1)
  and visit__application_details__application_name in ('BulkMDU','OneApp','SpecU')
  AND visit__application_details__application_type in ('Roku','OVP','iOS','Web','Android','FireTV','AppleTV')
  AND NOT (visit__application_details__application_type='AppleTV' and visit__device__model='Simulator')
  AND (message__context in ('stream2','buyFlow') AND state__view__current_page__page_name in ('signUp','stream2SignUp'))
;

SELECT partition_date_utc
  , count(1) as total_rows
  , count(distinct visit_id) as visits
  , count(distinct account_number_aes_256) as accounts
  , 'asp_digital_portals_step_0_${hiveconf:execid}_${hiveconf:stepid}' as source_table
FROM asp_digital_portals_step_0_${hiveconf:execid}_${hiveconf:stepid}
GROUP BY partition_date_utc
LIMIT 10;

    ------------------------pull other data from sspp CQE ------------------------
drop table if exists asp_digital_portals_step_0_sspp_${hiveconf:execid}_${hiveconf:stepid} PURGE;
create table asp_digital_portals_step_0_sspp_${hiveconf:execid}_${hiveconf:stepid}
AS
select
      visit__application_details__application_name as application_name,
      max(visit__account__details__mso) OVER (PARTITION BY partition_date_utc, visit__application_details__application_name, visit__visit_id) as max_visit_mso,
      visit__account__enc_account_number as acct_number_aes_256,
      visit__visit_id as visit_id,
      concat(visit__visit_id,'-',visit__account__enc_account_number) as unique_visit_key,
      message__sequence_number as message_sequence_number,
      received__timestamp as received_timestamp,
      state__view__current_page__page_name as state_view_current_page_page_name,
      visit__application_statistics__days_since_first_use as visit_application_statistics_days_since_first_use,
      visit__application_statistics__days_since_last_use as visit_application_statistics_days_since_last_use,
      concat_ws(',', state__view__current_page__user_journey) as customer_journey,
      concat_ws(',', state__view__current_page__user_sub_journey) as customer_sub_journey,
      epoch_converter(received__timestamp, 'America/Denver') as received_date_denver,
      partition_date_utc,
      max(visit__account__enc_account_number) OVER (PARTITION BY partition_date_utc, visit__application_details__application_name, visit__visit_id) as account_number_aes_256,
      0 as flag_enriched
 from ${env:ENVIRONMENT}.core_quantum_events_sspp
where partition_date_utc between DATE_ADD('${hiveconf:START_DATE_DENVER}',-2) and DATE_ADD('${hiveconf:END_DATE_DENVER}',1)
  and visit__application_details__application_name in ('SpecNet','SMB','MySpectrum','SpecMobile','SelfInstall')
;

SELECT partition_date_utc
  , count(1) as total_rows
  , count(distinct visit_id) as total_visits
  , count(distinct account_number_aes_256) as total_accounts
  , 'asp_digital_portals_step_0_sspp_${hiveconf:execid}_${hiveconf:stepid}' as source_table
FROM asp_digital_portals_step_0_sspp_${hiveconf:execid}_${hiveconf:stepid}
GROUP BY partition_date_utc
LIMIT 10;

----------------------- Enrich and reformat raw CQE data ------------------------------
    ---------sort out data from step 0 that does NOT need enrichment ---------
drop table if exists asp_digital_portals_step_1_${hiveconf:execid}_${hiveconf:stepid} PURGE;
create table asp_digital_portals_step_1_${hiveconf:execid}_${hiveconf:stepid}
AS
select
      application_name,
      CASE
         WHEN (max_visit_mso='TWC' or max_visit_mso='"TWC"') THEN 'TWC'
         WHEN (max_visit_mso= 'BH' or max_visit_mso='"BHN"' OR max_visit_mso='BHN') THEN 'BHN'
         WHEN (max_visit_mso= 'CHARTER' or max_visit_mso='"CHTR"' OR max_visit_mso='CHTR') THEN 'CHR'
         WHEN (max_visit_mso= 'NONE' or max_visit_mso='UNKNOWN') THEN NULL
         ELSE max_visit_mso
      END AS visit_mso,
      acct_number_aes_256,
      visit_id,
      unique_visit_key,
      message_sequence_number,
      received_timestamp,
      state_view_current_page_page_name,
      visit_application_statistics_days_since_first_use,
      visit_application_statistics_days_since_last_use,
      customer_journey,
      customer_sub_journey,
      received_date_denver,
      partition_date_utc,
      account_number_aes_256,
      flag_enriched
 from asp_digital_portals_step_0_${hiveconf:execid}_${hiveconf:stepid}
where account_number_aes_256 is not null
  AND       CASE
           WHEN (max_visit_mso='TWC' or max_visit_mso='"TWC"') THEN 'TWC'
           WHEN (max_visit_mso= 'BH' or max_visit_mso='"BHN"' OR max_visit_mso='BHN') THEN 'BHN'
           WHEN (max_visit_mso= 'CHARTER' or max_visit_mso='"CHTR"' OR max_visit_mso='CHTR') THEN 'CHR'
           WHEN (max_visit_mso= 'NONE' or max_visit_mso='UNKNOWN') THEN NULL
           ELSE max_visit_mso
        END IS NOT NULL
;

insert into asp_digital_portals_step_1_${hiveconf:execid}_${hiveconf:stepid}
select
      application_name,
      CASE
         WHEN (max_visit_mso='TWC' or max_visit_mso='"TWC"') THEN 'TWC'
         WHEN (max_visit_mso= 'BH' or max_visit_mso='"BHN"' OR max_visit_mso='BHN') THEN 'BHN'
         WHEN (max_visit_mso= 'CHARTER' or max_visit_mso='"CHTR"' OR max_visit_mso='CHTR') THEN 'CHR'
         WHEN (max_visit_mso= 'NONE' or max_visit_mso='UNKNOWN') THEN NULL
         ELSE max_visit_mso
      END AS visit_mso,
      acct_number_aes_256,
      visit_id,
      unique_visit_key,
      message_sequence_number,
      received_timestamp,
      state_view_current_page_page_name,
      visit_application_statistics_days_since_first_use,
      visit_application_statistics_days_since_last_use,
      customer_journey,
      customer_sub_journey,
      received_date_denver,
      partition_date_utc,
      account_number_aes_256,
      flag_enriched
 from asp_digital_portals_step_0_sspp_${hiveconf:execid}_${hiveconf:stepid}
where account_number_aes_256 is not null
 AND       CASE
          WHEN (max_visit_mso='TWC' or max_visit_mso='"TWC"') THEN 'TWC'
          WHEN (max_visit_mso= 'BH' or max_visit_mso='"BHN"' OR max_visit_mso='BHN') THEN 'BHN'
          WHEN (max_visit_mso= 'CHARTER' or max_visit_mso='"CHTR"' OR max_visit_mso='CHTR') THEN 'CHR'
          WHEN (max_visit_mso= 'NONE' or max_visit_mso='UNKNOWN') THEN NULL
          ELSE max_visit_mso
       END IS NOT NULL
;

SELECT partition_date_utc
  , count(1) as rows_not_enriched
  , count(distinct visit_id) as visits_not_enriched
  , count(distinct account_number_aes_256) as accounts_not_enriched
  , 'asp_digital_portals_step_1_${hiveconf:execid}_${hiveconf:stepid}' as source_table
FROM asp_digital_portals_step_1_${hiveconf:execid}_${hiveconf:stepid}
GROUP BY partition_date_utc
LIMIT 10;

    ---------sort out data from step 0 that DOES need enrichment ---------
drop table if exists asp_digital_portals_step_1_cqe_${hiveconf:execid}_${hiveconf:stepid} PURGE;
create table asp_digital_portals_step_1_cqe_${hiveconf:execid}_${hiveconf:stepid}
AS
select
       application_name,
       CASE
          WHEN (max_visit_mso='TWC' or max_visit_mso='"TWC"') THEN 'TWC'
          WHEN (max_visit_mso= 'BH' or max_visit_mso='"BHN"' OR max_visit_mso='BHN') THEN 'BHN'
          WHEN (max_visit_mso= 'CHARTER' or max_visit_mso='"CHTR"' OR max_visit_mso='CHTR') THEN 'CHR'
          WHEN (max_visit_mso= 'NONE' or max_visit_mso='UNKNOWN') THEN NULL
          ELSE max_visit_mso
       END AS visit_mso,
       acct_number_aes_256,
       visit_id,
       unique_visit_key,
       message_sequence_number,
       received_timestamp,
       state_view_current_page_page_name,
       visit_application_statistics_days_since_first_use,
       visit_application_statistics_days_since_last_use,
       customer_journey,
       customer_sub_journey,
       received_date_denver,
       partition_date_utc,
       account_number_aes_256,
       flag_enriched
  from asp_digital_portals_step_0_${hiveconf:execid}_${hiveconf:stepid}
 where account_number_aes_256 is null
  OR       CASE
           WHEN (max_visit_mso='TWC' or max_visit_mso='"TWC"') THEN 'TWC'
           WHEN (max_visit_mso= 'BH' or max_visit_mso='"BHN"' OR max_visit_mso='BHN') THEN 'BHN'
           WHEN (max_visit_mso= 'CHARTER' or max_visit_mso='"CHTR"' OR max_visit_mso='CHTR') THEN 'CHR'
           WHEN (max_visit_mso= 'NONE' or max_visit_mso='UNKNOWN') THEN NULL
           ELSE max_visit_mso
        END IS NULL
 ;

insert into asp_digital_portals_step_1_cqe_${hiveconf:execid}_${hiveconf:stepid}
select
       application_name,
       CASE
          WHEN (max_visit_mso='TWC' or max_visit_mso='"TWC"') THEN 'TWC'
          WHEN (max_visit_mso= 'BH' or max_visit_mso='"BHN"' OR max_visit_mso='BHN') THEN 'BHN'
          WHEN (max_visit_mso= 'CHARTER' or max_visit_mso='"CHTR"' OR max_visit_mso='CHTR') THEN 'CHR'
          WHEN (max_visit_mso= 'NONE' or max_visit_mso='UNKNOWN') THEN NULL
          ELSE max_visit_mso
       END AS visit_mso,
       acct_number_aes_256,
       visit_id,
       unique_visit_key,
       message_sequence_number,
       received_timestamp,
       state_view_current_page_page_name,
       visit_application_statistics_days_since_first_use,
       visit_application_statistics_days_since_last_use,
       customer_journey,
       customer_sub_journey,
       received_date_denver,
       partition_date_utc,
       account_number_aes_256,
       flag_enriched
  from asp_digital_portals_step_0_sspp_${hiveconf:execid}_${hiveconf:stepid}
 where account_number_aes_256 is null
  OR       CASE
           WHEN (max_visit_mso='TWC' or max_visit_mso='"TWC"') THEN 'TWC'
           WHEN (max_visit_mso= 'BH' or max_visit_mso='"BHN"' OR max_visit_mso='BHN') THEN 'BHN'
           WHEN (max_visit_mso= 'CHARTER' or max_visit_mso='"CHTR"' OR max_visit_mso='CHTR') THEN 'CHR'
           WHEN (max_visit_mso= 'NONE' or max_visit_mso='UNKNOWN') THEN NULL
           ELSE max_visit_mso
        END IS NULL
 ;

 SELECT partition_date_utc
   , count(1) as rows_need_enriching
   , count(distinct visit_id) as visits_need_enriching
   , 'asp_digital_portals_step_1_cqe_${hiveconf:execid}_${hiveconf:stepid}' as source_table
 FROM asp_digital_portals_step_1_cqe_${hiveconf:execid}_${hiveconf:stepid}
 GROUP BY partition_date_utc
 LIMIT 10;

    ---------Pull in BTM visit-id-lookup table for relevant dates ---------
drop table if exists asp_digital_portals_step_1_btm_${hiveconf:execid}_${hiveconf:stepid} PURGE;
create table asp_digital_portals_step_1_btm_${hiveconf:execid}_${hiveconf:stepid}
AS
SELECT *
  FROM `${env:BTM}`
 where partition_date_utc between DATE_ADD('${hiveconf:START_DATE_DENVER}',-2) and DATE_ADD('${hiveconf:END_DATE_DENVER}',1);

    --------------Pull in account atom for relevant dates ----------------
drop table if exists asp_digital_portals_step_1_atom_${hiveconf:execid}_${hiveconf:stepid} PURGE;
create table asp_digital_portals_step_1_atom_${hiveconf:execid}_${hiveconf:stepid}
AS
SELECT distinct
        encrypted_account_key_256,
        partition_date_denver,
        legacy_company
 FROM red_atom_snapshot_accounts_v
where partition_date_denver between DATE_ADD('${hiveconf:START_DATE_DENVER}',-2) and DATE_ADD('${hiveconf:END_DATE_DENVER}',1);

    ---------combine cqe data that needs enriching with BTM and atom ---------
drop table if exists asp_digital_portals_step_1_combine_${hiveconf:execid}_${hiveconf:stepid} PURGE;
create table asp_digital_portals_step_1_combine_${hiveconf:execid}_${hiveconf:stepid}
AS
SELECT /*+ MAPJOIN(b), MAPJOIN(c) */
       distinct
       a.application_name,
       COALESCE(a.visit_mso,c.legacy_company) as visit_mso,
       a.acct_number_aes_256,
       a.visit_id,
       concat(a.visit_id,'-',COALESCE(a.account_number_aes_256,b.encrypted_account_number)) as unique_visit_key,
       a.message_sequence_number,
       a.received_timestamp,
       a.state_view_current_page_page_name,
       a.visit_application_statistics_days_since_first_use,
       a.visit_application_statistics_days_since_last_use,
       a.customer_journey,
       a.customer_sub_journey,
       a.received_date_denver,
       a.partition_date_utc,
       COALESCE(a.account_number_aes_256,b.encrypted_account_number) as account_number_aes_256,
       case
           when a.account_number_aes_256 is null and b.encrypted_account_number is not null then 1
           else 0
       END as flag_enriched
 from asp_digital_portals_step_1_cqe_${hiveconf:execid}_${hiveconf:stepid} a
 left join asp_digital_portals_step_1_btm_${hiveconf:execid}_${hiveconf:stepid} b
   on a.visit_id = b.visit_id
  and a.partition_date_utc=b.partition_date_utc
 LEFT JOIN asp_digital_portals_step_1_atom_${hiveconf:execid}_${hiveconf:stepid} c
   on b.encrypted_account_key_256=c.encrypted_account_key_256
  and b.partition_date_utc=c.partition_date_denver;

SELECT partition_date_utc
  , count(1) as rows_enriched
  , count(distinct visit_id) as visits_need_enriched
  , count(distinct account_number_aes_256) as accounts_enriched
  , 'asp_digital_portals_step_1_combine_${hiveconf:execid}_${hiveconf:stepid}' as source_table
FROM asp_digital_portals_step_1_combine_${hiveconf:execid}_${hiveconf:stepid}
GROUP BY partition_date_utc
LIMIT 10;

INSERT INTO asp_digital_portals_step_1_${hiveconf:execid}_${hiveconf:stepid}
select *
  from asp_digital_portals_step_1_combine_${hiveconf:execid}_${hiveconf:stepid};

--------------------- data needed to join to calls in 3:calls.hql---------------------------
drop table if exists asp_digital_portals_for_call_${hiveconf:execid}_${hiveconf:stepid} PURGE;
create table asp_digital_portals_for_call_${hiveconf:execid}_${hiveconf:stepid}
AS
select application_name,
       account_number_aes_256,
       visit_id,
       state_view_current_page_page_name,
       customer_journey,
       message_sequence_number,
       received_timestamp,
       visit_mso,
       received_date_denver
  from asp_digital_portals_step_1_${hiveconf:execid}_${hiveconf:stepid}
 where application_name in ('SpecNet','SMB','MySpectrum','SpecMobile','SelfInstall','BulkMDU','OneApp','SpecU')
   and received_date_denver between DATE_ADD('${hiveconf:START_DATE_DENVER}',-1) and '${hiveconf:END_DATE_DENVER}'
;

-----Now that we know all the values we can, do a final filter of the data -----
drop table if exists asp_digital_portals_step_2_${hiveconf:execid}_${hiveconf:stepid} PURGE;
create table asp_digital_portals_step_2_${hiveconf:execid}_${hiveconf:stepid}
AS
select application_name,
       account_number_aes_256,
       visit_id,
       state_view_current_page_page_name,
       customer_journey,
       message_sequence_number,
       received_timestamp,
       visit_mso,
       received_date_denver
  from asp_digital_portals_step_1_${hiveconf:execid}_${hiveconf:stepid}
 where visit_mso in ('CHR','TWC','BHN')
   AND received_date_denver between '${hiveconf:START_DATE_DENVER}' and '${hiveconf:END_DATE_DENVER}'
   and account_number_aes_256 is not NULL
   and account_number_aes_256 not in ("zmJZ8slelYaSloUDytUH4w==", "7FbKtybuOWU4/Q0SRInbHA==");

SELECT received_date_denver
   , count(1) as total_rows
   , count(distinct visit_id) as total_visits
   , count(distinct account_number_aes_256) as total_accounts
   , 'asp_digital_portals_step_2_${hiveconf:execid}_${hiveconf:stepid}' as source_table
FROM asp_digital_portals_step_2_${hiveconf:execid}_${hiveconf:stepid}
GROUP BY received_date_denver
LIMIT 10;


----------------------- Make a table for each platform ------------------------------
drop table if exists asp_digital_portals_spec_${hiveconf:execid}_${hiveconf:stepid} PURGE;
create table asp_digital_portals_spec_${hiveconf:execid}_${hiveconf:stepid}
AS
SELECT account_number_aes_256 as account_number_aes_256_spec,
       received_date_denver as received_date_denver_spec,
       visit_mso as visit_mso_spec,
       count(distinct visit_id) as distinct_visits_spec,
       count(1) as action_count_spec,
       sum(if(lower(state_view_current_page_page_name) RLIKE "^support.*$", 1, 0)) as support_count_spec,
       collect_list(state_view_current_page_page_name) as page_name_list_spec,
       udf_counter(collect_list(customer_journey)) as customer_journey_spec,
       udf_group_counter(collect_list(concat(customer_journey, ':', visit_id))) as customer_visit_journey_spec,
       collect_list(concat(customer_journey, ':', visit_id)) as key_journey_map_list_spec
FROM asp_digital_portals_step_2_${hiveconf:execid}_${hiveconf:stepid}
WHERE application_name='SpecNet'
group by account_number_aes_256,
         received_date_denver,
         visit_mso;

drop table if exists asp_digital_portals_msa_${hiveconf:execid}_${hiveconf:stepid} PURGE;
create table asp_digital_portals_msa_${hiveconf:execid}_${hiveconf:stepid}
AS
SELECT account_number_aes_256 as account_number_aes_256_msa,
      received_date_denver as received_date_denver_msa,
      visit_mso as visit_mso_msa,
      count(distinct visit_id) as distinct_visits_msa,
      count(1) as action_count_msa,
      udf_counter(collect_list(customer_journey)) as customer_journey_msa,
      udf_group_counter(collect_list(concat(customer_journey, ':', visit_id))) as customer_visit_journey_msa,
      collect_list(concat(customer_journey, ':', visit_id)) as key_journey_map_list_msa
FROM asp_digital_portals_step_2_${hiveconf:execid}_${hiveconf:stepid}
WHERE application_name='MySpectrum'
group by account_number_aes_256,
        received_date_denver,
        visit_mso;

drop table if exists asp_digital_portals_smb_${hiveconf:execid}_${hiveconf:stepid} PURGE;
create table asp_digital_portals_smb_${hiveconf:execid}_${hiveconf:stepid}
AS
SELECT account_number_aes_256 as account_number_aes_256_smb,
       received_date_denver as received_date_denver_smb,
       visit_mso as visit_mso_smb,
       count(distinct visit_id) as distinct_visits_smb,
       count(1) as action_count_smb,
       sum(if(lower(state_view_current_page_page_name) RLIKE "^support.*$", 1, 0)) as support_count_smb,
       collect_list(state_view_current_page_page_name) as page_name_list_smb,
       udf_counter(collect_list(customer_journey)) as customer_journey_smb,
       udf_group_counter(collect_list(concat(customer_journey, ':', visit_id))) as customer_visit_journey_smb,
       collect_list(concat(customer_journey, ':', visit_id)) as key_journey_map_list_smb
FROM asp_digital_portals_step_2_${hiveconf:execid}_${hiveconf:stepid}
WHERE application_name='SMB'
group by account_number_aes_256,
         received_date_denver,
         visit_mso;

 drop table if exists asp_digital_portals_specmobile_${hiveconf:execid}_${hiveconf:stepid} PURGE;
 create table asp_digital_portals_specmobile_${hiveconf:execid}_${hiveconf:stepid}
 AS
 SELECT account_number_aes_256 as account_number_aes_256_specmobile,
        received_date_denver as received_date_denver_specmobile,
        visit_mso as visit_mso_specmobile,
        count(distinct visit_id) as distinct_visits_specmobile,
        count(1) as action_count_specmobile
 FROM asp_digital_portals_step_2_${hiveconf:execid}_${hiveconf:stepid}
 WHERE application_name='SpecMobile'
 group by account_number_aes_256,
          received_date_denver,
          visit_mso;

drop table if exists asp_digital_portals_selfinstall_${hiveconf:execid}_${hiveconf:stepid} PURGE;
create table asp_digital_portals_selfinstall_${hiveconf:execid}_${hiveconf:stepid}
AS
SELECT account_number_aes_256 as account_number_aes_256_selfinstall,
       received_date_denver as received_date_denver_selfinstall,
       visit_mso as visit_mso_selfinstall,
       count(distinct visit_id) as distinct_visits_selfinstall,
       count(1) as action_count_selfinstall,
       sum(if(lower(state_view_current_page_page_name) RLIKE "^support.*$", 1, 0)) as support_count_selfinstall,
       collect_list(state_view_current_page_page_name) as page_name_list_selfinstall,
       udf_counter(collect_list(customer_journey)) as customer_journey_selfinstall,
       udf_group_counter(collect_list(concat(customer_journey, ':', visit_id))) as customer_visit_journey_selfinstall,
       collect_list(concat(customer_journey, ':', visit_id)) as key_journey_map_list_selfinstall
FROM asp_digital_portals_step_2_${hiveconf:execid}_${hiveconf:stepid}
WHERE application_name='SelfInstall'
group by account_number_aes_256,
         received_date_denver,
         visit_mso;

drop table if exists asp_digital_portals_buyflow_${hiveconf:execid}_${hiveconf:stepid} PURGE;
create table asp_digital_portals_buyflow_${hiveconf:execid}_${hiveconf:stepid}
AS
SELECT account_number_aes_256 as account_number_aes_256_buyflow,
       received_date_denver as received_date_denver_buyflow,
       visit_mso as visit_mso_buyflow,
       count(distinct visit_id) as distinct_visits_buyflow,
       count(1) as action_count_buyflow,
       sum(if(lower(state_view_current_page_page_name) RLIKE "^support.*$", 1, 0)) as support_count_buyflow,
       collect_list(state_view_current_page_page_name) as page_name_list_buyflow,
       udf_counter(collect_list(customer_journey)) as customer_journey_buyflow,
       udf_group_counter(collect_list(concat(customer_journey, ':', visit_id))) as customer_visit_journey_buyflow,
       collect_list(concat(customer_journey, ':', visit_id)) as key_journey_map_list_buyflow
FROM asp_digital_portals_step_2_${hiveconf:execid}_${hiveconf:stepid}
WHERE application_name in ('BulkMDU','OneApp','SpecU')
group by account_number_aes_256,
         received_date_denver,
         visit_mso;
