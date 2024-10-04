USE ${env:DASP_db};

ADD JAR s3://pi-qtm-global-${env:ENVIRONMENT}-artifacts/outgoing/login-data/jars/epoch-1.0-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION epoch_converter AS 'Epoch_To_Denver';

drop table IF EXISTS ${env:TMP_db}.digital_${hiveconf:execid}_${hiveconf:stepid} PURGE;
create table ${env:TMP_db}.digital_${hiveconf:execid}_${hiveconf:stepid} AS
select partition_date_denver,
       name as digital_name,
       value as digital_value
  FROM
       (SELECT partition_date_denver,
               sum(call_counts) as digital_total_calls,
               SUM(distinct_Visits_msa) as digital_total_msa,
               SUM(distinct_Visits_spec) as digital_total_spec,
               SUM(distinct_visits_smb) as digital_total_smb,
               SUM(distinct_visits_specmobile) as digital_total_mobile,
               SUM(distinct_visits_selfinstall) as digital_total_selfinstall
          from asp_digital_adoption_daily
         where partition_date_denver between '${hiveconf:START_DATE_DENVER}' and '${hiveconf:END_DATE_DENVER}'
         group by partition_date_denver) a
LATERAL VIEW explode(map('calls',digital_total_calls,'msa',digital_total_msa,'spec',digital_total_spec,'smb',digital_total_smb,'mobile',digital_total_mobile,'selfinstall',digital_total_selfinstall)) b as name, value;
;

drop table IF EXISTS ${env:TMP_db}.atom_data_${hiveconf:execid}_${hiveconf:stepid} PURGE;
CREATE table ${env:TMP_db}.atom_data_${hiveconf:execid}_${hiveconf:stepid} as
SELECT distinct
       encrypted_account_key_256,
       partition_date_denver as acct_date,
       legacy_company as account_mso,
       encrypted_padded_account_number_256 as atom_acct
  from ${env:ACCOUNTATOM}
 where partition_date_denver between '${hiveconf:START_DATE_DENVER}' and '${hiveconf:END_DATE_DENVER}'
   AND legacy_company in ('CHR','TWC','BHN')
   aND extract_source = 'P270'
   AND account_status='CONNECTED'
   AND account_type='SUBSCRIBER'
;

drop table IF EXISTS ${env:TMP_db}.call_data_${hiveconf:execid}_${hiveconf:stepid} PURGE;
create table ${env:TMP_db}.call_data_${hiveconf:execid}_${hiveconf:stepid} as
SELECT distinct
       to_date(from_utc_timestamp(call_end_datetime_utc, 'America/Denver')) as call_end_denver,
       encrypted_account_key_256 as call_key,
       call_inbound_key,
       call_end_date_east as call_date,
       account_agent_mso as call_mso,
       encrypted_padded_account_number_256 as call_acct
  from ${env:ENVIRONMENT}.atom_cs_call_care_data_3
 where call_end_date_east between '${hiveconf:START_DATE_DENVER}' and '${hiveconf:END_DATE_DENVER}'
   AND encrypted_padded_account_number_256 is not NULL
   and encrypted_account_number_256 != "7TTmD9UkKwdYsmcWS5tPUQ=="
;

drop table IF EXISTS ${env:TMP_db}.call_summary_${hiveconf:execid}_${hiveconf:stepid} PURGE;
create table ${env:TMP_db}.call_summary_${hiveconf:execid}_${hiveconf:stepid} as
select acct_date,
       count(distinct call_inbound_key) as total_calls
  FROM
       (SELECT *
          from ${env:TMP_db}.atom_data_${hiveconf:execid}_${hiveconf:stepid} a
          LEFT join ${env:TMP_db}.call_data_${hiveconf:execid}_${hiveconf:stepid} b
            on a.encrypted_account_key_256=b.call_key
           and a.acct_date=b.call_end_denver) c
 group by acct_date
;

INSERT overwrite TABLE asp_digital_adoption_daily_check PARTITION (run_date)
select
       partition_date_denver,
       'total_calls',
       digital_value as digital_total_calls,
       total_calls,
       (total_calls-digital_value) as diff,
       100*abs(total_calls-digital_value)/((total_calls+digital_value)/2) as percent_diff,
       current_date as run_date
  from ${env:TMP_db}.digital_${hiveconf:execid}_${hiveconf:stepid}
  FULL OUTER join ${env:TMP_db}.call_summary_${hiveconf:execid}_${hiveconf:stepid}
 where acct_date = partition_date_denver
   and digital_name='calls'
;

drop table IF EXISTS ${env:TMP_db}.cqe_step0_${hiveconf:execid}_${hiveconf:stepid} PURGE;
create table ${env:TMP_db}.cqe_step0_${hiveconf:execid}_${hiveconf:stepid} As
SELECT max(visit__account__enc_account_number) OVER (PARTITION BY partition_date_utc, application_name, visit_id) as account_number_aes_256,
       max(visit_mso) OVER (PARTITION BY partition_date_utc, application_name, visit_id) as cqe_mso,
       application_name,
       denver_date,
       partition_date_utc,
       visit_id
FROM
(select distinct
       visit__account__enc_account_number,
       CASE
          WHEN (visit__account__details__mso='TWC' or visit__account__details__mso='"TWC"') THEN 'TWC'
          WHEN (visit__account__details__mso= 'BH' or visit__account__details__mso='"BHN"' OR visit__account__details__mso='BHN') THEN 'BHN'
          WHEN (visit__account__details__mso= 'CHARTER' or visit__account__details__mso='"CHTR"' OR visit__account__details__mso='CHTR') THEN 'CHR'
          WHEN (visit__account__details__mso= 'NONE' or visit__account__details__mso='UNKNOWN') THEN NULL
          ELSE visit__account__details__mso
       END AS visit_mso,
       CASE
          WHEN (visit__application_details__application_name='SMB') THEN 'smb'
          WHEN (visit__application_details__application_name= 'MySpectrum') THEN 'msa'
          WHEN (visit__application_details__application_name= 'SpecNet') THEN 'spec'
          WHEN (visit__application_details__application_name= 'SpecMobile') THEN 'mobile'
          WHEN (visit__application_details__application_name= 'SelfInstall') THEN 'selfinstall'
          ELSE visit__application_details__application_name
       END AS application_name,
       epoch_converter(received__timestamp, 'America/Denver') as denver_date,
       partition_date_utc,
       visit__visit_id as visit_id
  from ${env:ENVIRONMENT}.core_quantum_events_sspp
 where partition_date_utc between DATE_ADD('${hiveconf:START_DATE_DENVER}',-2) and DATE_ADD('${hiveconf:END_DATE_DENVER}',1)
   AND epoch_converter(received__timestamp, 'America/Denver') between '${hiveconf:START_DATE_DENVER}' and '${hiveconf:END_DATE_DENVER}'
   and visit__application_details__application_name in ("MySpectrum", "SMB", "SpecMobile", "SpecNet","SelfInstall")) a
;

drop table if exists ${env:TMP_db}.btm_data_${hiveconf:execid}_${hiveconf:stepid} PURGE;
create table ${env:TMP_db}.btm_data_${hiveconf:execid}_${hiveconf:stepid}
AS
SELECT *
  FROM `${env:BTM}`
 where partition_date_utc between DATE_ADD('${hiveconf:START_DATE_DENVER}',-2) and DATE_ADD('${hiveconf:END_DATE_DENVER}',1)
;

drop table if exists ${env:TMP_db}.atom_mso_${hiveconf:execid}_${hiveconf:stepid};
create table ${env:TMP_db}.atom_mso_${hiveconf:execid}_${hiveconf:stepid}
AS
SELECT distinct
       encrypted_account_key_256,
       partition_date_denver,
       legacy_company
  from ${env:ACCOUNTATOM}
 where partition_date_denver between DATE_ADD('${hiveconf:START_DATE_DENVER}',-2) and DATE_ADD('${hiveconf:END_DATE_DENVER}',1)
;

drop table if exists ${env:TMP_db}.combine_${hiveconf:execid}_${hiveconf:stepid} PURGE;
create table ${env:TMP_db}.combine_${hiveconf:execid}_${hiveconf:stepid}
AS
SELECT /*+ MAPJOIN(b), MAPJOIN(c) */
      distinct
      COALESCE(a.account_number_aes_256,b.encrypted_account_number) as account_number_aes_256,
      COALESCE(a.cqe_mso,c.legacy_company) as cqe_mso,
      a.application_name,
      a.denver_date,
      a.partition_date_utc,
      a.visit_id
 from ${env:TMP_db}.cqe_step0_${hiveconf:execid}_${hiveconf:stepid} a
 left join ${env:TMP_db}.btm_data_${hiveconf:execid}_${hiveconf:stepid} b
   on a.visit_id = b.visit_id
  and a.partition_date_utc=b.partition_date_utc
 LEFT JOIN ${env:TMP_db}.atom_mso_${hiveconf:execid}_${hiveconf:stepid} c
   on b.encrypted_account_key_256=c.encrypted_account_key_256
  and b.partition_date_utc=c.partition_date_denver
;

drop table IF EXISTS ${env:TMP_db}.cqe_data_${hiveconf:execid}_${hiveconf:stepid} PURGE;
create table ${env:TMP_db}.cqe_data_${hiveconf:execid}_${hiveconf:stepid} As
select *
  from ${env:TMP_db}.combine_${hiveconf:execid}_${hiveconf:stepid}
 where account_number_aes_256 is Not Null
   and account_number_aes_256 not in("zmJZ8slelYaSloUDytUH4w==", "7FbKtybuOWU4/Q0SRInbHA==")
   AND cqe_mso in ('CHR','TWC','BHN')
;

drop table IF EXISTS ${env:TMP_db}.cqe_summary_${hiveconf:execid}_${hiveconf:stepid} PURGE;
create table ${env:TMP_db}.cqe_summary_${hiveconf:execid}_${hiveconf:stepid} as
select denver_date,
       application_name,
       count(distinct visit_id) as cqe_value
  from
       (SELECT *
          from ${env:TMP_db}.atom_data_${hiveconf:execid}_${hiveconf:stepid} a
          left join ${env:TMP_db}.cqe_data_${hiveconf:execid}_${hiveconf:stepid} b
            on a.atom_acct = b.account_number_aes_256
           and a.account_mso = b.cqe_mso
           and a.acct_date = b.denver_date) c
 where application_name is not null
 group by denver_date,application_name
;

insert into asp_digital_adoption_daily_check PARTITION (run_date)
select
       partition_date_denver,
       application_name,
       digital_value,
       cqe_value,
       abs(cqe_value-digital_value) as value_diff,
       100*abs(cqe_value-digital_value)/(0.5*(cqe_value+digital_value)) as value_percent_diff,
       current_date as run_date
  from ${env:TMP_db}.digital_${hiveconf:execid}_${hiveconf:stepid} a
  full outer join ${env:TMP_db}.cqe_summary_${hiveconf:execid}_${hiveconf:stepid} b
    on denver_date = partition_date_denver
   and a.digital_name=b.application_name
 where b.application_name is not null
;

DROP TABLE IF EXISTS ${env:TMP_db}.digital_${hiveconf:execid}_${hiveconf:stepid} PURGE;
drop table IF EXISTS ${env:TMP_db}.atom_data_${hiveconf:execid}_${hiveconf:stepid} PURGE;
drop table IF EXISTS ${env:TMP_db}.call_data_${hiveconf:execid}_${hiveconf:stepid} PURGE;
drop table IF EXISTS ${env:TMP_db}.call_summary_${hiveconf:execid}_${hiveconf:stepid} PURGE;
drop table IF EXISTS ${env:TMP_db}.cqe_step0_${hiveconf:execid}_${hiveconf:stepid} PURGE;
drop table if exists ${env:TMP_db}.btm_data_${hiveconf:execid}_${hiveconf:stepid} PURGE;
drop table if exists ${env:TMP_db}.atom_mso_${hiveconf:execid}_${hiveconf:stepid};
drop table if exists ${env:TMP_db}.combine_${hiveconf:execid}_${hiveconf:stepid} PURGE;
drop table IF EXISTS ${env:TMP_db}.cqe_data_${hiveconf:execid}_${hiveconf:stepid} PURGE;
drop table IF EXISTS ${env:TMP_db}.cqe_summary_${hiveconf:execid}_${hiveconf:stepid} PURGE;
