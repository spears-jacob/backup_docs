USE ${env:DASP_db};

set hive.log.explain.output=FALSE;
set mapreduce.input.fileinputformat.split.maxsize=5368709120;
set mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.optimize.sort.dynamic.partition=false;
set hive.exec.dynamic.partition.mode=nonstrict;
set orc.force.positional.evolution=true;
SET hive.merge.tezfiles=true;
set hive.merge.smallfiles.avgsize=2048000000;
set hive.merge.size.per.task=2048000000;

ADD JAR ${env:SCALA_LIB};
ADD JAR s3://pi-global-${env:ENVIRONMENT}-udf-jars/daspscalaudf_2.11-0.1.jar;
CREATE TEMPORARY FUNCTION udf_remove_dups AS 'com.charter.dasp.udf.DupRemover';

ADD JAR ${env:JARS_S3_LOCATION}/epoch-1.0-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION epoch_converter AS 'Epoch_To_Denver';

-- Compare DigitalAdoption and call counts obtained from DA and (Atom, Call)/(Atom, CQE)
-- 1) Get DigitalAdoption and call counts Directly from DA using encrypted_padded_account_number_256
insert overwrite table asp_digital_adoption_monitor_da PARTITION(date_value)
select source_table,
       date_type,
       customer_type,
       metric_name,
       metric_value,
       '${env:CURRENT_TIME}' as run_time,
       partition_date_denver as data_value
from
(SELECT 'da_daily' as source_table,
        'partition_date_denver' as date_type,
        partition_date_denver,
        customer_type_distinct as customer_type,
        count(distinct CASE WHEN DigitalTP>0 THEN encrypted_legacy_account_number_256 ELSE NULL END) as Digital_Adoption,
        SUM(call_counts) as call_counts
from
      (select customer_type_distinct,
              partition_date_denver,
              encrypted_legacy_account_number_256,
              SUM(CASE WHEN customer_type_distinct='Residential'
                  ThEN distinct_visits_msa + distinct_visits_spec + distinct_visits_specmobile + distinct_visits_selfinstall
                  ELSE distinct_visits_smb END) as DigitalTP,
              sum(distinct_visits_smb) as DigitalTP_SMB,
              SUM(distinct_visits_msa + distinct_visits_spec + distinct_visits_specmobile + distinct_visits_selfinstall) as DigitalTP_Resi,
              --SUM(distinct_visits_msa + distinct_visits_spec + distinct_visits_specmobile + distinct_visits_selfinstall + distinct_visits_buyflow) as DigitalTP_Resi,
              SUM(call_counts) as call_counts
         from
             (SELECT encrypted_legacy_account_number_256,
                     partition_date_denver,
                     legacy_company,
                     udf_remove_dups(customer_type_list) as customer_type_distinct,
                     if (distinct_visits_smb is Null, 0, 1) as distinct_visits_smb,
                     if (distinct_visits_msa is Null, 0, 1) as distinct_visits_msa,
                     if (distinct_Visits_spec is Null, 0, 1) as distinct_visits_spec,
                     if (distinct_visits_specmobile is Null, 0, 1) as distinct_visits_specmobile,
                     if (distinct_visits_selfinstall is Null, 0, 1) as distinct_visits_selfinstall,
                     --if (distinct_visits_buyflow is Null, 0, 1) as distinct_visits_buyflow,
                     IF (call_counts is null, 0, call_counts) as call_counts
                from asp_digital_adoption_daily
               where partition_date_denver between '${hiveconf:START_DATE_DENVER}' and '${hiveconf:END_DATE_DENVER}') a
        group by customer_type_distinct, partition_date_denver, encrypted_legacy_account_number_256) b
group by customer_type_distinct, partition_date_denver) c
lateral view inline(array(
struct('digital_adoption',Digital_Adoption),
struct('call_counts',call_counts))) t as metric_name, metric_value
;

-- Create ATOM for Call and Visit
drop table IF EXISTS ${env:TMP_db}.atom_${hiveconf:execid}_${hiveconf:stepid};
CREATE table ${env:TMP_db}.atom_${hiveconf:execid}_${hiveconf:stepid} as
SELECT distinct
       partition_date_denver,
       customer_type,
       legacy_company,
       encrypted_account_key_256,
       encrypted_padded_account_number_256,
       encrypted_legacy_account_number_256
  from ${env:ACCOUNTATOM}
 where partition_date_denver between '${hiveconf:START_DATE_DENVER}' and DATE_ADD('${hiveconf:END_DATE_DENVER}',1)
   and extract_source = 'P270'
   and account_status='CONNECTED'
   AND account_type='SUBSCRIBER'
   AND legacy_company in ('CHR','TWC','BHN')
;

-- 2) Get call_counts Directly from Atom and Call
insert into asp_digital_adoption_monitor_da PARTITION(date_value)
select 'atom_call' as source_table,
       'partition_date_denver' as date_type,
       customer_type,
       'call_counts' as metric_name,
       sum(call_counts) metric_value,
       '${env:CURRENT_TIME}' as run_time,
       partition_date_denver as date_value
from (
      select *
        from ${env:TMP_db}.atom_${hiveconf:execid}_${hiveconf:stepid} a
        left join
        (SELECT encrypted_account_key_256,
                epoch_converter(call_end_timestamp_utc, 'America/Denver') AS call_end_date_denver,
                count(distinct call_inbound_key) call_counts
           from ${env:ENVIRONMENT}.atom_cs_call_care_data_3
          where call_end_date_utc between '${hiveconf:START_DATE_DENVER}' and DATE_ADD('${hiveconf:END_DATE_DENVER}',1)
          group by encrypted_account_key_256,
                   epoch_converter(call_end_timestamp_utc, 'America/Denver')) b
        on a.encrypted_account_key_256=b.encrypted_account_key_256
       AND a.partition_date_denver=b.call_end_date_denver) c
group by partition_date_denver, customer_type
having partition_date_denver between '${hiveconf:START_DATE_DENVER}' and '${hiveconf:END_DATE_DENVER}'
;

--Get BTM
drop table IF EXISTS ${env:TMP_db}.btm_${hiveconf:execid}_${hiveconf:stepid};
create table ${env:TMP_db}.btm_${hiveconf:execid}_${hiveconf:stepid}
AS
SELECT *
  FROM `${env:BTM}`
 where partition_date_utc between DATE_ADD('${hiveconf:START_DATE_DENVER}',-2) and DATE_ADD('${hiveconf:END_DATE_DENVER}',1);

drop table if exists ${env:TMP_db}.atom_mso_${hiveconf:execid}_${hiveconf:stepid};
create table ${env:TMP_db}.atom_mso_${hiveconf:execid}_${hiveconf:stepid}
AS
SELECT distinct
       encrypted_account_key_256,
       partition_date_denver,
       legacy_company
  from ${env:ACCOUNTATOM}
 where partition_date_denver between DATE_ADD('${hiveconf:START_DATE_DENVER}',-2) and DATE_ADD('${hiveconf:END_DATE_DENVER}',1);

drop table IF EXISTS ${env:TMP_db}.cqe_${hiveconf:execid}_${hiveconf:stepid};
create table ${env:TMP_db}.cqe_${hiveconf:execid}_${hiveconf:stepid} AS
SELECT distinct
       partition_date_utc,
       application_name,
       received_date_denver,
       visit_id,
       max(visit__account__enc_account_number) OVER (PARTITION BY partition_date_utc, application_name, visit_id) as account_number_aes_256,
       max(visit_mso) OVER (PARTITION BY partition_date_utc, application_name, visit_id) as visit_mso
FROM
    (select distinct
            partition_date_utc,
            visit__application_details__application_name as application_name,
            CASE
               WHEN (visit__account__details__mso='TWC' or visit__account__details__mso='"TWC"') THEN 'TWC'
               WHEN (visit__account__details__mso= 'BH' or visit__account__details__mso='"BHN"' OR visit__account__details__mso='BHN') THEN 'BHN'
               WHEN (visit__account__details__mso= 'CHARTER' or visit__account__details__mso='"CHTR"' OR visit__account__details__mso='CHTR') THEN 'CHR'
               WHEN (UPPER(visit__account__details__mso)= 'NONE' or UPPER(visit__account__details__mso)='UNKNOWN') THEN NULL
               ELSE visit__account__details__mso
            END AS visit_mso,
            epoch_converter(received__timestamp, 'America/Denver') as received_date_denver,
            visit__visit_id as visit_id,
            visit__account__enc_account_number
       from ${env:ENVIRONMENT}.core_quantum_events_sspp
      where partition_date_utc between DATE_ADD('${hiveconf:START_DATE_DENVER}',-2) and DATE_ADD('${hiveconf:END_DATE_DENVER}',1)
        and visit__application_details__application_name in ('SpecNet','SMB','MySpectrum','SpecMobile','SelfInstall')
        AND epoch_converter(received__timestamp, 'America/Denver') between '${hiveconf:START_DATE_DENVER}' and '${hiveconf:END_DATE_DENVER}') a
;

drop table IF EXISTS ${env:TMP_db}.cqe_btm_mso_${hiveconf:execid}_${hiveconf:stepid};
create table ${env:TMP_db}.cqe_btm_mso_${hiveconf:execid}_${hiveconf:stepid} AS
select distinct
       a.partition_date_utc,
       a.received_date_denver,
       a.application_name,
       a.visit_id,
       COALESCE(a.account_number_aes_256,b.encrypted_account_number) as account_number_aes_256,
       COALESCE(a.visit_mso,c.legacy_company) as visit_mso
  from ${env:TMP_db}.cqe_${hiveconf:execid}_${hiveconf:stepid} a
  left JOIN ${env:TMP_db}.btm_${hiveconf:execid}_${hiveconf:stepid} b
    on a.visit_id = b.visit_id
   and a.partition_date_utc=b.partition_date_utc
  LEFT JOIN ${env:TMP_db}.atom_mso_${hiveconf:execid}_${hiveconf:stepid} c
    on b.encrypted_account_key_256=c.encrypted_account_key_256
   and b.partition_date_utc=c.partition_date_denver;
;

--2) GET digital_adoption directly FROM ATOM-CQE-BTM
insert into asp_digital_adoption_monitor_da PARTITION(date_value)
select 'atom_cqe_btm' as source_table,
       'partition_date_denver' as date_type,
       customer_type,
       'digital_adoption' as metric_name,
       count(distinct
             CASE WHEN customer_type='Commercial' and application_name='SMB' THEN encrypted_legacy_account_number_256
                  WHEN customer_type='Residential' and application_name in ('SpecNet','MySpectrum','SpecMobile','SelfInstall') THEN encrypted_legacy_account_number_256
                  ELSE NULL END) as metric_value,
       '${env:CURRENT_TIME}' as run_time,
       partition_date_denver as date_value
  from (SELECT *
          from ${env:TMP_db}.atom_${hiveconf:execid}_${hiveconf:stepid} a
          LEFT JOIN ${env:TMP_db}.cqe_btm_mso_${hiveconf:execid}_${hiveconf:stepid} b
            ON a.encrypted_legacy_account_number_256 =b.account_number_aes_256
           AND a.partition_date_denver=b.received_date_denver
           AND a.legacy_company=b.visit_mso) c
 where visit_id is not null
 and partition_date_denver between '${hiveconf:START_DATE_DENVER}' and '${hiveconf:END_DATE_DENVER}'
 group by customer_type, partition_date_denver
;

insert into asp_digital_adoption_monitor_da_archive PARTITION(run_date)
select source_table,
       date_type,
       customer_type,
       metric_name,
       metric_value,
       '${env:CURRENT_TIME}' as run_time,
       date_value,
       SUBSTR('${env:CURRENT_TIME}',1,10) as run_date
from asp_digital_adoption_monitor_da
where date_value between '${hiveconf:START_DATE_DENVER}' and '${hiveconf:END_DATE_DENVER}';

--INSERT INTO table asp_digital_adoption_monitor_log PARTITION(run_month)
INSERT INTO table asp_digital_adoption_monitor_log PARTITION(run_month)
SELECT '${env:SCRIPT_VERSION}',
       '${hiveconf:START_DATE_DENVER}',
       '${hiveconf:END_DATE_DENVER}',
       '${env:ADDITIONAL_PARAMS}',
       '${env:CURRENT_TIME}',
       '${env:CURRENT_DAY}',
        substr('${env:CURRENT_DAY}',0,7) as run_month;

DROP TABLE IF EXISTS ${env:TMP_db}.atom_${hiveconf:execid}_${hiveconf:stepid}  PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.btm_${hiveconf:execid}_${hiveconf:stepid}  PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.atom_mso_${hiveconf:execid}_${hiveconf:stepid}  PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.cqe_${hiveconf:execid}_${hiveconf:stepid}  PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.cqe_btm_mso_${hiveconf:execid}_${hiveconf:stepid}  PURGE;
