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

ADD JAR ${env:JARS_S3_LOCATION}/hadoop-libs-hive-1.0.10.jar;
DROP FUNCTION IF EXISTS aes_decrypt256;
CREATE TEMPORARY FUNCTION aes_decrypt256 AS 'com.spectrum.v1_0_10.hive.udfs.AesDecrypt256';

--For Atom
INSERT OVERWRITE table asp_digital_adoption_monitor_null PARTITION(date_value)
select source_table,
       date_type,
       '' as application_name,
       '' as mso_set,
       metric_name,
       metric_total,
       metric_count,
       metric_count_enc_distinct,
       metric_count_distinct,
       metric_count/metric_total,
       '${env:CURRENT_TIME}' as run_time,
       partition_date_denver as date_value
from (select 'atom' as source_table,
             'partition_date_denver' as date_type,
             partition_date_denver,
             count(1) as count_total,
             count(customer_type) as count_customer_type,
             count(legacy_company) as count_legacy_company,
             count(encrypted_account_key_256) as count_account_key,
             count(encrypted_padded_account_number_256) as count_padded_account,
             count(encrypted_legacy_account_number_256) as count_legacy_account,
             count(distinct customer_type) count_dist_customer_type,
             count(distinct legacy_company) as count_dist_legacy_company,
             count(distinct encrypted_account_key_256) as count_dist_enc_account_key,
             count(distinct encrypted_padded_account_number_256) as count_dist_enc_padded_account,
             count(distinct encrypted_legacy_account_number_256) as count_dist_enc_legacy_account,
             count(distinct aes_decrypt256(encrypted_account_key_256, 'aes256')) as count_dist_account_key,
             count(distinct aes_decrypt256(encrypted_padded_account_number_256, 'aes256')) as count_dist_padded_account,
             count(distinct aes_decrypt256(encrypted_legacy_account_number_256, 'aes256')) as count_dist_legacy_account
        FROM ${env:ACCOUNTATOM}
       where legacy_company in ('CHR','TWC','BHN')
         and extract_source = 'P270'
         AND account_status = 'CONNECTED'
         AND account_type = 'SUBSCRIBER'
         AND partition_date_denver between '${hiveconf:START_DATE_DENVER}' and '${hiveconf:END_DATE_DENVER}'
       group by partition_date_denver) a
lateral view inline(array(
       struct('count_customer_type',count_total, count_customer_type, 0, count_dist_customer_type),
       struct('count_legacy_company',count_total, count_legacy_company, 0, count_dist_legacy_company),
       struct('count_account_key',count_total, count_account_key, count_dist_enc_account_key, count_dist_account_key),
       struct('count_padded_account',count_total, count_padded_account, count_dist_enc_padded_account, count_dist_padded_account),
       struct('count_legacy_account',count_total, count_legacy_account, count_dist_enc_legacy_account, count_dist_legacy_account))) t as metric_name, metric_total, metric_count, metric_count_enc_distinct, metric_count_distinct
;

--For BTM
INSERT INTO asp_digital_adoption_monitor_null PARTITION(date_value)
select source_table,
       date_type,
       '' as application_name,
       '' as mso_set,
       metric_name,
       metric_total,
       metric_count,
       metric_count_enc_distinct,
       metric_count_distinct,
       metric_count/metric_total,
       '${env:CURRENT_TIME}' as run_time,
       partition_date_utc as date_value
FROM (select 'btm' as source_table,
             'partition_date_utc' as date_type,
             partition_date_utc,
             COUNT(1) as count_total,
             count(visit_id) as count_visit,
             count(encrypted_account_number) as count_account_number,
             count(encrypted_account_key_256) as count_account_key,
             count(distinct visit_id) as count_dist_visit,
             count(distinct encrypted_account_number) as count_dist_enc_account_number,
             count(distinct encrypted_account_key_256) as count_dist_enc_account_key,
             count(distinct aes_decrypt256(encrypted_account_number, 'aes256')) as count_dist_account_number,
             count(distinct aes_decrypt256(encrypted_account_key_256, 'aes256')) as count_dist_account_key
        from `${env:BTM}`
       where partition_date_utc between '${hiveconf:START_DATE_DENVER}' and '${hiveconf:END_DATE_DENVER}'
       group by partition_date_utc) a
lateral view inline(array(
        struct('count_visit',count_total, count_visit, 0, count_dist_visit),
        struct('count_account_number',count_total, count_account_number, count_dist_enc_account_number, count_dist_account_number),
        struct('count_account_key',count_total, count_account_key, count_dist_enc_account_key, count_dist_account_key))) t as metric_name, metric_total, metric_count, metric_count_enc_distinct, metric_count_distinct
;

--For Call
INSERT INTO asp_digital_adoption_monitor_null PARTITION(date_value)
select source_table,
       date_type,
       '' as application_name,
       '' as mso_set,
       metric_name,
       metric_total,
       metric_count,
       metric_count_enc_distinct,
       metric_count_distinct,
       metric_count/metric_total,
       '${env:CURRENT_TIME}' as run_time,
       call_end_date_utc as date_value
from (select 'call' as source_table,
             'call_end_date_utc' as date_type,
             call_end_date_utc,
             count(1) as count_total,
             count(encrypted_account_key_256) as count_account_key,
             count(encrypted_account_number_256) as count_account_number,
             count(call_inbound_key) as count_inbound_key,
             count(segment_id) as count_segment_id,
             count(call_id) as count_call_id,
             count(distinct encrypted_account_key_256) as count_dist_enc_account_key,
             count(distinct encrypted_account_number_256) as count_dist_enc_account_number,
             count(distinct call_inbound_key) as count_dist_inbound_key,
             count(distinct segment_id) as count_dist_segment_id,
             count(distinct call_id) as count_dist_call_id,
             count(distinct encrypted_account_key_256) as count_dist_account_key,
             count(distinct encrypted_account_number_256) as count_dist_account_number
        from ${env:ENVIRONMENT}.atom_cs_call_care_data_3
       where call_end_date_utc between '${hiveconf:START_DATE_DENVER}' and '${hiveconf:END_DATE_DENVER}'
       group by call_end_date_utc) a
lateral view inline(array(
        struct('count_call_id',count_total, count_call_id, 0, count_dist_call_id),
        struct('count_segment_id',count_total, count_segment_id, 0, count_dist_segment_id),
        struct('count_inbound_key',count_total, count_inbound_key, 0, count_dist_inbound_key),
        struct('count_account_number',count_total, count_account_number, count_dist_enc_account_number, count_dist_account_number),
        struct('count_account_key',count_total, count_account_key, count_dist_enc_account_key, count_dist_account_key))) t as metric_name, metric_total, metric_count, metric_count_enc_distinct, metric_count_distinct
;

--For CQE
INSERT INTO asp_digital_adoption_monitor_null PARTITION(date_value)
select source_table,
       date_type,
       application_name,
       mso_set,
       metric_name,
       metric_total,
       metric_count,
       metric_count_enc_distinct,
       metric_count_distinct,
       metric_count/metric_total,
       '${env:CURRENT_TIME}' as run_time,
       partition_date_utc as date_value
FROM (SELECT 'cqe_sspp' as source_table,
             'partition_date_utc' as date_type,
             partition_date_utc,
             application_name,
             collect_set(visit_mso) as set_mso,
             count(visit_id) as count_total,
             count(visit_mso) as count_mso,
             count(visit_id) as count_visit,
             count(acct_number_aes_256) as count_account_number,
             count(user_journey) as count_user_journey,
             count(user_sub_journey) as count_user_sub_journey,
             count(distinct visit_mso) as count_dist_mso,
             count(distinct visit_id) as count_dist_visit,
             count(distinct acct_number_aes_256) as count_dist_enc_account_number,
             count(distinct user_journey) as count_dist_user_journey,
             count(distinct user_sub_journey) as count_dist_user_sub_journey,
             count(distinct aes_decrypt256(acct_number_aes_256, 'aes256')) as count_dist_account_number
        from
             (select distinct partition_date_utc,
                     visit__application_details__application_name as application_name,
                     CASE
                        WHEN (visit__account__details__mso='TWC' or visit__account__details__mso='"TWC"') THEN 'TWC'
                        WHEN (visit__account__details__mso= 'BH' or visit__account__details__mso='"BHN"' OR visit__account__details__mso='BHN') THEN 'BHN'
                        WHEN (visit__account__details__mso= 'CHARTER' or visit__account__details__mso='"CHTR"' OR visit__account__details__mso='CHTR') THEN 'CHR'
                        WHEN (trim(visit__account__details__mso)='' OR UPPER(visit__account__details__mso)= 'NONE' or UPPER(visit__account__details__mso)='UNKNOWN') THEN NULL
                        ELSE visit__account__details__mso
                     END AS visit_mso,
                     state__view__current_page__user_journey as user_journey,
                     state__view__current_page__user_sub_journey as user_sub_journey,
                     visit__visit_id as visit_id,
                     visit__account__enc_account_number as acct_number_aes_256
                from ${env:ENVIRONMENT}.core_quantum_events_sspp
               where partition_date_utc between '${hiveconf:START_DATE_DENVER}' and '${hiveconf:END_DATE_DENVER}'
                 and visit__application_details__application_name in ('SpecNet','SMB','MySpectrum','SpecMobile','SelfInstall')) a
        group by partition_date_utc,
                 application_name) b
lateral view inline(array(
        struct('count_mso',count_total, count_mso, 0, count_dist_mso, concat_ws(',',sort_array(set_mso))),
        struct('count_visit',count_total, count_visit, 0, count_dist_visit, ''),
        struct('count_account_number',count_total, count_account_number, count_dist_enc_account_number, count_dist_account_number, ''),
        struct('count_user_journey',count_total, count_user_journey, 0, count_dist_user_journey, ''),
        struct('count_user_sub_journey',count_total, count_user_sub_journey, 0, count_dist_user_sub_journey, ''))) t as metric_name, metric_total, metric_count, metric_count_enc_distinct, metric_count_distinct,mso_set
;

-- For CQE
insert overwrite table asp_digital_adoption_monitor_cqe PARTITION(date_value)
select source_table,
       date_type,
       customer_type,
       application_name,
       visit_mso,
       user_journey,
       user_sub_journey,
       grouping_id,
       metric_name,
       metric_count,
       metric_count_distinct,
       metric_total,
       metric_count/metric_total,
       '${env:CURRENT_TIME}' as run_time,
       partition_date_utc as date_value
  FROM (SELECT 'cqe_sspp' as source_table,
               'partition_date_utc' as date_type,
               partition_date_utc,
               '' as customer_type,
               application_name,
               visit_mso,
               user_journey,
               user_sub_journey,
               CAST(grouping__id AS INT) AS grouping_id,
               count(visit__visit_id) as count_total,
               count(visit__account__enc_account_number) as count_account_number,
               count(visit__visit_id) as count_visit,
               count(distinct visit__account__enc_account_number) as count_dist_account_number,
               count(distinct visit__visit_id) as count_dist_visit
          from
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
                      state__view__current_page__user_journey as user_journey,
                      state__view__current_page__user_sub_journey as user_sub_journey,
                      visit__visit_id,
                      visit__account__enc_account_number
                 from ${env:ENVIRONMENT}.core_quantum_events_sspp
                where partition_date_utc between '${hiveconf:START_DATE_DENVER}' and '${hiveconf:END_DATE_DENVER}'
                  and visit__application_details__application_name in ('SpecNet','SMB','MySpectrum','SpecMobile','SelfInstall')) a
                group by partition_date_utc,
                         application_name,
                         visit_mso,
                         user_journey,
                         user_sub_journey
                         GROUPING SETS (
                            (partition_date_utc, application_name), --7
                            (partition_date_utc, application_name, visit_mso), -- 3
                            (partition_date_utc, application_name, visit_mso, user_journey, user_sub_journey) --0
                         )) b
lateral view inline(array(
struct('count_account_number',count_account_number,count_dist_account_number,count_total),
struct('count_visit',count_visit,count_dist_visit,count_total))) t as metric_name, metric_count, metric_count_distinct, metric_total
;

insert into asp_digital_adoption_monitor_cqe PARTITION(date_value)
SELECT 'cqe_sspp' as source_table,
       'partition_date_utc' as date_type,
       '' as customer_type,
       application_name,
       '' as visit_mso,
       collect_set(CAST(NULL AS STRING)) as user_journey,
       collect_set(CAST(NULL AS STRING)) as user_sub_journey,
       '' as grouping_id,
       'number of visit with more than 1 mso' as metric_name,
       sum(if(count_dist_mso>1, count_visit, 0)) as count_visit_wm_mso,
       0 as metric_count_distinct,
       max(count_visit),
       sum(if(count_dist_mso>1, count_visit, 0))/max(count_visit) as pct_visit_wm_mso,
       '${env:CURRENT_TIME}' as run_time,
       partition_date_utc as date_value
  FROM
       (SELECT CAST(grouping__id AS INT) AS grouping_id,
               partition_date_utc,
               application_name,
               count_dist_mso,
               count(distinct visit_id) as count_visit
          FROM
               (SELECT COUNT(DISTINCT visit_mso) count_dist_mso,
                       partition_date_utc,
                       application_name,
                       visit_id
                  FROM
                       (SELECT
                               CASE
                               WHEN (visit__account__details__mso='TWC' or visit__account__details__mso='"TWC"') THEN 'TWC'
                               WHEN (visit__account__details__mso= 'BH' or visit__account__details__mso='"BHN"' OR visit__account__details__mso='BHN') THEN 'BHN'
                               WHEN (visit__account__details__mso= 'CHARTER' or visit__account__details__mso='"CHTR"' OR visit__account__details__mso='CHTR') THEN 'CHR'
                               WHEN (visit__account__details__mso= 'NONE' or visit__account__details__mso='UNKNOWN') THEN NULL
                               ELSE visit__account__details__mso
                               END AS visit_mso,
                               partition_date_utc,
                               visit__application_details__application_name as application_name,
                               visit__visit_id as visit_id
                          FROM ${env:ENVIRONMENT}.core_quantum_events_sspp
                         WHERE partition_date_utc between '${hiveconf:START_DATE_DENVER}' and '${hiveconf:END_DATE_DENVER}'
                           AND visit__account__enc_account_number is Not Null
                           AND visit__visit_id IS NOT NULL
                           AND visit__account__enc_account_number NOT IN ('dZJho5z9MUSD35BuytdIhg==','Qn+T/sa8AB7Gnxhi4Wx2Xg==')
                           AND visit__application_details__application_name IN ('SpecNet','SMB','MySpectrum','IDManagement','PrivacyMicrosite','SpecMobile', 'SpectrumCommunitySolutions')) a
                 GROUP BY partition_date_utc, application_name, visit_id) b
         GROUP BY partition_date_utc, application_name, count_dist_mso
         GROUPING SETS(
                  (partition_date_utc, application_name),
                  (partition_date_utc, application_name, count_dist_mso)
                  )) c
 GROUP BY partition_date_utc, application_name;

insert into asp_digital_adoption_monitor_null_archive PARTITION(run_date)
select source_table,
       date_type,
       application_name,
       mso_set,
       metric_name,
       metric_total,
       metric_count,
       metric_count_enc_distinct,
       metric_count_distinct,
       metric_count_pct,
       '${env:CURRENT_TIME}' as run_time,
       date_value,
       SUBSTR('${env:CURRENT_TIME}',1,10) as run_date
from asp_digital_adoption_monitor_null
where date_value between '${hiveconf:START_DATE_DENVER}' and '${hiveconf:END_DATE_DENVER}';

insert into asp_digital_adoption_monitor_cqe_archive PARTITION(run_date)
select source_table,
       date_type,
       customer_type,
       application_name,
       visit_mso,
       user_journey,
       user_sub_journey,
       grouping_id,
       metric_name,
       metric_count,
       metric_count_distinct,
       metric_total,
       metric_count_pct,
       '${env:CURRENT_TIME}' as run_time,
       date_value,
       SUBSTR('${env:CURRENT_TIME}',1,10) as run_date
from asp_digital_adoption_monitor_cqe
where date_value between '${hiveconf:START_DATE_DENVER}' and '${hiveconf:END_DATE_DENVER}';
