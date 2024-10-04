USE ${env:TMP_db};

set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.merge.tezfiles=true;
set hive.msck.path.validation=ignore;

ADD JAR ${env:JARS_S3_LOCATION}/epoch-1.0-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION epoch_converter AS 'Epoch_To_Denver';

SELECT '***** getting external_session_id from asp_asapp_convos_metadata ******'
;

-- visit_id for last_event_date
 msck repair table `${hiveconf:prod_only_table_location}prod_dasp.asp_asapp_convos_metadata`;

-- analyze table `${hiveconf:prod_only_table_location}prod_dasp.asp_asapp_convos_metadata` partition (last_event_date) compute statistics;

DROP TABLE IF EXISTS asp_tmp_asapp_visitid_latest PURGE;
CREATE TEMPORARY TABLE IF NOT EXISTS asp_tmp_asapp_visitid_latest
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION 'hdfs:///tmp/tmp_asapp-visitid-data/asp_tmp_asapp_visitid_latest'
TBLPROPERTIES('orc.compress'='snappy') AS
select distinct
       external_session_id
  FROM `${hiveconf:prod_only_table_location}prod_dasp.asp_asapp_convos_metadata`
 WHERE last_event_date = DATE_ADD("${hiveconf:START_DATE}",1)
   and external_session_id is not null;

SELECT '***** making temp tables to hold basic and enriched visitid data *****'
;

CREATE TABLE IF NOT EXISTS asp_extract_asapp_visitid_data_daily_${hiveconf:execid}_${hiveconf:stepid}
  (
    external_session_id string,
    application_name string,
    acct_enc string,
    acct string,
    account_number string,
    biller_type string,
    division string,
    division_id string,
    sys string,
    prn string,
    agn string,
    acct_site_id string,
    acct_company string,
    acct_franchise string,
    flag_enriched boolean,
    mobile_acct_enc string,
    date_denver string
  )
;

CREATE TABLE IF NOT EXISTS asp_extract_asapp_visitid_data_to_enrich_${hiveconf:execid}_${hiveconf:stepid}
  (
    external_session_id string,
    application_name string,
    acct_enc string,
    acct string,
    account_number string,
    biller_type string,
    division string,
    division_id string,
    sys string,
    prn string,
    agn string,
    acct_site_id string,
    acct_company string,
    acct_franchise string,
    mobile_acct_enc string,
    date_denver string
  )
;

CREATE TABLE IF NOT EXISTS asp_extract_asapp_visitid_data_cqe_pull_${hiveconf:execid}_${hiveconf:stepid}
  (
    external_session_id string,
    application_name string,
    acct_enc string,
    acct string,
    account_number string,
    biller_type string,
    division string,
    division_id string,
    sys string,
    prn string,
    agn string,
    acct_site_id string,
    acct_company string,
    acct_franchise string,
    mobile_acct_enc string,
    date_denver string
  )
;


SELECT '***** getting account number for visitid data ******'
;

INSERT INTO asp_extract_asapp_visitid_data_cqe_pull_${hiveconf:execid}_${hiveconf:stepid}
Select
        visit__visit_id as external_session_id,
        visit__application_details__application_name as application_name,
        MAX(visit__account__enc_account_number) as acct_enc,
        MAX(visit__account__account_number) as acct,
        MAX(visit__account__enc_account_number) as account_number,
        MAX(state__view__current_page__biller_type) as biller_type,
        MAX(visit__account__enc_account_billing_division) as division,
        MAX(visit__account__enc_account_billing_division_id) as division_id,
        MAX(visit__account__enc_system_sys) as sys,
        MAX(visit__account__enc_system_prin) as prn,
        MAX(visit__account__enc_system_agent) as agn,
        MAX(visit__account__enc_account_site_id) as acct_site_id,
        MAX(visit__account__enc_account_company) as acct_company,
        MAX(visit__account__enc_account_franchise) as acct_franchise,
        MAX(visit__account__enc_mobile_account_number) as mobile_acct_enc,
        epoch_converter(received__timestamp,'America/Denver') as date_denver
        FROM ${env:ENVIRONMENT}.core_quantum_events_sspp
 where  (partition_date_hour_utc >= '${hiveconf:START_DATE_TZ}' and partition_date_hour_utc < '${hiveconf:END_DATE_TZ}')
   and visit__application_details__application_name in ('MySpectrum', 'SpecNet', 'SMB')
   and  (NVL(visit__account__enc_account_number,visit__account__account_number) NOT IN('7FbKtybuOWU4/Q0SRInbHA==','GSWNkZXIfDPD6x25Na3i8g==') OR visit__account__enc_account_number IS NULL)
   AND  visit__visit_id in (select external_session_id from asp_tmp_asapp_visitid_latest)
   AND  epoch_converter(received__timestamp,'America/Denver')=DATE_ADD("${hiveconf:START_DATE}",1)
group by visit__application_details__application_name,
       visit__visit_id ,
       epoch_converter(received__timestamp,'America/Denver')
;

INSERT INTO TABLE asp_extract_asapp_visitid_data_daily_${hiveconf:execid}_${hiveconf:stepid}
SELECT
  external_session_id,
  application_name,
  acct_enc,
  acct,
  account_number,
  biller_type,
  division,
  division_id,
  sys,
  prn,
  agn,
  acct_site_id,
  acct_company,
  acct_franchise,
  0 as flag_enriched,
  mobile_acct_enc,
  date_denver
 FROM asp_extract_asapp_visitid_data_cqe_pull_${hiveconf:execid}_${hiveconf:stepid} WHERE acct_enc IS NOT NULL
;

INSERT INTO TABLE asp_extract_asapp_visitid_data_to_enrich_${hiveconf:execid}_${hiveconf:stepid}
SELECT * FROM asp_extract_asapp_visitid_data_cqe_pull_${hiveconf:execid}_${hiveconf:stepid} WHERE acct_enc IS NULL
;

SELECT '***** enriching account info with btm join ******'
;

INSERT INTO TABLE asp_extract_asapp_visitid_data_daily_${hiveconf:execid}_${hiveconf:stepid}
SELECT
        btm.visit_id as external_session_id,
        cqe.application_name as application_name,
        act.encrypted_padded_account_number_256 as acct_enc,
        NULL as acct,
        act.encrypted_padded_account_number_256 as account_number,
        MAX(act.billing_system) as biller_type,
        MAX(act.encrypted_division_id_256) as division,
        NULL as division_id, -- no field in act that exactly corresponds to this field in CQE
        MAX(CASE WHEN act.billing_system='CSG' then act.encrypted_system_256 else NULL end) as sys,
        MAX(CASE WHEN act.billing_system='CSG' then act.encrypted_prin_256 else NULL end) as prn,
        MAX(CASE WHEN act.billing_system='CSG' then act.encrypted_agent_256 else NULL end) as agn,
        MAX(CASE WHEN act.billing_system='ICOMS' then act.encrypted_agent_256 else NULL end) as acct_site_id,
        MAX(CASE WHEN act.billing_system='ICOMS' then act.encrypted_prin_256 else NULL end) as acct_company,
        NULL as acct_franchise, -- no field in act that exactly corresponds to this field in CQE
        1 as flag_enriched,
        MAX(id.mobile_account_number_aes256) as mobile_acct_enc,
--        'test string' as date_denver
     cqe.date_denver as date_denver
FROM
  asp_extract_asapp_visitid_data_to_enrich_${hiveconf:execid}_${hiveconf:stepid} cqe
  JOIN `${hiveconf:prod_only_table_location}prod.experiment_btm_visit_id_account_key_lookup` btm
    on cqe.external_session_id=btm.visit_id
    and cqe.date_denver=btm.partition_date_utc
  JOIN `${hiveconf:prod_only_table_location}prod.atom_accounts_snapshot` act
    on act.encrypted_account_key_256=btm.encrypted_account_key_256
    and act.partition_date_denver=btm.partition_date_utc
  JOIN `${hiveconf:prod_only_table_location}prod.atom_identity_lookup` id
   on id.encrypted_account_key_256=btm.encrypted_account_key_256
GROUP BY
        btm.visit_id,
        cqe.application_name,
        act.encrypted_padded_account_number_256,
        act.encrypted_padded_account_number_256,
        cqe.date_denver
;

INSERT OVERWRITE TABLE ${env:DASP_db}.asp_extract_asapp_visitid_data_daily PARTITION (date_denver)
SELECT * FROM asp_extract_asapp_visitid_data_daily_${hiveconf:execid}_${hiveconf:stepid}
;

SELECT '***** getting pivot table and limit to last date for accounts******'
;

-- pivot table from asp_extract_asapp_visitid_data_daily and limit to last date
INSERT OVERWRITE TABLE ${env:DASP_db}.asp_extract_asapp_visitid_data_daily_pvt PARTITION (date_denver)
select  external_session_id,
              application_name,
              account_number,
              biller_type,
              division,
              division_id,
              sys,
              prn,
              agn,
              acct_site_id,
              acct_company,
              acct_franchise,
              flag_enriched,
              mobile_acct_enc,
              date_denver
      from ${env:DASP_db}.asp_extract_asapp_visitid_data_daily
     where date_denver = DATE_ADD("${hiveconf:START_DATE}",1)
;

--- Keep GLUE and HDFS clean by deleting temporary tables after use
DROP TABLE IF EXISTS asp_tmp_asapp_visitid_latest PURGE;
DROP TABLE IF EXISTS asp_extract_asapp_visitid_data_cqe_pull_${hiveconf:execid}_${hiveconf:stepid} PURGE;
DROP TABLE IF EXISTS asp_extract_asapp_visitid_data_daily_${hiveconf:execid}_${hiveconf:stepid} PURGE;
DROP TABLE IF EXISTS asp_extract_asapp_visitid_data_to_enrich_${hiveconf:execid}_${hiveconf:stepid} PURGE;

SELECT '***** data insert into asp_extract_asapp_visitid_data_daily_pvt complete ******'
;
