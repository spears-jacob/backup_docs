USE ${env:ENVIRONMENT};

SELECT '***** getting external_session_id from asp_asapp_convos_metadata ******'
;

-- visit_id for last_event_date
DROP TABLE IF EXISTS asp_tmp_asapp_visitid_latest PURGE;
CREATE TEMPORARY TABLE asp_tmp_asapp_visitid_latest AS
select distinct
       external_session_id
  FROM prod.asp_asapp_convos_metadata
 WHERE last_event_date = DATE_ADD("${env:START_DATE}",1)
   and external_session_id is not null;


SELECT '***** getting account number for visitid data ******'
;

-- daily insert of last visitid data for account number
INSERT OVERWRITE TABLE asp_extract_asapp_visitid_data_daily PARTITION (date_denver)

Select
        visit__visit_id as external_session_id,
        visit__application_details__application_name as application_name,
        visit__account__enc_account_number as acct_enc,
        visit__account__account_number as acct,
        NVL(visit__account__enc_account_number,visit__account__account_number) as account_number,
        MAX(state__view__current_page__biller_type) as biller_type,
        MAX(nvl(visit__account__enc_account_billing_division,AES_ENCRYPT256(visit__account__account_billing_division))) as division,
        MAX(nvl(visit__account__enc_account_billing_division_id,AES_ENCRYPT256(visit__account__account_billing_division_id))) as division_id,
        MAX(nvl(visit__account__enc_system_sys,AES_ENCRYPT256(visit__account__system_sys))) as sys,
        MAX(nvl(visit__account__enc_system_prin,AES_ENCRYPT256(visit__account__system_prin))) as prn,
        MAX(nvl(visit__account__enc_system_agent,AES_ENCRYPT256(visit__account__system_agent))) as agn,
        MAX(NVL(visit__account__enc_account_site_id,AES_ENCRYPT256(visit__account__account_site_id))) as acct_site_id,
        MAX(NVL(visit__account__enc_account_company,AES_ENCRYPT256(visit__account__account_company))) as acct_company,
        MAX(NVL(visit__account__enc_account_franchise,AES_ENCRYPT256(visit__account__account_franchise))) as acct_franchise,
        epoch_converter(received__timestamp,'America/Denver') as date_denver
  from  core_quantum_events_portals_v
 where  (partition_date_hour_utc >= '${env:START_DATE_TZ}' and partition_date_hour_utc < '${env:END_DATE_TZ}')
   and  NVL(visit__account__enc_account_number,visit__account__account_number) is not null
   and  NVL(visit__account__enc_account_number,visit__account__account_number) NOT IN('7FbKtybuOWU4/Q0SRInbHA==','GSWNkZXIfDPD6x25Na3i8g==') -- exclude blank or 'pending login'
   and  visit__application_details__application_name in ('MySpectrum','SpecNet','SMB')
   AND  visit__visit_id in (select external_session_id from asp_tmp_asapp_visitid_latest)
 group by visit__application_details__application_name,
          visit__visit_id,
          visit__account__enc_account_number,
          visit__account__account_number,
          NVL(visit__account__enc_account_number,visit__account__account_number),
          epoch_converter(received__timestamp,'America/Denver')
;


SELECT '***** getting pivot table and limit to last date for accounts******'
;

-- pivot table from asp_extract_asapp_visitid_data_daily and limit to last date
INSERT OVERWRITE TABLE asp_extract_asapp_visitid_data_daily_pvt PARTITION (date_denver)
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
              date_denver
      from asp_extract_asapp_visitid_data_daily
     where date_denver = DATE_ADD("${env:START_DATE}",1)
;

SELECT '***** data insert into asp_extract_asapp_visitid_data_daily_pvt complete ******'
;
