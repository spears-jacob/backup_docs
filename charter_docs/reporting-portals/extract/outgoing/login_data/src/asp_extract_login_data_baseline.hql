USE ${env:ENVIRONMENT};

set mapreduce.input.fileinputformat.split.maxsize=5368709120;
set mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.optimize.sort.dynamic.partition = false;
set hive.vectorized.execution.enabled = false;

--drop table IF EXISTS asp_extract_login_data_baseline;
CREATE TABLE IF NOT EXISTS asp_extract_login_data_baseline (
  account_number string,
  application_name string,
  message__name string,
  operation__success string,
  division_id string,
  site_sys string,
  prn string,
  agn string,
  biller_type string,
  division string,
  acct_site_id string,
  acct_company string,
  acct_franchise string
)
PARTITIONED BY (date_denver string)
;

--Run Monthly
--set START_DATE_TZ=2020-01-01_06;
--set END_DATE_TZ=2020-02-01_06;

--set START_DATE_TZ=2020-02-01_06;
--set END_DATE_TZ=2020-03-01_06;
--
--set START_DATE_TZ=2020-03-01_06;
--set END_DATE_TZ=2020-04-01_06;
--
--set START_DATE_TZ=2020-04-01_06;
--set END_DATE_TZ=2020-05-01_06;
--
--set START_DATE_TZ=2020-05-01_06;
--set END_DATE_TZ=2020-06-01_06;
--
set START_DATE_TZ=2020-06-01_06;
set END_DATE_TZ=2020-06-15_06;

SELECT '***** getting login data per account base ******'
;
--get the max date for each month
INSERT INTO TABLE asp_extract_login_data_baseline PARTITION (date_denver)
Select  account_number,
        application_name,
        message__name,
        operation__success,
        division_id,
        site_sys,
        prn,
        agn,
        biller_type,
        division,
        acct_site_id,
        acct_company,
        acct_franchise,
        max(date_denver) as date_denver
from
  ( Select
        epoch_converter(received__timestamp,'America/Denver') as date_denver,
        visit__account__enc_account_number as account_number,
        visit__visit_id,
        case when visit__application_details__application_name='SpecNet'
             THEN max(message__name) over
                  (partition by visit__application_details__application_name,
                   visit__account__enc_account_number, visit__visit_id)
             ELSE message__name
        end as message__name,
        case when visit__application_details__application_name='SpecNet'
             THEN max(operation__success) over
                  (partition by visit__application_details__application_name,
                   visit__account__enc_account_number, visit__visit_id)
             ELSE operation__success
        end as operation__success,
        visit__application_details__application_name as application_name,
        case when visit__application_details__application_name='SpecNet'
             THEN max(visit__account__enc_account_billing_division_id) over
                  (partition by visit__application_details__application_name,
                   visit__account__enc_account_number, visit__visit_id)
             ELSE visit__account__enc_account_billing_division_id
        end as division_id,
        case when visit__application_details__application_name='SpecNet'
             THEN max(visit__account__enc_system_sys) over
                  (partition by visit__application_details__application_name,
                   visit__account__enc_account_number, visit__visit_id)
             ELSE visit__account__enc_system_sys
        end as site_sys,
        case when visit__application_details__application_name='SpecNet'
             THEN max(visit__account__enc_system_prin) over
                  (partition by visit__application_details__application_name,
                   visit__account__enc_account_number, visit__visit_id)
             ELSE visit__account__enc_system_prin
        end as prn,
        case when visit__application_details__application_name='SpecNet'
             THEN max(visit__account__enc_system_agent) over
                  (partition by visit__application_details__application_name,
                   visit__account__enc_account_number, visit__visit_id)
             ELSE visit__account__enc_system_agent
        end as agn,
        case when visit__application_details__application_name='SpecNet'
             THEN max(state__view__current_page__biller_type) over
                  (partition by visit__application_details__application_name,
                   visit__account__enc_account_number, visit__visit_id)
             ELSE state__view__current_page__biller_type
        end as biller_type,
        case when visit__application_details__application_name='SpecNet'
             THEN max(visit__account__enc_account_billing_division) over
                  (partition by visit__application_details__application_name,
                   visit__account__enc_account_number, visit__visit_id)
             ELSE visit__account__enc_account_billing_division
        end as division,
        case when visit__application_details__application_name='SpecNet'
             THEN max(visit__account__enc_account_site_id) over
                  (partition by visit__application_details__application_name,
                   visit__account__enc_account_number, visit__visit_id)
             ELSE visit__account__enc_account_site_id
        end as acct_site_id,
        case when visit__application_details__application_name='SpecNet'
             THEN max(visit__account__enc_account_company) over
                  (partition by visit__application_details__application_name,
                   visit__account__enc_account_number, visit__visit_id)
             ELSE visit__account__enc_account_company
        end as acct_company,
        case when visit__application_details__application_name='SpecNet'
             THEN max(visit__account__enc_account_franchise) over
                  (partition by visit__application_details__application_name,
                   visit__account__enc_account_number, visit__visit_id)
             ELSE visit__account__enc_account_franchise
        end as acct_franchise
    from core_quantum_events
    where (partition_date_hour_utc >= '${hiveconf:START_DATE_TZ}'
       and partition_date_hour_utc < '${hiveconf:END_DATE_TZ}')
      and visit__account__enc_account_number is not null
      and visit__account__enc_account_number NOT IN('7FbKtybuOWU4/Q0SRInbHA==','GSWNkZXIfDPD6x25Na3i8g==') -- exclude blank or 'pending login'
      and (
           (visit__application_details__application_name in ('MySpectrum','SpecMobile')
            AND operation__success = TRUE
            AND message__name = 'loginStop'
            )
         OR(visit__application_details__application_name = 'SpecNet'
               AND ((operation__success = TRUE and message__name = 'loginStop')
                  OR message__name = 'userConfigSet'))
         OR(visit__application_details__application_name = 'SMB'
            AND message__name = 'userConfigSet')
          )
    group by epoch_converter(received__timestamp,'America/Denver'),
             visit__account__enc_account_number,
             visit__visit_id,
             visit__application_details__application_name,
             message__name,
             operation__success,
             visit__account__enc_account_billing_division_id,
             visit__account__enc_system_sys,
             visit__account__enc_system_prin,
             visit__account__enc_system_agent,
             state__view__current_page__biller_type,
             visit__account__enc_account_billing_division,
             visit__account__enc_account_site_id,
             visit__account__enc_account_company,
             visit__account__enc_account_franchise
) subquery
group by  account_number,
          application_name,
          operation__success,
          message__name,
          division_id,
          site_sys,
          prn,
          agn,
          biller_type,
          division,
          acct_site_id,
          acct_company,
          acct_franchise
;

SELECT '***** getting pivot table and limit to last date for accounts******'
;
--get the max date for account
DROP TABLE IF EXISTS asp_extract_login_data_baseline_pvt;
CREATE TABLE IF NOT EXISTS asp_extract_login_data_baseline_pvt AS
Select  account_number,
        max(myspectrum) as myspectrum,
        max(specnet) as specnet,
        max(spectrumbusiness) as spectrumbusiness,
        division_id AS division_id,
        MAX(site_sys) AS site_sys,
        MAX(prn) as prn,
        MAX(agn) as agn,
        max(specmobile) as specmobile,
        MAX(biller_type) as biller_type ,
        division as division,
        MAX(acct_site_id) as acct_site_id,
        MAX(acct_company) as acct_company,
        MAX(acct_franchise) as acct_franchise,
        current_date as date_denver --run date
from (select  account_number,
              case when application_name = 'MySpectrum' then max(date_denver) else null end as myspectrum,
              case when application_name = 'SpecNet' then MAX(date_denver) else null end as specnet,
              case when application_name = 'SMB' then MAX(date_denver) else null end as spectrumbusiness,
              case when application_name = 'SpecMobile' then MAX(date_denver) else null end as specmobile,
              MAX(date_denver) as date_denver,
              division_id,
              site_sys,
              prn,
              agn,
              biller_type,
              division,
              acct_site_id,
              acct_company,
              acct_franchise
      from asp_extract_login_data_baseline
      where application_name in ('MySpectrum','SpecNet','SMB','SpecMobile')
      group by account_number,
               application_name,
               operation__success,
               message__name,
               division_id,
               site_sys,
               prn,
               agn,
               biller_type,
               division,
               acct_site_id,
               acct_company,
               acct_franchise
     ) subquery
group by account_number,
         division,
         division_id
;

SELECT '***** data insert into asp_extract_login_data_baseline_pvt complete ******'
;
