USE ${env:ENVIRONMENT};

SELECT '***** getting login data per account base ******'
;

-- daily insert of last login data for account number
INSERT OVERWRITE TABLE asp_extract_login_data_daily PARTITION (date_denver)

Select  account_number,
        application_name,
        message__name,
        operation__success,
        division_id,
        site_sys,
        prn,
        agn,
        date_denver
from
  ( Select
        epoch_converter(received__timestamp,'America/Denver') as date_denver,
        visit__account__enc_account_number as account_number,
        visit__visit_id,
        message__name,
        operation__success,
        visit__application_details__application_name as application_name,
        visit__account__enc_account_billing_division_id as division_id,
        visit__account__enc_system_sys as site_sys,
        visit__account__enc_system_prin as prn,
        visit__account__enc_system_agent as agn
    from core_quantum_events_portals_v
    where (partition_date_hour_utc >= '${env:START_DATE_TZ}'
       and partition_date_hour_utc < '${env:END_DATE_TZ}')
      and visit__account__enc_account_number is not null
      and visit__account__enc_account_number NOT IN('7FbKtybuOWU4/Q0SRInbHA==','GSWNkZXIfDPD6x25Na3i8g==') -- exclude blank or 'pending login'
      and (
           (visit__application_details__application_name in ('MySpectrum','SpecNet')
            AND operation__success = TRUE
            AND message__name = 'loginStop'
            )
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
             visit__account__enc_system_agent
) subquery
group by  account_number,
          application_name,
          operation__success,
          message__name,
          date_denver,
          division_id,
          site_sys,
          prn,
          agn
;


SELECT '***** getting pivot table and limit to last date for accounts******'
;

-- pivot table from asp_extract_login_data_daily and limit to last date
INSERT OVERWRITE TABLE asp_extract_login_data_daily_pvt PARTITION (date_denver)
Select  account_number,
        max(myspectrum) as myspectrum,
        max(specnet) as specnet,
        max(spectrumbusiness) as spectrumbusiness,
        division_id,
        site_sys,
        prn,
        agn,
        date_denver


from (select  account_number,
              case when application_name = 'MySpectrum' then date_denver else null end as myspectrum,
              case when application_name = 'SpecNet' then date_denver else null end as specnet,
              case when application_name = 'SMB' then date_denver else null end as spectrumbusiness,
              date_denver,
              division_id,
              site_sys,
              prn,
              agn
      from asp_extract_login_data_daily
      where date_denver = '${env:START_DATE}'
     ) subquery
group by account_number,
         date_denver,
         division_id,
         site_sys,
         prn,
         agn
;

SELECT '***** data insert into asp_extract_login_data_daily_pvt complete ******'
;
