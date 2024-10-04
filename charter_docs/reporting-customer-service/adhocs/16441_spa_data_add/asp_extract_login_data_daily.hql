use dev;

CREATE TABLE IF NOT EXISTS asp_extract_login_data_daily (
  account_number string,
  application_name string,
  message__name string,
  operation__success string,
  division_id string,
  site_sys string,
  prn string,
  agn string
)
  PARTITIONED BY (
  date_denver string
)
;

SELECT '***** Creating asp_extract_login_data_daily_pvt table ******'
;

CREATE TABLE IF NOT EXISTS asp_extract_login_data_daily_pvt (
  account_number string,
  myspectrum string,
  specnet string,
  spectrumbusiness string,
  division_id string,
  site_sys string,
  prn string,
  agn string
)
  PARTITIONED BY (
  date_denver string
)
;

INSERT OVERWRITE TABLE asp_extract_login_data_daily PARTITION (date_denver)

Select  visit__account__account_number as account_number,
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
        visit__account__account_number,
        visit__visit_id,
        message__name,
        operation__success,
        visit__application_details__application_name as application_name,
        visit__account__account_billing_division_id as division_id,
        visit__account__system_sys as site_sys,
        visit__account__system_prin as prn,
        visit__account__system_agent as agn
    from core_quantum_events_portals_v
    where (partition_date_hour_utc >= '2019-11-06_06'
       and partition_date_hour_utc < '2019-11-07_05')
      and visit__account__account_number is not null
      and visit__account__account_number NOT IN('7FbKtybuOWU4/Q0SRInbHA==','GSWNkZXIfDPD6x25Na3i8g==') -- exclude blank or 'pending login'
      and (
           (visit__application_details__application_name in ('MySpectrum','SpecNet')
            AND operation__success = TRUE
            AND message__name = 'loginStop'
            )
         OR(visit__application_details__application_name = 'SMB'
            AND message__name = 'userConfigSet')
          )
    group by epoch_converter(received__timestamp,'America/Denver'),
             visit__account__account_number,
             visit__visit_id,
             visit__application_details__application_name,
             message__name,
             operation__success,
             visit__account__account_billing_division_id,
             visit__account__system_sys,
             visit__account__system_prin,
             visit__account__system_agent
) subquery
group by  visit__account__account_number,
          application_name,
          operation__success,
          message__name,
          date_denver,
          division_id,
          site_sys,
          prn,
          agn
;

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
      where date_denver = '2019-11-06'
     ) subquery
group by account_number,
         date_denver,
         division_id,
         site_sys,
         prn,
         agn
;
