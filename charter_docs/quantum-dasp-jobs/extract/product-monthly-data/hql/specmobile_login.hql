USE ${env:DASP_db};

set mapreduce.input.fileinputformat.split.maxsize=512000000;
set mapreduce.input.fileinputformat.split.minsize=512000000;
set hive.optimize.sort.dynamic.partition = false;
set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.merge.tezfiles=true;
set hive.merge.mapredfiles=true;

ADD JAR s3://pi-global-${env:ENVIRONMENT}-udf-jars/epoch-1.0-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION epoch_converter AS 'Epoch_To_Denver';

SELECT "\n\nFor: SpecMobile Login \n\n";

INSERT OVERWRITE TABLE asp_specmobile_login partition(date_denver)
Select  distinct --account level
        login_timestamp,
        account_number,
        division_id,
        division,
        site_sys,
        prn,
        agn,
        date_denver
FROM
    (Select  --visit level
            date_denver,
            min(received__timestamp) over (partition by account_number, visit__visit_id, division_id, division, site_sys, prn, agn order by received__timestamp) as login_timestamp,
            account_number,
            visit__visit_id,
            division_id,
            division,
            site_sys,
            prn,
            agn
    from
      (Select distinct  --visit level
              epoch_converter(received__timestamp,'America/Denver') as date_denver,
              partition_date_utc,
              received__timestamp,
              visit__account__enc_account_number as account_number,
              visit__visit_id,
              visit__account__enc_account_billing_division_id as division_id,
              visit__account__enc_system_sys as site_sys,
              visit__account__enc_system_prin as prn,
              visit__account__enc_system_agent as agn,
              state__view__current_page__biller_type as biller_type,
              visit__account__enc_account_billing_division as division,
              visit__account__enc_account_site_id as acct_site_id,
              visit__account__enc_account_company as acct_company,
              visit__account__enc_account_franchise as acct_franchise
        from ${env:ENVIRONMENT}.core_quantum_events_sspp
        where (partition_date_hour_utc >= '${hiveconf:START_DATE_TZ}'
          and partition_date_hour_utc < '${hiveconf:END_DATE_TZ}')
          and visit__account__enc_account_number is not null
          and visit__account__enc_account_number NOT IN('7FbKtybuOWU4/Q0SRInbHA==','GSWNkZXIfDPD6x25Na3i8g==') -- exclude blank or 'pending login'
          and (visit__application_details__application_name in ('SpecMobile')
          AND operation__success = TRUE
          AND message__name = 'loginStop')) a
  ) b
;
