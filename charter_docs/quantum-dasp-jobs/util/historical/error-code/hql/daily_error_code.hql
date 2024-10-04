USE ${env:DASP_db};

set hive.optimize.sort.dynamic.partition = false;
set hive.exec.dynamic.partition.mode=nonstrict;
set orc.force.positional.evolution=true;

SET hive.merge.mapredfiles=true;
SET hive.merge.tezfiles=true;
--set hive.merge.smallfiles.avgsize=1024000000;
--set hive.merge.size.per.task=1024000000;

ADD JAR s3://pi-global-${env:ENVIRONMENT}-udf-jars/epoch-1.0-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION epoch_converter AS 'Epoch_To_Denver';

INSERT OVERWRITE TABLE asp_daily_error_code PARTITION(date_denver)
SELECT  domain,
        url,
        error_type,
        error_code,
        error_msg,
        client_error_code,
        min(date_denver_week) as date_denver_week,
        min(date_denver_month) as date_denver_month,
        count(1) as error_count,
        date_denver
FROM  (SELECT
              cast(epoch_converter(received__timestamp,'America/Denver') as date) AS date_denver,
              case when date_format(cast(epoch_converter(received__timestamp,'America/Denver') as date),'u')=7 THEN
                        cast(epoch_converter(received__timestamp,'America/Denver') as date)
                   else next_day(cast(epoch_converter(received__timestamp,'America/Denver') as date), 'SUN')
              END as date_denver_week,
              last_day(cast(epoch_converter(received__timestamp,'America/Denver') as date)) date_denver_month,
              visit__application_details__application_name as domain,
              application__error__client_error_code as client_error_code,
              application__error__error_code as error_code,
              application__error__error_type as error_type,
              application__error__error_message as error_msg,
              CASE WHEN state__view__current_page__page_url in (
                      'https://www.spectrumbusiness.net/support/forms/smb_contract_buyout',
                      'https://www.spectrumbusiness.net/support/forms/smb_account_takeover',
                      'https://www.spectrumbusiness.net/support/forms/smb_name_change_confirmation',
                      'https://www.spectrum.net/support/forms/contract_buyout',
                      'https://www.spectrum.net/support/forms/account_takeover',
                      'https://www.spectrum.net/support/forms/name_change',
                      'https://www.spectrum.net/support/forms/name_confirmation'
                   ) THEN state__view__current_page__page_url
              END AS url
        FROM ${env:GLOBAL_DB}.core_quantum_events_sspp
       WHERE (( partition_date_hour_utc  >= '${hiveconf:START_DATE_TZ}' AND partition_date_hour_utc < '${hiveconf:END_DATE_TZ}')
               AND (visit__application_details__application_name IN ('SpecNet','SMB')))
       ) dictionary
WHERE url is not null
and (error_type is not null or error_code is not null or error_msg is not null or client_error_code is not null)
GROUP BY domain,
         url,
         error_type,
         error_code,
         error_msg,
         client_error_code,
         date_denver
;
