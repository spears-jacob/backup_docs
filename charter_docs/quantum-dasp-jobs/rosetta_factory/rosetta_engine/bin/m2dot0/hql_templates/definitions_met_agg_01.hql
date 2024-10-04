USE ${env:DASP_db};

set mapreduce.input.fileinputformat.split.maxsize=5368709120;
set mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.optimize.sort.dynamic.partition=false;
set hive.exec.dynamic.partition.mode=nonstrict;
set orc.force.positional.evolution=true;
SET hive.merge.tezfiles=true;
set hive.merge.smallfiles.avgsize=2048000000;
set hive.merge.size.per.task=2048000000;

ADD JAR ${env:ARTIFACTS_PATH}/jars/epoch-1.0-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION epoch_converter AS 'Epoch_To_Denver';

INSERT OVERWRITE TABLE ${env:DASP_db}.asp_m2dot0_metric_agg PARTITION (partition_date_utc)

select
        visit__application_details__application_name,
        visit__application_details__application_type,
        visit__application_details__app_version,
          max(case
          when visit__account__details__service_subscriptions is null
              THEN '0 - null'
          WHEN (lower(visit__account__details__service_subscriptions["mobile"]) in ('active', 'true'))
            AND (
              lower(visit__account__details__service_subscriptions["internet"]) in ('active', 'true') OR
              lower(visit__account__details__service_subscriptions["tv"]) in ('active', 'true') OR
              lower(visit__account__details__service_subscriptions["voice"]) in ('active', 'true')
            ) THEN '4 - coreAndMobile'
          WHEN
            (lower(visit__account__details__service_subscriptions["mobile"]) in ('active','true'))
              THEN '3 - mobile'
          WHEN
                (
              lower(visit__account__details__service_subscriptions["internet"]) in ('active', 'true') OR
              lower(visit__account__details__service_subscriptions["tv"]) in ('active', 'true') OR
              lower(visit__account__details__service_subscriptions["voice"]) in ('active', 'true')
            ) THEN '2 - core'
          ELSE '1 - other'
          END) AS agg_CUSTOM_visit__account__details__service_subscriptions,
          max(visit__account__configuration_factors) as agg_visit__account__configuration_factors,
          case when
            (min(case
              when visit__account__configuration_factors is null then 'null'
              when visit__account__configuration_factors Rlike 'MG|M2'
              then 'Migrated'
              else 'Not Migrated'
              end)) = 'Migrated'
            AND
            max(case
              when visit__application_details__application_name <> 'MySpectrum' then 'N/A'
              when visit__application_details__application_name = 'MySpectrum' and
              (cast(regexp_replace(visit__application_details__app_version,'[^0-9\\s]','') as int) >= 9150
              or cast(substr(visit__application_details__app_version,1,instr(visit__application_details__app_version,'.')-1) as int) > 9
              ) then 'New UI'
              else 'Old UI'
              end) <> 'Old UI'
            THEN 'Mobile 2.0 Customer'
            ELSE 'Not Mobile / Not Migrated'
          END as agg_custom_customer_group,
          max(case
          when visit__account__details__mso = 'CHTR' then 'CHR'
          when visit__account__details__mso = 'CHARTER' then 'CHR'
          when visit__account__details__mso = 'BH' then 'BHN'
          when visit__account__details__mso like '%TWC%' then 'TWC'
          else visit__account__details__mso end) as agg_visit__account__details__mso,
          visit__account__enc_account_number,
          visit__account__enc_mobile_account_number,
          visit__visit_id,
          visit__visit_start_timestamp,
          visit__device__enc_uuid,
