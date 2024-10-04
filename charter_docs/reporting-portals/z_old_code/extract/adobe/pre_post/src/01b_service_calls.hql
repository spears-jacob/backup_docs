USE ${env:ENVIRONMENT};

-- dev My Spectrum -- service call succeses, failures, and advisories
INSERT OVERWRITE TABLE asp_metrics_detail
PARTITION(metric,platform,domain,company,date_denver,source_table)
    SELECT
        count(message__name) AS value,
        message__name as detail,
        'instances' as unit,
        CASE
          WHEN message__name Rlike '.*Failure.*' THEN 'service_call_failures'
          WHEN message__name Rlike '.*Success.*' THEN 'service_call_successes'
          ELSE 'service_call_advise'
        END as metric,
        'asp' AS platform,
        'app' AS domain,
        CASE
           WHEN state__view__previous_page__sub_section = 'BH' THEN 'BHN'
           WHEN state__view__previous_page__sub_section = 'CHARTER' THEN 'CHTR'
           WHEN state__view__previous_page__sub_section = 'TWC' THEN 'TWCC'
           WHEN state__view__previous_page__sub_section IS NULL THEN 'UNAUTH'
           ELSE 'UNDEFINED'
        END as company,
        epoch_converter(cast(message__timestamp * 1000 as bigint),'America/Denver') as date_denver,
        'asp_v_spc_app_dev_events' as source_table
    FROM asp_v_spc_app_dev_events
    WHERE partition_date_hour_utc BETWEEN '${env:START_DATE_TZ}' AND '${env:END_DATE_TZ}'
    AND message__category = 'Custom Link'
    and message__name RLIKE '.*Svc Call (Failure|Success).*'
    GROUP BY  message__name,
              epoch_converter(cast(message__timestamp * 1000 as bigint),'America/Denver'),
              CASE
                 WHEN state__view__previous_page__sub_section = 'BH' THEN 'BHN'
                 WHEN state__view__previous_page__sub_section = 'CHARTER' THEN 'CHTR'
                 WHEN state__view__previous_page__sub_section = 'TWC' THEN 'TWCC'
                 WHEN state__view__previous_page__sub_section IS NULL THEN 'UNAUTH'
                 ELSE 'UNDEFINED'
              END
;

-- production My Spectrum -- service call succeses, failures, and advisories
INSERT OVERWRITE TABLE asp_metrics_detail
PARTITION(metric,platform,domain,company,date_denver,source_table)
    SELECT
        count(message__name) AS value,
        message__name as detail,
        'instances' as unit,
        CASE
          WHEN message__name Rlike '.*Failure.*' THEN 'service_call_failures'
          WHEN message__name Rlike '.*Success.*' THEN 'service_call_successes'
          ELSE 'service_call_advise'
        END as metric,
        'asp' AS platform,
        'app' AS domain,
        CASE
           WHEN state__view__previous_page__sub_section = 'BH' THEN 'BHN'
           WHEN state__view__previous_page__sub_section = 'CHARTER' THEN 'CHTR'
           WHEN state__view__previous_page__sub_section = 'TWC' THEN 'TWCC'
           WHEN state__view__previous_page__sub_section IS NULL THEN 'UNAUTH'
           ELSE 'UNDEFINED'
        END as company,
        epoch_converter(cast(message__timestamp * 1000 as bigint),'America/Denver') as date_denver,
        'asp_v_spc_app_events' as source_table
    FROM asp_v_spc_app_events
    WHERE partition_date_hour_utc BETWEEN '${env:START_DATE_TZ}' AND '${env:END_DATE_TZ}'
    AND message__category = 'Custom Link'
    and message__name RLIKE '.*Svc Call (Failure|Success).*'
    GROUP BY  message__name,
              epoch_converter(cast(message__timestamp * 1000 as bigint),'America/Denver'),
              CASE
                 WHEN state__view__previous_page__sub_section = 'BH' THEN 'BHN'
                 WHEN state__view__previous_page__sub_section = 'CHARTER' THEN 'CHTR'
                 WHEN state__view__previous_page__sub_section = 'TWC' THEN 'TWCC'
                 WHEN state__view__previous_page__sub_section IS NULL THEN 'UNAUTH'
                 ELSE 'UNDEFINED'
              END
;
