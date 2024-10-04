USE ${env:ENVIRONMENT};

-- My Spectrum -- service_call_successes
INSERT OVERWRITE TABLE asp_metrics_detail
PARTITION(metric,platform,domain,company,date_denver,source_table)

SELECT
    count(message__name) AS value,
    concat(application__api__api_category,'-',
           application__api__api_name,'-',
           state__view__current_page__page_name,'-',
           application__api__response_code
           )    as detail,
    'instances' as unit,
    'service_call_successes' as metric,
    'asp' AS platform,
    'app' AS domain,
    COALESCE (visit__account__details__mso,'Unknown') company,
    epoch_converter(cast(received__timestamp as bigint),'America/Denver') as date_denver,
    'asp_v_venona_events_portals_msa' as source_table
FROM asp_v_venona_events_portals_msa
WHERE (partition_date_hour_utc >= '${env:START_DATE_TZ}'
   AND partition_date_hour_utc < '${env:END_DATE_TZ}'
   AND LOWER(visit__application_details__application_name) = LOWER('MySpectrum')
   AND LOWER(message__name) = LOWER('apiCall')
   AND (LOWER(application__api__response_code) RLIKE '2.*'
    OR application__api__response_code = '429'))
GROUP BY
         concat(application__api__api_category,'-',
                application__api__api_name,'-',
                state__view__current_page__page_name,'-',
                application__api__response_code),
         COALESCE (visit__account__details__mso,'Unknown'),
         epoch_converter(cast(received__timestamp as bigint),'America/Denver')
;
