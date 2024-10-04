USE ${env:ENVIRONMENT};

-- Spectrum.net -- select action by state view current page elements standardized name
INSERT OVERWRITE TABLE asp_metrics_detail
PARTITION(metric,platform,domain,company,date_denver,source_table)

SELECT
    count(message__name) AS value,
    state__view__current_page__elements__standardized_name as detail,
    'instances' as unit,
    'Select Actions' as metric,
    'asp' AS platform,
    'resi' AS domain,
    COALESCE (visit__account__details__mso,'Unknown') company,
    epoch_converter(cast(received__timestamp as bigint),'America/Denver') as date_denver,
    'asp_v_venona_events_portals_specnet' as source_table
FROM asp_v_venona_events_portals_specnet
WHERE (partition_date_hour_utc >= '${env:START_DATE_TZ}'
   AND partition_date_hour_utc < '${env:END_DATE_TZ}'
   AND message__name = 'selectAction')
GROUP BY state__view__current_page__elements__standardized_name,
         COALESCE (visit__account__details__mso,'Unknown'),
         epoch_converter(cast(received__timestamp as bigint),'America/Denver')
;
