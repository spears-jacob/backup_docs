USE ${env:ENVIRONMENT};

-- My Spectrum -- modal views by modal name
INSERT OVERWRITE TABLE asp_metrics_detail
PARTITION(metric,platform,domain,company,date_denver,source_table)

SELECT
    count(message__name) AS value,
    state__view__modal__name as detail,
    'instances' as unit,
    'Modal Views' as metric,
    'asp' AS platform,
    'app' AS domain,
    COALESCE (visit__account__details__mso,'Unknown') company,
    epoch_converter(cast(received__timestamp as bigint),'America/Denver') as date_denver,
    'asp_v_venona_events_portals_msa' as source_table
FROM asp_v_venona_events_portals_msa
WHERE (partition_date_hour_utc >= '${env:START_DATE_TZ}'
   AND partition_date_hour_utc < '${env:END_DATE_TZ}'
   AND message__name = 'modalView')
GROUP BY state__view__modal__name,
         COALESCE (visit__account__details__mso,'Unknown'),
         epoch_converter(cast(received__timestamp as bigint),'America/Denver')
;
