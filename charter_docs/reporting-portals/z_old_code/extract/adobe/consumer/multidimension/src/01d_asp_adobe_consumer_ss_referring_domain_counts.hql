USE ${env:ENVIRONMENT};

INSERT OVERWRITE TABLE asp_metrics_detail
PARTITION(metric,platform,domain,company,date_denver,source_table)

--ss_referring_domain is for support_section_referring_domain
SELECT
      SIZE(COLLECT_SET(visit__device__enc_uuid)) AS value,
      state__view__previous_page__page_id as detail,
      'devices' AS unit,
      'ss_referring_domain' as metric,
      'asp' AS platform,
      'resi' AS domain,
      'L-CHTR' as company,
      epoch_converter(cast(message__timestamp * 1000 as bigint),'America/Denver') as date_denver,
      'asp_v_net_events' as source_table
 FROM asp_v_net_events
WHERE (partition_date >= '${env:START_DATE}'
  AND partition_date < '${env:END_DATE}')
  AND LOWER(state__view__current_page__section) = 'support'
  AND message__category='Page View'
GROUP BY
      epoch_converter(cast(message__timestamp * 1000 as bigint),'America/Denver')
    , state__view__previous_page__page_id
;
