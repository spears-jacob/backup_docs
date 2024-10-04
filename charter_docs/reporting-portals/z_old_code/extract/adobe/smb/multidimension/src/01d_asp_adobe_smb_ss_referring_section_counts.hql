USE ${env:ENVIRONMENT};

--ss_referring_site_section s for support_section_referring_site_section
INSERT OVERWRITE TABLE asp_metrics_detail
PARTITION(metric,platform,domain,company,date_denver,source_table)

SELECT
      SIZE(COLLECT_SET(visit__device__enc_uuid)) AS value,
      state__view__previous_page__section as detail,
      'devices' AS unit,
      'ss_referring_site_section' as metric,
      'asp' AS platform,
      'smb' AS domain,
      'L-CHTR' as company,
      epoch_converter(cast(message__timestamp * 1000 as bigint),'America/Denver') as date_denver,
      'asp_v_sbnet_events' as source_table
 FROM asp_v_sbnet_events
WHERE partition_date_utc >= '${env:START_DATE_TZ}'
  AND partition_date_utc < '${env:END_DATE_TZ}'
  AND LOWER(state__view__current_page__section) = 'support'
  AND message__category = 'Page View'
GROUP BY
      epoch_converter(cast(message__timestamp * 1000 as bigint),'America/Denver')
    , state__view__previous_page__section
;
