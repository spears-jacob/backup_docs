USE ${env:ENVIRONMENT};

--ss_support_articles_unauth is for support_section_support_articles_unauth
INSERT OVERWRITE TABLE asp_metrics_detail
PARTITION(metric,platform,domain,company,date_denver,source_table)

SELECT
      SIZE(COLLECT_SET(visit__device__enc_uuid)) AS value,
      state__view__current_page__name as detail,
      'devices' AS unit,
      'ss_support_articles_unauth' as metric,
      'asp' AS platform,
      'resi' AS domain,
      'L-CHTR' as company,
      epoch_converter(cast(message__timestamp * 1000 as bigint),'America/Denver') as date_denver,
      'asp_v_net_events' as source_table
 FROM asp_v_net_events
WHERE (partition_date >= '${env:START_DATE}'
  AND partition_date < '${env:END_DATE}')
  AND LOWER(state__view__current_page__section) = 'support'
  AND message__category = 'Page View'
  AND visit__account__enc_account_number is null
GROUP BY
      epoch_converter(cast(message__timestamp * 1000 as bigint),'America/Denver')
    , state__view__current_page__name
;
