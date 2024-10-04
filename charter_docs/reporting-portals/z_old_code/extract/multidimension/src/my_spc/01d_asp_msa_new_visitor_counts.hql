USE ${env:ENVIRONMENT};

set hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set mapreduce.input.fileinputformat.split.maxsize=5368709120;
set mapreduce.input.fileinputformat.split.minsize=5368709120;

-- My Spectrum -- Daily New Visitor
INSERT OVERWRITE TABLE asp_metrics_detail
PARTITION(metric,platform,domain,company,date_denver,source_table)

select
      size(collect_set (visitor)) AS value,
      'unique new visitor' as detail,
      'devices' as unit,
      'new visitor' as metric,
      'asp' AS platform,
      'app' AS domain,
      legacy_footprint as company,
      date_den,
      'asp_v_venona_events_portals_msa' as source_table
FROM
      (SELECT
             COALESCE (visit__account__details__mso,'Unknown') AS legacy_footprint,
             visit__device__enc_uuid as visitor,
             epoch_converter(cast(min(message__timestamp) as bigint),'America/Denver') as date_den
        FROM prod.asp_v_venona_events_portals_msa
       WHERE (partition_date_hour_utc >= '${env:START_DATE_TZ}'
          AND partition_date_hour_utc < '${env:END_DATE_TZ}'
         AND LOWER(message__name)=LOWER('pageView')
         AND LOWER(state__view__current_page__page_name) = lower('billingHome') )
       GROUP BY
             COALESCE (visit__account__details__mso,'Unknown'),
             visit__device__enc_uuid) a
group by legacy_footprint,
         date_den;
