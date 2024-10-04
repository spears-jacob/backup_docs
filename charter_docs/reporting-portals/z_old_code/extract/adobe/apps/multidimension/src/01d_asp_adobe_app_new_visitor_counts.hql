USE ${env:ENVIRONMENT};

set hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set mapreduce.input.fileinputformat.split.maxsize=5368709120;
set mapreduce.input.fileinputformat.split.minsize=5368709120;

set hive.exec.dynamic.partition.mode=nonstrict;

-- My Spectrum -- New Visitor
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
      'asp_v_spc_app_events' as source_table
FROM
      (SELECT
             visit__settings["post_prop7"] AS legacy_footprint,
             visit__device__enc_uuid as visitor,
             epoch_converter(cast(min(message__timestamp)*1000 as bigint),'America/Denver') as date_den
        FROM prod.asp_v_spc_app_events
       WHERE (partition_date_utc >= '2018-05-03' and partition_date_utc < '2018-07-19')
         AND (epoch_converter(cast(message__timestamp*1000 as bigint),'America/Denver') >= '2018-05-03'
         and  epoch_converter(cast(message__timestamp*1000 as bigint),'America/Denver') < '2018-07-19')
         AND message__category='Page View'
         and message__name = 'Bill Pay View'
       GROUP BY
             visit__settings["post_prop7"],
             visit__device__enc_uuid) a
group by legacy_footprint,
         date_den;
