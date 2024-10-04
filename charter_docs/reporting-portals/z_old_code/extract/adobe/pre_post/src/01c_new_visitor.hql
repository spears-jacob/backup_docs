USE ${env:ENVIRONMENT};

-- dev My Spectrum -- service call succeses, failures, and advisories
INSERT OVERWRITE TABLE asp_counts_daily
PARTITION(unit,platform,domain,company,date_denver,source_table)

    SELECT  value,
            'new_logged_in_visitor',
            'devices',
            'asp',
            'app',
            company,
            date_denver,
            'asp_v_spc_app_dev_events'
    FROM (select
                  legacy_footprint as company,
                  date_den as date_denver,
                  size(collect_set (visitor)) AS value
            FROM
                 (SELECT
                         visit__settings["post_prop7"] AS legacy_footprint,
                         visit__device__enc_uuid as visitor,
                         epoch_converter(cast(min(message__timestamp)*1000 as bigint),'America/Denver') as date_den
                    FROM asp_v_spc_app_dev_events
                    WHERE (partition_date_hour_utc >= '${env:START_DATE_TZ}'
                       AND partition_date_hour_utc <  '${env:END_DATE_TZ}')
                     AND message__category='Page View'
                     and message__name = 'Bill Pay View'
                   GROUP BY
                         visit__settings["post_prop7"],
                         visit__device__enc_uuid) a
           group by legacy_footprint,
                    date_den
           ORDER BY legacy_footprint,
                    date_den) as derived_table
;

-- production My Spectrum -- service call succeses, failures, and advisories
INSERT OVERWRITE TABLE asp_counts_daily
PARTITION(unit,platform,domain,company,date_denver,source_table)

    SELECT  value,
            'new_logged_in_visitor',
            'devices',
            'asp',
            'app',
            company,
            date_denver,
            'asp_v_spc_app_events'
    FROM (select
                  legacy_footprint as company,
                  date_den as date_denver,
                  size(collect_set (visitor)) AS value
            FROM
                 (SELECT
                         visit__settings["post_prop7"] AS legacy_footprint,
                         visit__device__enc_uuid as visitor,
                         epoch_converter(cast(min(message__timestamp)*1000 as bigint),'America/Denver') as date_den
                    FROM asp_v_spc_app_events
                    WHERE (partition_date_hour_utc >= '${env:START_DATE_TZ}'
                       AND partition_date_hour_utc <  '${env:END_DATE_TZ}')
                     AND message__category='Page View'
                     and message__name = 'Bill Pay View'
                   GROUP BY
                         visit__settings["post_prop7"],
                         visit__device__enc_uuid) a
           group by legacy_footprint,
                    date_den
           ORDER BY legacy_footprint,
                    date_den) as derived_table
;
