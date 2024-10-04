set hive.vectorized.execution.enabled = false;

USE prod;

DROP TABLE IF EXISTS ${env:TMP_db}.asp_venona_counts_hourly_counts_instances_columns;
CREATE TEMPORARY TABLE ${env:TMP_db}.asp_venona_counts_hourly_counts_instances_columns AS
    SELECT
        COALESCE (visit__account__details__mso,'Unknown') as company,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('SpecMobile')  AND LOWER(visit__device__device_type) RLIKE LOWER('and.*') , 1, 0)) AS os_android,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('SpecMobile')  AND LOWER(visit__device__device_type) RLIKE LOWER('ip.*') , 1, 0)) AS os_ios,
        'specmobile' AS platform,
        'specmobile' AS domain,
        prod.epoch_datehour(cast(received__timestamp as bigint),'America/Denver') as date_hour_denver,
        epoch_converter(cast(received__timestamp as bigint),'America/Denver') as date_denver
    FROM asp_v_venona_events_specmobile
         
    WHERE (partition_date_hour_utc >= '${env:START_DATE_TZ}'
       AND partition_date_hour_utc <  '${env:END_DATE_TZ}')
    GROUP BY
        epoch_converter(cast(received__timestamp as bigint),'America/Denver'),
        prod.epoch_datehour(cast(received__timestamp as bigint),'America/Denver'),
        COALESCE (visit__account__details__mso,'Unknown')
    ;
INSERT OVERWRITE TABLE prod.asp_venona_counts_hourly
PARTITION(unit,platform,domain,company,date_denver,source_table)

    SELECT  value,
            metric,
            date_hour_denver,
            'instances',
            'specmobile',
            'specmobile',
            company,
            date_denver,
            'asp_v_venona_events_specmobile'
    FROM (SELECT  company,
                  date_denver,
                  date_hour_denver,
                  MAP(

                      'os_android|Operating System - Android|SpectrumMobile||', os_android,
                      'os_ios|Operating System - iOS|SpectrumMobile||', os_ios
                  ) as map_column
          FROM ${env:TMP_db}.asp_venona_counts_hourly_counts_instances_columns
        ) as derived_table
    LATERAL VIEW EXPLODE (map_column) exploded_table AS metric, value
;
