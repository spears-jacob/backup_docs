set hive.vectorized.execution.enabled = false;

USE prod;

DROP TABLE IF EXISTS ${env:TMP_db}.asp_venona_counts_weekly_counts_devices_columns;
CREATE TEMPORARY TABLE ${env:TMP_db}.asp_venona_counts_weekly_counts_devices_columns AS
    SELECT
        COALESCE (visit__account__details__mso,'Unknown') as company,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SpecMobile')  AND LOWER(visit__device__device_type) RLIKE LOWER('and.*') , visit__device__enc_uuid, Null))) AS os_android_devices,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SpecMobile')  AND LOWER(visit__device__device_type) RLIKE LOWER('ip.*') , visit__device__enc_uuid, Null))) AS os_ios_devices,
        'specmobile' AS platform,
        'specmobile' AS domain,
        
        week_ending_date as week_ending_date_denver
    FROM asp_v_venona_events_specmobile
         LEFT JOIN prod_lkp.week_ending ON epoch_converter(cast(received__timestamp as bigint),'America/Denver') = partition_date
    WHERE (week_ending_date >= '${env:START_DATE}'
       AND week_ending_date <  '${env:END_DATE}')
    GROUP BY
        week_ending_date,
        
        COALESCE (visit__account__details__mso,'Unknown')
    ;
INSERT OVERWRITE TABLE prod.asp_venona_counts_weekly
PARTITION(unit,platform,domain,company,week_ending_date_denver,source_table)

    SELECT  value,
            metric,
            
            'devices',
            'specmobile',
            'specmobile',
            company,
            week_ending_date_denver,
            'asp_v_venona_events_specmobile'
    FROM (SELECT  company,
                  week_ending_date_denver,
                  
                  MAP(

                      'os_android|Operating System - Android|SpectrumMobile||', os_android_devices,
                      'os_ios|Operating System - iOS|SpectrumMobile||', os_ios_devices
                  ) as map_column
          FROM ${env:TMP_db}.asp_venona_counts_weekly_counts_devices_columns
        ) as derived_table
    LATERAL VIEW EXPLODE (map_column) exploded_table AS metric, value
;
