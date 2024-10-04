set hive.vectorized.execution.enabled = false;

USE prod;

DROP TABLE IF EXISTS ${env:TMP_db}.asp_venona_counts_fiscal_monthly_counts_visits_columns;
CREATE TEMPORARY TABLE ${env:TMP_db}.asp_venona_counts_fiscal_monthly_counts_visits_columns AS
    SELECT
        COALESCE (visit__account__details__mso,'Unknown') as company,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SpecMobile')  AND LOWER(visit__device__device_type) RLIKE LOWER('and.*') , visit__visit_id, Null))) AS os_android_visits,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SpecMobile')  AND LOWER(visit__device__device_type) RLIKE LOWER('ip.*') , visit__visit_id, Null))) AS os_ios_visits,
        'specmobile' AS platform,
        'specmobile' AS domain,
        
        fiscal_month as year_fiscal_month_denver
    FROM asp_v_venona_events_specmobile
         LEFT JOIN prod_lkp.chtr_fiscal_month ON epoch_converter(cast(received__timestamp as bigint),'America/Denver') = partition_date
    WHERE (partition_date_hour_utc >= '${env:START_DATE_TZ}'
       AND partition_date_hour_utc <  '${env:END_DATE_TZ}')
    GROUP BY
        fiscal_month,
        
        COALESCE (visit__account__details__mso,'Unknown')
    ;
INSERT OVERWRITE TABLE prod.asp_venona_counts_fiscal_monthly
PARTITION(unit,platform,domain,company,year_fiscal_month_denver,source_table)

    SELECT  value,
            metric,
            
            'visits',
            'specmobile',
            'specmobile',
            company,
            year_fiscal_month_denver,
            'asp_v_venona_events_specmobile'
    FROM (SELECT  company,
                  year_fiscal_month_denver,
                  
                  MAP(

                      'os_android|Operating System - Android|SpectrumMobile||', os_android_visits,
                      'os_ios|Operating System - iOS|SpectrumMobile||', os_ios_visits
                  ) as map_column
          FROM ${env:TMP_db}.asp_venona_counts_fiscal_monthly_counts_visits_columns
        ) as derived_table
    LATERAL VIEW EXPLODE (map_column) exploded_table AS metric, value
;
