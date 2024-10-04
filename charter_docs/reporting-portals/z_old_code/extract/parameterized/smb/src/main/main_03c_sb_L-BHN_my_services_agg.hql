USE ${env:ENVIRONMENT};

-- Drop and rebuild L-BHN My Services temp table --
DROP TABLE IF EXISTS ${env:TMP_db}.sb_bhn_my_services_${env:CADENCE};

-- For troubleshooting queries, remove 'TEMPORARY'
CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.sb_bhn_my_services_${env:CADENCE} AS

SELECT  --Page Views
        SUM(IF(message__category='Page View'
          AND lower(state__view__current_page__page_id) NOT LIKE '%bhnbusiness%'
          AND lower(state__view__current_page__page_id) NOT LIKE '%login%'
          AND lower(state__view__current_page__page_id) NOT LIKE '%businesssolutions.brighthouse.com/home%'
          AND lower(state__view__current_page__page_id) NOT LIKE '%businesssolutions.brighthouse.com/content/mobile/business/home%',1,0))
            AS my_account_page_views,
        'L-BHN' AS company,
        ${env:pf}
FROM  (SELECT ${env:ap} as ${env:pf},
             message__category,
             state__view__current_page__page_id
      FROM asp_v_bhn_my_services_events
      LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
      WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
      and state__view__current_page__page_type = 'small-medium'  -- makes sure that this refers to smb
      ) dictionary
GROUP BY ${env:pf}
;


INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)

SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (SELECT  'my_account_page_views' as metric,
               my_account_page_views AS value,
               'Visits' as unit,
               'sb' as domain,
               company,
               ${env:pf}
       FROM ${env:TMP_db}.sb_bhn_my_services_${env:CADENCE}
    ) q
;
