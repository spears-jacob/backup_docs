-------------------------------------------------------------------------------

--Populates the temp table new_id_counts_all with new id counts by company
--Populates data from various sources for L-CHTR, L-BHN, and L-TWC

-------------------------------------------------------------------------------

USE ${env:ENVIRONMENT};

--------------------------------------------------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin L-CHTR New ID Calculations --

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)

SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (
        SELECT
          SIZE(COLLECT_SET(IF(state__view__current_page__name IN('my-account.create-id-final.bam','my-account.create-id-final.btm') AND message__category = 'Page View', visit__visit_id,NULL)))
          +
          SIZE(COLLECT_SET(IF(state__view__current_page__name IN('my-account.create-id-final.nbtm') AND message__category = 'Page View', visit__visit_id,NULL)))
        AS value,
        'visits' as unit,
        'resi' as domain,
        'L-CHTR' AS company,
        ${env:pf},
        "new_ids_charter_count_all" as metric
        FROM
            ( SELECt ${env:ap} as ${env:pf},
                     state__view__current_page__name,
                     message__category,
                     visit__visit_id
              FROM asp_v_net_events ne
              LEFT JOIN ${env:LKP_db}.${env:fm_lkp} fm on ne.partition_date  = fm.partition_date
              WHERE (ne.partition_date >= ("${env:START_DATE}") AND ne.partition_date < ("${env:END_DATE}"))
              AND state__view__current_page__name IN ('my-account.create-id-final.bam','my-account.create-id-final.btm','my-account.create-id-final.nbtm' )
              AND message__category = 'Page View'
            ) dictionary
        GROUP BY ${env:pf} ) q
;

SELECT '*****-- End L-CHTR New ID Calculations --*****' -- 95.406 seconds
;

-- End L-CHTR New ID Calculations --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin L-BHN New ID Calculations --

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)

SELECT value, unit, domain, company, ${env:pf}, metric
FROM  ( SELECT  MAX(IF(site_id='003' and role='Administrator'
                      ,CAST(REGEXP_REPLACE(REGEXP_REPLACE(new_accounts,'"',''),',','') AS BIGINT)
                      ,NULL)
                    ) AS value,
                'visits' as unit,
                'resi' as domain,
                "new_ids_charter_count_all" as metric,
                'L-BHN' AS company,
                year_month as ${env:pf}
        FROM ${env:TMP_db}.sbnet_exec_monthly_bhn_accounts_manual
        WHERE year_month = "${env:ym}"
        AND "${env:CADENCE}" <> 'daily'
        GROUP BY year_month
      ) q
;


SELECT '*****-- End L-BHN New ID Calculations --*****' -- 95.406 seconds
;

-- End L-BHN New ID Calculations --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin L-TWC New ID Calculations --

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)

SELECT value, unit, domain, company, ${env:pf}, metric
FROM  ( SELECT SUM(IF(array_contains(message__feature__name,'Custom Event 5'),1,0)) AS value,
              'visits' as unit,
              'resi' as domain,
              'L-TWC' AS company,
              ${env:pf},
              "new_ids_charter_count_all" as metric
        FROM
                (SELECT message__feature__name,
                        ${env:ap} as ${env:pf}
                  FROM asp_v_twc_residential_global_events
                  LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
                  WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
                ) dictionary
        GROUP BY ${env:pf}
      ) q
;

SELECT '*****-- End L-TWC New ID Calculations --*****' -- 95.406 seconds
;

-- End L-TWC New ID Calculations --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- End New ID Calculations --

SELECT '*****-- End New ID Calculations --*****' -- 95.406 seconds
;
