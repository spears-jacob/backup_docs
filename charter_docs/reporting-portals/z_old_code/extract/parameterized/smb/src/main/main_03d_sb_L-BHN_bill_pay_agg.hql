USE ${env:ENVIRONMENT};

-- Drop and rebuild L-BHN My Services temp table --
DROP TABLE IF EXISTS ${env:TMP_db}.sb_bhn_bill_pay_${env:CADENCE};

-- For troubleshooting queries, remove 'TEMPORARY'
CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.sb_bhn_bill_pay_${env:CADENCE} AS
-- 2018-03-06 CJ changed payment metrics from size collect set to sum if to count the instances instead of unique visits. In the future we should change the unit from visits to instances.
SELECT  --OBP
        SUM(IF(message__name RLIKE('.*stmtdownload/.*') AND message__category = 'Exit Link' AND visit__settings["post_evar9"] = 'SMB', 1, 0)) AS online_statement_views,
        SUM(IF(ARRAY_CONTAINS(message__feature__name,'Custom Event 36') AND state__view__current_page__page_type='SMB',1,0)) AS one_time_payment_attempts,
        sum(IF(ARRAY_CONTAINS(message__feature__name,'Custom Event 31') AND state__view__current_page__page_type='SMB',1,0)) AS one_time_payments,
        SUM(IF(ARRAY_CONTAINS(message__feature__name,'Custom Event 19') AND state__view__current_page__page_type='SMB',1,0)) AS auto_pay_setup_attempts,
        SUM(IF(ARRAY_CONTAINS(message__feature__name,'Custom Event 24') AND state__view__current_page__page_type='SMB',1,0)) AS auto_pay_setup_successes,
        sum(IF(ARRAY_CONTAINS(message__feature__name,'Custom Event 31') AND state__view__current_page__page_type='SMB',1,0)) AS one_time_payment_updated_wotap,
        SUM(IF(ARRAY_CONTAINS(message__feature__name,'Custom Event 24') AND state__view__current_page__page_type='SMB',1,0)) AS auto_payment_confirm_updated_wotap,
        SIZE(COLLECT_SET(IF(visit__visit_id IS NOT NULL, visit__visit_id, NULL))) AS visits,
        'L-BHN' AS company,
        ${env:pf}
FROM  (SELECT ${env:ap} AS ${env:pf},
          message__name,
          message__category,
          visit__settings,
          message__feature__name,
          state__view__current_page__page_type,
          visit__visit_id
      FROM asp_v_bhn_bill_pay_events
      LEFT JOIN ${env:LKP_db}.${env:fm_lkp} ON ${env:ape}  = partition_date
      WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
      ) dictionary
GROUP BY ${env:pf}
;


INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)

SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (SELECT  'online_statement_views' AS metric,
               online_statement_views AS value,
               'Visits' AS unit,
               'sb' AS domain,
               company,
               ${env:pf}
       FROM ${env:TMP_db}.sb_bhn_bill_pay_${env:CADENCE}
       UNION
       SELECT  'one_time_payment_attempts' AS metric,
               one_time_payment_attempts AS value,
               'Visits' AS unit,
               'sb' AS domain,
               company,
               ${env:pf}
      FROM ${env:TMP_db}.sb_bhn_bill_pay_${env:CADENCE}
      UNION
      SELECT  'one_time_payments' AS metric,
              one_time_payments AS value,
              'Visits' AS unit,
              'sb' AS domain,
              company,
              ${env:pf}
      FROM ${env:TMP_db}.sb_bhn_bill_pay_${env:CADENCE}
      UNION
      SELECT  'auto_pay_setup_attempts' AS metric,
              auto_pay_setup_attempts AS value,
              'Visits' AS unit,
              'sb' AS domain,
              company,
              ${env:pf}
      FROM ${env:TMP_db}.sb_bhn_bill_pay_${env:CADENCE}
      UNION
      SELECT  'auto_pay_setup_successes' AS metric,
              auto_pay_setup_successes AS value,
              'Visits' AS unit,
              'sb' AS domain,
              company,
              ${env:pf}
      FROM ${env:TMP_db}.sb_bhn_bill_pay_${env:CADENCE}
      UNION
      SELECT  'auto_payment_confirm_updated_wotap' AS metric,
              auto_payment_confirm_updated_wotap AS value,
              'Visits' AS unit,
              'sb' AS domain,
              company,
              ${env:pf}
      FROM ${env:TMP_db}.sb_bhn_bill_pay_${env:CADENCE}
      UNION
      SELECT  'one_time_payment_updated_wotap' AS metric,
              one_time_payment_updated_wotap AS value,
              'Visits' AS unit,
              'sb' AS domain,
              company,
              ${env:pf}
      FROM ${env:TMP_db}.sb_bhn_bill_pay_${env:CADENCE}
      UNION
      SELECT  'visits' AS metric,
              visits AS value,
              'Visits' AS unit,
              'sb' AS domain,
              company,
              ${env:pf}
      FROM ${env:TMP_db}.sb_bhn_bill_pay_${env:CADENCE}
    ) q
;
