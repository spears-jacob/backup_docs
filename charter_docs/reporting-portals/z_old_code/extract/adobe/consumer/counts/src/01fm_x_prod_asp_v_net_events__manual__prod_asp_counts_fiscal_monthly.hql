------------------------------------------------------------------------------
-- Adobe hold-overs which are too complex for counts.tsv
------------------------------------------------------------------------------
USE ${env:ENVIRONMENT};

drop table if exists ${env:TMP_db}.asp_consumer_agg_long_metric_names;

CREATE TEMPORARY TABLE ${env:TMP_db}.asp_consumer_agg_long_metric_names as
SELECT
  'visits' as unit,
  'asp' AS platform,
  'resi' AS domain,
  'L-CHTR' as company,
  year_fiscal_month_denver,
  'asp_v_net_events' as source_table,


  -- new id attempts
  -- new_ids_created_attempts|New IDs Created Attempts|Spectrum.net_adobe||

  SIZE(COLLECT_SET(IF(state__view__current_page__name IN('my-account.create-id-1.bam','my-account.create-id') AND message__category = 'Page View', visit__visit_id,NULL)))
  -
  SIZE(COLLECT_SET(IF(message__name = 'Sign-In-Now'
                  AND message__category = 'Custom Link'
                  AND state__view__current_page__name IN('my-account.create-id-2.btm','my-account.create-id-2.bam'), visit__visit_id,NULL)))
  +
  SIZE(COLLECT_SET(IF(state__view__current_page__name IN('my-account.create-id-1.nbtm') AND message__category = 'Page View', visit__visit_id,NULL)))
  -
  SIZE(COLLECT_SET(IF(message__name = 'Sign-In-Now'
                  AND message__category = 'Custom Link'
                  AND state__view__current_page__name IN('my-account.create-id-2.nbtm'), visit__visit_id,NULL)))
  -
  SIZE(COLLECT_SET(IF(state__view__current_page__name IN('Create.redirectTWC') AND message__category = 'Page View', visit__visit_id,NULL)))
  -
  SIZE(COLLECT_SET(IF(state__view__current_page__name IN('Create.redirectBHN') AND message__category = 'Page View', visit__visit_id,NULL)))
  AS attempts_create_id_count_all,

-- news ids created success
-- ids_created_successes|New IDs Created Successes|Spectrum.net_adobe||

  SIZE(COLLECT_SET(IF(state__view__current_page__name IN('my-account.create-id-final.bam','my-account.create-id-final.btm') AND message__category = 'Page View', visit__visit_id,NULL)))
          +
  SIZE(COLLECT_SET(IF(state__view__current_page__name IN('my-account.create-id-final.nbtm') AND message__category = 'Page View', visit__visit_id,NULL)))
  as new_ids_charter_count_all,

--- user recovery success
--  username_recovery_successes|Username Recovery Successes|Spectrum.net_adobe||

  SIZE(COLLECT_SET(if((state__view__current_page__name IN('Recover-final2.btm','Recover-final1.bam','Recover-final2.bam') AND message__category = 'Page View'),visit__visit_id,NULL)))
  +
    SIZE(COLLECT_SET(if((state__view__current_page__name = 'Recover-final1.nbtm') AND (message__category = 'Page View'), visit__visit_id,NULL)))
  + SIZE(COLLECT_SET(IF(state__view__current_page__name = 'Recover-final2.nbtm' AND (message__category = 'Page View'), visit__visit_id,NULL)))
  AS successful_username_recovery_count_all

FROM  (SELECt  fiscal_month as year_fiscal_month_denver,
               message__name,
               message__category,
               visit__visit_id,
               state__view__current_page__name
       FROM asp_v_net_events ne
       LEFT JOIN prod_lkp.chtr_fiscal_month fm on ne.partition_date = fm.partition_date
       WHERE (ne.partition_date >= ("${env:START_DATE}") AND ne.partition_date < ("${env:END_DATE}"))
     ) dictionary
GROUP BY year_fiscal_month_denver
;

----- Insert into agg table -------
INSERT INTO TABLE prod.asp_counts_fiscal_monthly
PARTITION(unit,platform,domain,company,year_fiscal_month_denver,source_table)
  SELECT  attempts_create_id_count_all as value,
         'new_ids_created_attempts|New IDs Created Attempts|Spectrum.net_adobe||' as metric,
          unit,
          platform,
          domain,
          company,
          year_fiscal_month_denver,
          source_table
  FROM ${env:TMP_db}.asp_consumer_agg_long_metric_names
;

INSERT INTO TABLE prod.asp_counts_fiscal_monthly
PARTITION(unit,platform,domain,company,year_fiscal_month_denver,source_table)
  SELECT  new_ids_charter_count_all as value,
         'ids_created_successes|New IDs Created Successes|Spectrum.net_adobe||' as metric,
          unit,
          platform,
          domain,
          company,
          year_fiscal_month_denver,
          source_table
  FROM ${env:TMP_db}.asp_consumer_agg_long_metric_names
;

INSERT INTO TABLE prod.asp_counts_fiscal_monthly
PARTITION(unit,platform,domain,company,year_fiscal_month_denver,source_table)
  SELECT successful_username_recovery_count_all as value,
        'username_recovery_successes|Username Recovery Successes|Spectrum.net_adobe||' as metric,
         unit,
         platform,
         domain,
         company,
         year_fiscal_month_denver,
         source_table
  FROM ${env:TMP_db}.asp_consumer_agg_long_metric_names
;
