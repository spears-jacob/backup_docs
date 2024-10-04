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
  date_denver,
  'asp_v_net_events' as source_table,
-- new ids including sub accounts - new_ids_incl_sub_accts
SIZE(COLLECT_SET(IF(UPPER(state__view__current_page__name)
      IN('MY-ACCOUNT.CREATE-ID-FINAL.BTM')
      AND message__category = 'Page View',visit__visit_id,NULL)))
+
SIZE(COLLECT_SET(IF(UPPER(state__view__current_page__name)
      IN('MY-ACCOUNT.CREATE-ID-FINAL.NBTM')
      AND message__category = 'Page View',visit__visit_id,NULL)))
+
SIZE(COLLECT_SET(IF(UPPER(state__view__current_page__name)
      IN('MY-ACCOUNT.CREATE-ID-FINAL.BAM')
      AND message__category = 'Page View',visit__visit_id,NULL)))
+
SUM(IF(state__view__current_page__page_id
      = 'https://www.spectrum.net/my-account/add-user#/confirmation'
      AND message__category = 'Page View',1,0)) AS new_ids_incl_sub_accts,
-- combined_credential_recoveries
SIZE(COLLECT_SET(IF(UPPER(state__view__current_page__name) = 'RECOVERRESET-FINAL1.BTM' AND message__category = 'Page View', visit__visit_id,NULL)))
 +
SIZE(COLLECT_SET(IF(UPPER(state__view__current_page__name) = 'RECOVER-FINAL1.BTM' AND message__category = 'Page View', visit__visit_id,NULL)))
 +
SIZE(COLLECT_SET(IF(UPPER(state__view__current_page__name) = 'RECOVERRESET-FINAL1.BAM' AND message__category = 'Page View', visit__visit_id,NULL)))
 +
SIZE(COLLECT_SET(IF(UPPER(state__view__current_page__name) = 'RECOVER-FINAL1.BAM' AND message__category = 'Page View', visit__visit_id,NULL)))
 +
SIZE(COLLECT_SET(IF(UPPER(state__view__current_page__name) = 'RECOVERRESET-FINAL1.NBTM' AND message__category = 'Page View', visit__visit_id,NULL)))
 +
SIZE(COLLECT_SET(IF(UPPER(state__view__current_page__name) = 'RECOVER-FINAL1.NBTM' AND message__category = 'Page View', visit__visit_id,NULL)))
 +
SIZE(COLLECT_SET(IF(UPPER(state__view__current_page__name) = 'RECOVER-FINAL2.BAM' AND message__category = 'Page View', visit__visit_id,NULL)))
 +
SIZE(COLLECT_SET(IF(UPPER(state__view__current_page__name) = 'RECOVERRESET-FINAL2.BAM' AND message__category = 'Page View', visit__visit_id,NULL)))
 +
SIZE(COLLECT_SET(IF(UPPER(state__view__current_page__name) = 'RESET-FINAL.BAM' AND message__category = 'Page View', visit__visit_id,NULL)))
 +
SIZE(COLLECT_SET(IF(UPPER(state__view__current_page__name) = 'RECOVER-FINAL2.NBTM' AND message__category = 'Page View', visit__visit_id,NULL)))
 +
SIZE(COLLECT_SET(IF(UPPER(state__view__current_page__name) = 'RECOVERRESET-FINAL2.NBTM' AND message__category = 'Page View', visit__visit_id,NULL)))
 +
SIZE(COLLECT_SET(IF(UPPER(state__view__current_page__name) = 'RESET-FINAL.BTM' AND message__category = 'Page View', visit__visit_id,NULL)))
 +
SIZE(COLLECT_SET(IF(UPPER(state__view__current_page__name) = 'RECOVER-FINAL2.BTM' AND message__category = 'Page View', visit__visit_id,NULL)))
 +
SIZE(COLLECT_SET(IF(UPPER(state__view__current_page__name) = 'RECOVERRESET-FINAL2.BTM' AND message__category = 'Page View', visit__visit_id,NULL)))
 +
SIZE(COLLECT_SET(IF(UPPER(state__view__current_page__name) = 'RESET-FINAL.NBTM' AND message__category = 'Page View', visit__visit_id,NULL)))
AS combined_credential_recoveries

FROM  (SELECt  partition_date as date_denver,
               state__view__current_page__page_id,
               message__category,
               visit__visit_id,
               state__view__current_page__name
       FROM asp_v_net_events ne
       WHERE (ne.partition_date >= ("${env:START_DATE}") AND ne.partition_date < ("${env:END_DATE}"))
     ) dictionary
GROUP BY date_denver
;

----- Insert into agg table -------
INSERT INTO TABLE prod.asp_counts_daily
PARTITION(unit,platform,domain,company,date_denver,source_table)
  SELECT  new_ids_incl_sub_accts as value,
         'new_ids_incl_sub_accts' as metric,
          unit,
          platform,
          domain,
          company,
          date_denver,
          source_table
  FROM ${env:TMP_db}.asp_consumer_agg_long_metric_names
;

INSERT INTO TABLE prod.asp_counts_daily
PARTITION(unit,platform,domain,company,date_denver,source_table)
  SELECT  combined_credential_recoveries as value,
         'combined_credential_recoveries' as metric,
          unit,
          platform,
          domain,
          company,
          date_denver,
          source_table
  FROM ${env:TMP_db}.asp_consumer_agg_long_metric_names
;
