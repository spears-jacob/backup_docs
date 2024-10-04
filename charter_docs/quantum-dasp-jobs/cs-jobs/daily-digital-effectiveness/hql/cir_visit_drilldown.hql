USE ${env:DASP_db};

set mapreduce.input.fileinputformat.split.maxsize=5368709120;
set mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.optimize.sort.dynamic.partition=false;
set hive.exec.dynamic.partition.mode=nonstrict;
set orc.force.positional.evolution=true;
SET hive.merge.tezfiles=true;
set hive.merge.smallfiles.avgsize=2048000000;
set hive.merge.size.per.task=2048000000;

insert into cir_drilldown_calls_with_visits_${hiveconf:execid}_${hiveconf:stepid}
SELECT *
  FROM `${env:CALLVISIT}`
 WHERE call_date BETWEEN DATE_ADD('${hiveconf:END_DATE_UTC}',-7) AND DATE_ADD('${hiveconf:START_DATE_UTC}',-1)
;

insert into cir_drilldown_cqe_enrichment_${hiveconf:execid}_${hiveconf:stepid}
SELECT distinct
         partition_date_utc
        ,visit__account__enc_account_number
        ,visit__visit_id
        ,state__view__current_page__page_name
        ,state__view__current_page__app_section
        ,CASE
           WHEN (visit__account__details__mso='TWC' or visit__account__details__mso='"TWC"') THEN 'TWC'
           WHEN (visit__account__details__mso= 'BH' or visit__account__details__mso='"BHN"' OR visit__account__details__mso='BHN') THEN 'BHN'
           WHEN (visit__account__details__mso= 'CHARTER' or visit__account__details__mso='"CHTR"' OR visit__account__details__mso='CHTR') THEN 'CHR'
           WHEN (visit__account__details__mso= 'NONE' or visit__account__details__mso='UNKNOWN') THEN NULL
           ELSE visit__account__details__mso
         END AS visit__account__details__mso
        ,visit__application_details__application_name
        ,state__view__current_page__user_journey
        ,state__view__current_page__user_sub_journey
        ,received__timestamp
  FROM ${env:ENVIRONMENT}.core_quantum_events_sspp
 WHERE partition_date_utc BETWEEN DATE_ADD('${hiveconf:END_DATE_UTC}',-8) and DATE_ADD('${hiveconf:START_DATE_UTC}',-2)
   AND visit__application_details__application_name IN ('MySpectrum','SpecNet','SMB','SpecMobile','SelfInstall')
   AND message__name = 'pageView'
   AND visit__account__enc_account_number IS NOT NULL
   AND visit__account__details__mso IS NOT NULL
   AND state__view__current_page__page_name IS NOT NULL
;

INSERT overwrite TABLE asp_de_visit_drilldown
SELECT visit__account__details__mso
       ,visit__application_details__application_name
       ,state__view__current_page__user_journey
       ,state__view__current_page__user_sub_journey
       ,state__view__current_page__page_name
       ,visit__visit_id
       ,partition_date_utc
FROM
(SELECT *
       ,ROW_NUMBER() OVER (PARTITION BY partition_date_utc, state__view__current_page__page_name) AS rn
from
(SELECT DISTINCT
        visit__visit_id
       ,state__view__current_page__page_name
       ,visit__account__details__mso
       ,visit__application_details__application_name
       ,state__view__current_page__user_journey
       ,state__view__current_page__user_sub_journey
       ,CASE WHEN b.call_date IS NULL THEN partition_date_utc ELSE b.call_date END AS partition_date_utc
  FROM cir_drilldown_cqe_enrichment_${hiveconf:execid}_${hiveconf:stepid} a
  LEFT JOIN cir_drilldown_calls_with_visits_${hiveconf:execid}_${hiveconf:stepid} b
    ON a.visit__visit_id = b.visit_id
   and call_inbound_key IS NOT NULL) c ) d
 WHERE rn <=10
;

DROP TABLE IF EXISTS cir_drilldown_cqe_enrichment_${hiveconf:execid}_${hiveconf:stepid} PURGE;
DROP TABLE IF EXISTS cir_drilldown_cqe_enrichment_${hiveconf:execid}_${hiveconf:stepid} PURGE;
