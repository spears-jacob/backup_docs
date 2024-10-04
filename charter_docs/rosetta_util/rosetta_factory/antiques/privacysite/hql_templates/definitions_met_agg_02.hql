
'test' AS placeholder_for_additional_strings,
partition_date_utc
FROM core_quantum_events
WHERE partition_date_utc = '${hiveconf:START_DATE}'
  and visit__application_details__application_name ='PrivacyMicrosite'
  and message__name IN('pageView', 'selectAction', 'error')
group by partition_date_utc,
         state__view__current_page__app_section,
         visit__user__role,
         message__context,
         visit__device__enc_uuid,
         visit__visit_id
;

INSERT OVERWRITE TABLE asp_privacysite_metric_agg PARTITION (partition_date_utc)
select * from ${env:TMP_db}.ps_metric_agg_${env:CLUSTER};

DROP TABLE IF EXISTS ${env:TMP_db}.ps_metric_agg_${env:CLUSTER} PURGE;
