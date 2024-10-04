
cast(NULL as INT) as calls_within_24_hrs,
min(partition_date_utc)
FROM ${env:GLOBAL_DB}.core_quantum_events_sspp
WHERE (partition_date_utc >= "${hiveconf:START_DATE}"
  AND partition_date_utc <  "${hiveconf:END_DATE}"
  and visit__application_details__application_name in ('SpecNet', 'SMB', 'MySpectrum'))
group by
visit__application_details__application_name,
visit__application_details__application_type,
visit__application_details__app_version,
case
  when visit__application_details__application_name <> 'MySpectrum' then 'N/A'
  when visit__application_details__application_name = 'MySpectrum' and
  (cast(regexp_replace(visit__application_details__app_version,'[^0-9\\s]','') as int) >= 9150
  or cast(substr(visit__application_details__app_version,1,instr(visit__application_details__app_version,'.')-1) as int) > 9
  ) then 'New UI'
  else 'Old UI'
  end,
visit__account__enc_account_number,
visit__account__enc_mobile_account_number,
visit__visit_id,
visit__visit_start_timestamp,
visit__device__enc_uuid,
cast(NULL as INT)
;
