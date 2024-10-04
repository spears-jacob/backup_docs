-------------------------------------------------
---- End Rosetta-Generated Metric Defintions ----
---------------- IDM Metric Agg -----------------
-------------------------------------------------
visit__account__enc_account_number as portals_unique_acct_key,
prod.epoch_converter(received__timestamp, 'America/Denver') as denver_date
FROM core_quantum_events_portals_v
WHERE partition_date_hour_utc >= '${env:START_DATE_TZ}'
  AND partition_date_hour_utc <  '${env:END_DATE_TZ}'
  and partition_date_hour_utc >= '2019-12-11_07'
and visit__application_details__application_name ='IDManagement'
and message__name IN('pageView', 'selectAction', 'error', 'apiCall')

group by prod.epoch_converter(received__timestamp, 'America/Denver'),
    epoch_converter(received__timestamp, 'America/Denver'),
    state__view__current_page__page_name,
    state__view__current_page__app_section,
    visit__user__role,
    visit__device__enc_uuid,
    visit__visit_id,
    visit__application_details__application_type,
    LOWER(visit__device__device_type),
    visit__application_details__app_version,
    visit__login__logged_in,
    LOWER(visit__application_details__application_name),
    'All OS Names',
    visit__device__operating_system,
    visit__device__browser__name,
    visit__device__browser__version,
    state__view__current_page__ui_responsive_breakpoint,
    visit__device__device_form_factor,
    parse_url(visit__application_details__referrer_link,'HOST'),
    visit__account__enc_account_number
;
