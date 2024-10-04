-- Tested in HIVE

USE ${env:ENVIRONMENT};

-- BEGIN L-CHTR Portal CALCULATIONS,
SELECT 'Beginning L-CHTR Portal Calcs';


INSERT INTO TABLE ${env:TMP_db}.fhr_monthly_bpss_metrics_agg
SELECT
  net.support_overview_page_visitors AS support_overview_page_visitors,
  bc.support_videos_viewed AS support_videos_viewed,
  net.equipment_refresh_tv AS equipment_refresh_tv,
  net.equipment_refresh_internet AS equipment_refresh_internet,
  net.equipment_refresh_voice AS equipment_refresh_voice,
  net.equipment_refresh_total AS equipment_refresh_total,
  net.service_appointments_cancelled AS service_appointments_cancelled,
  net.service_appointments_rescheduled AS service_appointments_rescheduled,
  net.contact_info_updates AS contact_info_updates,
  net.pref_comm_set AS pref_comm_set,
  net.legacy_company AS legacy_company,
  net.platform AS platform,
  net.year_month AS year_month
FROM ${env:TMP_db}.fhr_chtr_brightcove_support_videos_manual bc
LEFT JOIN (
  SELECT
    SIZE(COLLECT_SET(CASE WHEN state__view__current_page__name='support' THEN visit__device__enc_uuid END)) AS support_overview_page_visitors,
    SUM(IF(message__name='Refresh' AND message__category='Custom Link',1,0)) AS equipment_refresh_tv,
    SUM(IF(message__name='Reset-Modem' AND operation__type IN ('Internet','Internet-Voice') AND message__category='Custom Link',1,0)) AS equipment_refresh_internet,
    SUM(IF(message__name='Reset-Modem' AND operation__type='Voice' AND message__category='Custom Link',1,0)) AS equipment_refresh_voice,
    SUM(IF(message__category='Custom Link' AND (message__name='Refresh' OR (message__name='Reset-Modem' AND operation__type IN ('Internet','Internet-Voice','Voice'))),1,0)) AS equipment_refresh_total,
    SIZE(COLLECT_SET(CASE WHEN message__name IN ('cancel-success-page','cancel-success-page_STVA') THEN visit__visit_id ELSE NULL END)) AS service_appointments_cancelled,
    SIZE(COLLECT_SET(CASE WHEN message__name IN ('reschedule-confirm') THEN visit__visit_id ELSE NULL END)) AS service_appointments_rescheduled,
    COUNT(CASE WHEN (message__name='Save'
      AND (state__view__current_page__elements__name='Phone'
      OR state__view__current_page__elements__name='Email'
      OR state__view__current_page__elements__name='Address')) THEN '1' END)  AS contact_info_updates,
    COUNT(CASE WHEN (message__name='Save'
      AND (state__view__current_page__elements__name='Billing Notifications'
      OR state__view__current_page__elements__name='Account Notifications'
      OR state__view__current_page__elements__name='Service Alerts'
      OR state__view__current_page__elements__name='Appointment Reminders')) THEN '1' END) AS pref_comm_set,
    'L-CHTR' AS legacy_company,
    'Portal' AS platform,
    date_yearmonth('${env:MONTH_START_DATE}') AS year_month
  FROM net_events
  WHERE
    partition_date BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
  ) net
ON bc.year_month = net.year_month
;


SELECT 'Finishing L-CHTR Portal Calcs';


-- BEGIN L-TWC Portal CALCULATIONS
SELECT 'Beginning L-TWC Portal Calcs';

INSERT INTO TABLE ${env:TMP_db}.fhr_monthly_bpss_metrics_agg
SELECT
  SIZE(COLLECT_SET(CASE WHEN state__view__current_page__page_name='support > overview' THEN visit__visit_id END)) AS support_overview_page_visitors,
  SUM(IF(state__view__current_page__page_name LIKE 'x-ref-content > support > video >%' AND message__category = 'Page View',1,0)) AS support_videos_viewed,
  SIZE(COLLECT_SET(CASE WHEN state__view__current_page__elements__name RLIKE '.*ehc:tv.*' THEN visit__visit_id END)) AS equipment_refresh_tv,
  SIZE(COLLECT_SET(CASE WHEN state__view__current_page__elements__name RLIKE '.*ehc:internet.*' THEN visit__visit_id END)) AS equipment_refresh_internet,
  SIZE(COLLECT_SET(CASE WHEN state__view__current_page__elements__name RLIKE '.*ehc:homephone.*' THEN visit__visit_id END)) AS equipment_refresh_voice,
  SIZE(COLLECT_SET(CASE WHEN state__view__current_page__elements__name RLIKE '.*ehc:tv.*' THEN visit__visit_id END)) +
    SIZE(COLLECT_SET(CASE WHEN state__view__current_page__elements__name RLIKE '.*ehc:internet.*' THEN visit__visit_id END)) +
    SIZE(COLLECT_SET(CASE WHEN state__view__current_page__elements__name RLIKE '.*ehc:hmoephone.*' THEN visit__visit_id END)) AS equipment_refresh_total,
  SUM(IF(state__view__current_page__sub_section = 'my services > appointment manager > cancel submitted',1,0)) AS service_appointments_cancelled,
  SUM(IF(state__view__current_page__sub_section = 'services : my services : appointment manager : reschedule submitted',1,0)) AS service_appointments_rescheduled,
  SUM(IF(state__view__current_page__elements__name IN(
    'my profile edit > password',
    'my profile edit > contactnumbers',
    'my profile edit > username',
    'my profile edit > security questions',
    'my profile edit > billing address')
    AND array_contains(message__feature__name,'Instance of eVar7'),1,0)) AS contact_info_updates,
  SUM(IF(state__view__current_page__elements__name IN(
    'mys:user management:user added',
    'mys:user management:invite deleted',
    'mys:user management:user disabled',
    'mys:user management:user enabled',
    'mys:user management:user edited')
    AND array_contains(message__feature__name,'Instance of eVar7'),1,0)) as pref_comm_set,
  'L-TWC' AS legacy_company,
  'Portal' AS platform,
  date_yearmonth('${env:MONTH_START_DATE}') AS year_month
FROM twc_residential_global_events
WHERE
  (partition_date_utc BETWEEN DATE_ADD('${env:MONTH_START_DATE}',-1) AND DATE_ADD('${env:MONTH_END_DATE}',1))
AND
  epoch_converter(CAST(message__timestamp*1000 AS BIGINT),'America/Denver') BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
;

SELECT 'Finishing L-TWC Portal Calcs';


-- BEGIN L-BHN Portal CALCULATIONS
SELECT 'Beginning L-BHN Portal Calcs';

INSERT INTO TABLE ${env:TMP_db}.fhr_monthly_bpss_metrics_agg
SELECT
  COUNT(DISTINCT (CASE WHEN state__view__current_page__page_name = 'Customer Care         -         Bright House Networks Support' THEN visit__device__enc_uuid ELSE NULL END)) AS support_overview_page_visitors,
  CAST(NULL AS INT) AS support_videos_viewed,
  CAST(NULL AS INT) AS equipment_refresh_tv,
  CAST(NULL AS INT) AS equipment_refresh_internet,
  CAST(NULL AS INT) AS equipment_refresh_voice,
  COUNT(DISTINCT (CASE WHEN state__view__current_page__page_name = 'My Services | Manage Equipment | Spectrum' THEN visit__device__enc_uuid ELSE NULL END)) AS equipment_refresh_total,
  CAST(NULL AS INT) AS service_appointments_cancelled,
  CAST(NULL AS INT) AS service_appointments_rescheduled,
  SUM(IF(state__view__modal__name = 'Update Email Address' AND array_contains(message__feature__name,'Instance of eVar20'),1,0))
    + SUM(IF(state__view__modal__name = 'Update Email Address' AND array_contains(message__feature__name,'Instance of eVar29'),1,0))
    + SUM(IF(state__view__modal__name = 'Update Email Address' AND array_contains(message__feature__name,'Instance of eVar32'),1,0)) AS contact_info_updates,
  CAST(NULL AS INT) AS pref_comm_set,
  'L-BHN' AS legacy_company,
  'Portal' AS platform,
  date_yearmonth('${env:MONTH_START_DATE}') AS year_month
FROM bhn_residential_events
WHERE
  (partition_date_utc BETWEEN DATE_ADD('${env:MONTH_START_DATE}',-1) AND DATE_ADD('${env:MONTH_END_DATE}',1))
AND
  epoch_converter(CAST(message__timestamp*1000 AS BIGINT),'America/Denver') BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
;



SELECT 'Finishing L-BHN Portal Calcs';


-- BEGIN L-CHTR APP CALCULATIONS
SELECT 'Beginning L-CHTR APP Calcs';


SELECT 'Finishing L-CHTR APP Calcs';


-- BEGIN L-CHTR APP CALCULATIONS
SELECT 'Beginning L-TWC APP Calcs';


SELECT 'Finishing L-TWC APP Calcs';


-- BEGIN L-CHTR APP CALCULATIONS
SELECT 'Beginning L-BHN APP Calcs';


SELECT 'Finishing L-BHN APP Calcs';
