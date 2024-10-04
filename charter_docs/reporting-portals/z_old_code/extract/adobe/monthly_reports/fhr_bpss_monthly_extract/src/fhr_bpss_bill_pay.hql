-- Tested in HIVE

USE ${env:ENVIRONMENT};

-- BEGIN L-CHTR Porta  contact_info_updates -- tested
SELECT 'Beginning L-CHTR Portal Calcs';

INSERT INTO TABLE ${env:TMP_db}.fhr_monthly_bpss_bill_pay
SELECT 
  MAX(net.visitors_viewing_bill) AS visitors_viewing_bill,
  SUM(IF(bpa.step IN ('3a','3b','3c','3d','3e','3f'),bpa.count_visits,0)) AS one_time_payments_confirmed,
  SUM(IF(bpa.step IN ('2a','2b','2c','2d','2e','2f'),bpa.count_visits,0)) AS one_time_payments_attempted,
  SUM(IF(bpa.step IN ('6a','6b','6c','9a','9b','9c'),bpa.count_visits,0)) AS auto_pay_setup_confirmed,
  SUM(IF(bpa.step IN ('5a','5b','5c','8a','8b','8c'),bpa.count_visits,0)) AS auto_pay_setup_attempted,
  MAX(mbp.auto_pay_processed_cc_dc) AS auto_pay_processed_cc_dc,
  MAX(mbp.auto_pay_processed_eft) AS auto_pay_processed_eft,
  MAX(mbp.online_one_time_payments) AS online_one_time_payments,
  MAX(mbp.payments_other) AS payments_other,
  'L-CHTR' AS legacy_company,
  'Portal' AS platform,
  date_yearmonth('${env:MONTH_START_DATE}') AS year_month
FROM ${env:TMP_db}.net_bill_pay_analytics_monthly bpa
LEFT JOIN 
  (SELECT
    SIZE(COLLECT_SET(IF(message__name IN ('View Current Bill', 'View Statement'),visit__device__enc_uuid,NULL))) AS visitors_viewing_bill,
    date_yearmonth(partition_date) as year_month
    FROM net_events
    WHERE date_yearmonth(partition_date) = date_yearmonth('${env:MONTH_START_DATE}')
    GROUP BY date_yearmonth(partition_date)
    ) net
ON bpa.year_month = net.year_month
LEFT JOIN ${env:TMP_db}.fhr_chtr_bill_pay_iva_metrics_manual mbp
ON bpa.year_month = mbp.year_month
WHERE
  bpa.company = 'L-CHTR'
  AND bpa.year_month = date_yearmonth('${env:MONTH_START_DATE}')
;

SELECT 'Finishing L-CHTR Portal Calcs';


-- BEGIN L-TWC Portal CALCULATIONS 
SELECT 'Beginning L-TWC Portal Calcs';

INSERT INTO TABLE ${env:TMP_db}.fhr_monthly_bpss_bill_pay
SELECT 
  SIZE(COLLECT_SET(CASE WHEN state__view__current_page__elements__name = 'ebpp:statement download:current' AND array_contains(message__feature__name,'Instance of eVar7') THEN visit__device__enc_uuid END)) AS visitors_viewing_bill,
  SIZE(COLLECT_SET(CASE WHEN state__view__current_page__page_name = 'ebpp > bill pay > step 4 / thank you' 
    AND operation__operation_type 
    IN ('one time payment > dc',
    'one time:saved > cc',
    'one time:saved > dc',
    'one time:unknown > ach',
    'one time:unknown > cc',
    'one time:unknown > dc') 
    THEN visit__visit_id END)) AS one_time_payments_confirmed,
  CAST(NULL AS INT) AS one_time_payments_attempted,
  SIZE(COLLECT_SET(CASE WHEN state__view__current_page__page_name = 'ebpp > bill pay > step 4 / thank you' 
    AND operation__operation_type 
    IN ('recurring:unknown > cc','recurring:unknown > ach','recurring:unknown > dc') 
    THEN visit__visit_id END)) AS auto_pay_setup_confirmed,
  CAST(NULL AS INT) AS auto_pay_setup_attempted,
  CAST(NULL AS INT) AS auto_pay_processed_cc_dc,
  CAST(NULL AS INT) AS auto_pay_processed_eft,
  CAST(NULL AS INT) AS online_one_time_payments,
  CAST(NULL AS INT) AS payments_other,
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

INSERT INTO TABLE ${env:TMP_db}.fhr_monthly_bpss_bill_pay
SELECT 
  MAX(ev.visitors_viewing_bill) AS visitors_viewing_bill,
  MAX(ev.one_time_payments_confirmed) AS one_time_payments_confirmed,
  MAX(ev.one_time_payments_attempted) AS one_time_payments_attempted,
  MAX(ev.auto_pay_setup_confirmed) AS auto_pay_setup_confirmed,
  MAX(ev.auto_pay_setup_attempted) AS auto_pay_setup_attempted,
  MAX(man.auto_pay_processed_cc_dc) AS auto_pay_processed_cc_dc,
  MAX(man.auto_pay_processed_eft) AS auto_pay_processed_eft,
  MAX(man.online_one_time_payments) AS online_one_time_payments,
  MAX(man.payments_other) AS payments_other,
  'L-BHN' AS legacy_company,
  'Portal' AS platform,
  MAX(man.year_month) AS year_month
FROM ${env:TMP_db}.fhr_bhn_bill_pay_type_manual man
LEFT JOIN 
  (SELECT
    SIZE(COLLECT_SET(CASE WHEN(array_contains(message__feature__name, 'Custom Event 11') AND state__view__current_page__page_type='RES') THEN visit__device__enc_uuid END)) AS visitors_viewing_bill,
    SIZE(COLLECT_SET(CASE WHEN(array_contains(message__feature__name, 'Custom Event 31') AND state__view__current_page__page_type='RES') THEN visit__visit_id END)) AS one_time_payments_confirmed,
    SIZE(COLLECT_SET(CASE WHEN(array_contains(message__feature__name, 'Custom Event 36') AND state__view__current_page__page_type='RES') THEN visit__visit_id END)) AS one_time_payments_attempted,
    SIZE(COLLECT_SET(CASE WHEN(array_contains(message__feature__name, 'Custom Event 24') AND state__view__current_page__page_type='RES') THEN visit__visit_id END)) AS auto_pay_setup_confirmed,
    SIZE(COLLECT_SET(CASE WHEN(array_contains(message__feature__name, 'Custom Event 19') AND state__view__current_page__page_type='RES') THEN visit__visit_id END)) AS auto_pay_setup_attempted,
    date_yearmonth('${env:MONTH_START_DATE}') as year_month
  FROM bhn_bill_pay_events
  WHERE 
    (partition_date_utc BETWEEN DATE_ADD('${env:MONTH_START_DATE}',-1) AND DATE_ADD('${env:MONTH_END_DATE}',1))
  AND 
    epoch_converter(CAST(message__timestamp*1000 AS BIGINT),'America/Denver') BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
    ) ev
ON man.year_month = ev.year_month
WHERE 
  man.year_month = date_yearmonth('${env:MONTH_START_DATE}')
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