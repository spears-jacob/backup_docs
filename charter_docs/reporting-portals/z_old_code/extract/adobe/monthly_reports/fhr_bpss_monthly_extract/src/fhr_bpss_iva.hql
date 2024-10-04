-- Tested in HIVE

USE ${env:ENVIRONMENT};

-- BEGIN L-CHTR Portal contact_info_updates INT,
SELECT 'Beginning L-CHTR Portal Calcs';

INSERT INTO TABLE ${env:TMP_db}.fhr_monthly_bpss_iva
SELECT
  MAX(man.total_questions_asked_conversations) AS total_questions_asked_conversations,
  MAX(man.conversations_iva) AS conversations_iva,
  MAX(net.user_requests_percent_visits) AS user_requests_percent_visits,
  MAX(man.conversations_live_chat) AS conversations_live_chat,
  MAX(man.conversations_live_chat_deflected) AS conversations_live_chat_deflected,
  'L-CHTR' AS legacy_company,
  'Portal' AS platform,
  date_yearmonth('${env:MONTH_START_DATE}') AS year_month
FROM ${env:TMP_db}.fhr_chtr_bill_pay_iva_metrics_manual man
LEFT JOIN
  (SELECT
    SIZE(COLLECT_SET(CASE WHEN message__name='Ask-Spectrum' AND message__category='Custom Link' THEN visit__visit_id END)) / SIZE(COLLECT_SET(visit__visit_id)) AS user_requests_percent_visits,
    date_yearmonth('${env:MONTH_START_DATE}') AS year_month
  FROM net_events
  WHERE
    partition_date BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
  GROUP BY 
    date_yearmonth('${env:MONTH_START_DATE}')
    ) net
ON man.year_month = net.year_month
WHERE
  man.year_month = date_yearmonth('${env:MONTH_START_DATE}')
;


SELECT 'Finishing L-CHTR Portal Calcs';


-- BEGIN L-TWC Portal CALCULATIONS 
SELECT 'Beginning L-TWC Portal Calcs';

INSERT INTO TABLE ${env:TMP_db}.fhr_monthly_bpss_iva
SELECT
  MAX(man.total_questions_asked_conversations) AS total_questions_asked_conversations,
  MAX(man.conversations_iva) AS conversations_iva,
  MAX(ev.user_requests_percent_visits) AS user_requests_percent_visits,
  MAX(man.conversations_live_chat) AS conversations_live_chat,
  MAX(man.conversations_live_chat_deflected) AS conversations_live_chat_deflected,
  'L-TWC' AS legacy_company,
  'Portal' AS platform,
  date_yearmonth('${env:MONTH_START_DATE}') AS year_month
FROM ${env:TMP_db}.fhr_twc_iva_metrics_manual man
LEFT JOIN
  (SELECT
    SIZE(COLLECT_SET(CASE WHEN state__view__current_page__page_name IN ('virtual assistant window', 'virtual assistant window - dual') THEN visit__visit_id END))
      / SIZE(COLLECT_SET(visit__visit_id)) AS user_requests_percent_visits,
    date_yearmonth('${env:MONTH_START_DATE}') AS year_month
  FROM twc_residential_global_events
  WHERE 
    (partition_date_utc BETWEEN DATE_ADD('${env:MONTH_START_DATE}',-1) AND DATE_ADD('${env:MONTH_END_DATE}',1))
    AND epoch_converter(CAST(message__timestamp*1000 AS BIGINT),'America/Denver') BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
  GROUP BY 
    date_yearmonth('${env:MONTH_START_DATE}')
    ) ev
ON man.year_month = ev.year_month
WHERE
  man.year_month = date_yearmonth('${env:MONTH_START_DATE}')
;

SELECT 'Finishing L-TWC Portal Calcs';


-- BEGIN L-BHN Portal CALCULATIONS 
SELECT 'Beginning L-BHN Portal Calcs';

INSERT INTO TABLE ${env:TMP_db}.fhr_monthly_bpss_iva
SELECT
  temp.total_questions_asked_conversations AS total_questions_asked_conversations,
  temp.conversations_iva AS conversations_iva,
  temp.user_requests_percent_visits AS user_requests_percent_visits,
  temp.conversations_live_chat AS conversations_live_chat,
  temp.conversations_live_chat_deflected AS conversations_live_chat_deflected,
  temp.legacy_company AS legacy_company,
  temp.platform AS platform,
  temp.year_month AS year_month
FROM (
SELECT
  CAST(NULL AS INT) AS total_questions_asked_conversations,
  CAST(NULL AS INT) AS conversations_iva,
  CAST(NULL AS INT) AS user_requests_percent_visits,
  CAST(NULL AS INT) AS conversations_live_chat,
  CAST(NULL AS INT) AS conversations_live_chat_deflected,
  'L-BHN' AS legacy_company,
  'Portal' AS platform,
  date_yearmonth('${env:MONTH_START_DATE}') AS year_month
) temp
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