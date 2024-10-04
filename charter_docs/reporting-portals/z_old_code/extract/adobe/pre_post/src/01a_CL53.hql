USE ${env:TMP_db};

TRUNCATE TABLE net_asp_kpis_auth;
TRUNCATE TABLE net_asp_kpis_one_time_payments;
TRUNCATE TABLE net_asp_kpis_auto_payments;
TRUNCATE TABLE net_asp_kpis_bill_pay_search;
TRUNCATE TABLE net_asp_kpis_upgrade_link_clicks;
TRUNCATE TABLE net_asp_kpis_search_email_voice;
TRUNCATE TABLE net_asp_kpis_iva_email_voice;
TRUNCATE TABLE asp_kpis_metrics_agg;


USE ${env:ENVIRONMENT};

-- Omniture translations for net_events (from asp_v_net_events.pig)
-- p3  = post_prop3  ==mapped-to==> visit__isp__status (in Omniture: Login Status (3), either 'Logged Out' or 'Logged In')
-- p22 = post_prop22 ==mapped-to==> state__view__current_page__name
-- p48 = post_prop48 ==mapped-to==> state__view__current_page__page_id
-- p51 = post_prop51 ==mapped-to==> operation__user_entry__enc_text  (IVA search keyword)
-- p52 = post_prop52 ==mapped-to==> state__view__modal__text   (classification of the IVA response)
-- p55 = post_prop55 ==mapped-to==> state__view__current_page__search_text   (site search keyword)

-- Metric Lookups
DROP TABLE IF EXISTS ${env:LKP_db}.net_asp_kpis_metrics PURGE;

CREATE TABLE IF NOT EXISTS ${env:LKP_db}.net_asp_kpis_metrics
(
  metric STRING,
  sub_metric STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE TBLPROPERTIES('serialization.null.format'='');
;

INSERT OVERWRITE TABLE ${env:LKP_db}.net_asp_kpis_metrics VALUES
  ('Visits', 'Logged In'),
  ('Visits', 'Not Logged In'),
  ('Visits', 'Visits'),
  ('Visits', 'Per ten thousand that become authenticated'),
  ('One Time Payments', 'Old Pages'),
  ('One Time Payments', 'New Pages'),
  ('Auto Payments', 'Old Pages'),
  ('Auto Payments', 'New Pages'),
  ('Bill Pay Search', 'Bill Pay IVA Entry'),
  ('Bill Pay Search', 'Bill Pay IVA Results'),
  ('Bill Pay Search', 'Bill Pay Search Terms'),
  ('Upgrade Link Clicks', 'Upgrade-Voice-Banner'),
  ('Upgrade Link Clicks', 'Upgrade-TV-Banner'),
  ('Upgrade Link Clicks', 'Upgrade-Internet-Banner'),
  ('Upgrade Link Clicks', 'Upgrade-Overview-Banner'),
  ('Search Email', 'IVA'),
  ('Search Email', 'Search'),
  ('Search Voice', 'IVA'),
  ('Search Voice', 'Search'),
  ('Bill Pay Card or Local Nav', 'Local Nav'),
  ('Bill Pay Card or Local Nav', 'Bill Pay Card'),
  ('Average Time Before Login Attempt','Before Homepage Change'),
  ('Average Time Before Login Attempt','After Homepage Change')

;


---- {1} Average time on site before log in attempt.
---- How to determine average time on site before log in attempt?
---- p48 = post_prop48 ==mapped-to==> state__view__current_page__page_id
---- Is this decreasing because the login form is surfaced on the home page?
---- trend this
----
---- exclude "p48 contains https://www.spectrum.net/login/?ReferringPartner="
---- (use p3='Logged In' to check for authentication)

-- {1} Average time on site before log in attempt.

SELECT "\n\nNow Running {1} Average time on site before log in attempt.\n\n ";

DROP TABLE IF EXISTS ${env:TMP_db}.asp_login_attempt_timing PURGE;

CREATE TEMPORARY TABLE ${env:TMP_db}.asp_login_attempt_timing AS
select  state__view__current_page__name as page_name,
        visit__visit_id as visit_id,
        min(message__timestamp) AS message__timestamp,
        message__category,
        message__name,
        visit__isp__status AS login_status,
        partition_date
from asp_v_net_events
where (partition_date BETWEEN '${env:START_DATE}' AND '${env:END_DATE}')
AND (state__view__current_page__page_id IS NULL OR
     state__view__current_page__page_id NOT LIKE "%https://www.spectrum.net/login/?ReferringPartner=%")
AND ( (state__view__current_page__name = 'home'
        AND visit__isp__status='Logged Out'
        AND message__category='Page View')
          OR
      (state__view__current_page__name = 'home-unauth'
        AND message__category='Page View')
          OR
      (state__view__current_page__name in('login', 'home-unauth')
              AND message__category = 'Custom Link'
              AND message__name RLIKE 'Sign-| In' )
    )
GROUP BY  state__view__current_page__name,
          visit__visit_id,
          message__category,
          message__name,
          visit__isp__status,
          partition_date
;

---- By comparing each visit id's 2 timestamps...
---- message__timestamp - "Before Sign In":
----     Before Homepage Change: pageName = home && prop3 = "logged out" and event = page view  
----     After Homepage Change: pageName = "home-unauth" and event = page view
---- message__timestamp - "Login Attempts"
----    Before Homepage Change: customLink = "sign-in" and pageName = "login"  
----    After Homepage Change: customLink = "sign-in" and pageName = "home-unauth"

-- BEFORE Homepage Change
INSERT OVERWRITE TABLE ${env:TMP_db}.asp_kpis_metrics_agg PARTITION(report_date)
SELECT  'Average Time Before Login Attempt' as metric,
        'Before Homepage Change' as submetric,
        NULL as detail,
        ROUND(AVG(LoginAttempt.message__timestamp - BeforeSignIn.message__timestamp),0) as counts,
        NULL as count_visits,
        BeforeSignIn.partition_date as report_date
FROM ${env:TMP_db}.asp_login_attempt_timing BeforeSignIn
INNER JOIN (SELECT  message__timestamp,
                    visit_id,
                    partition_date
            FROM ${env:TMP_db}.asp_login_attempt_timing
            where page_name = 'login'
              AND message__category = 'Custom Link'
              AND message__name RLIKE 'Sign-| In') LoginAttempt
ON BeforeSignIn.visit_id = LoginAttempt.visit_id
  AND BeforeSignIn.partition_date = LoginAttempt.partition_date
where login_status = 'Logged Out' -- prop3
  AND message__category = 'Page View'
  AND page_name = 'home'
  AND BeforeSignIn.message__timestamp < LoginAttempt.message__timestamp
GROUP BY BeforeSignIn.partition_date;

-- AFTER Before Homepage Change
INSERT OVERWRITE TABLE ${env:TMP_db}.asp_kpis_metrics_agg PARTITION(report_date)
SELECT  'Average Time Before Login Attempt' as metric,
        'After Homepage Change' as submetric,
        NULL as detail,
        ROUND(AVG(LoginAttempt.message__timestamp - BeforeSignIn.message__timestamp),0) as counts,
        NULL as count_visits,
        BeforeSignIn.partition_date as report_date
FROM ${env:TMP_db}.asp_login_attempt_timing BeforeSignIn
INNER JOIN (SELECT  message__timestamp,
                    visit_id,
                    partition_date
            FROM ${env:TMP_db}.asp_login_attempt_timing
            where page_name = 'home-unauth'
              AND message__category = 'Custom Link'
              AND message__name RLIKE 'Sign-| In') LoginAttempt
ON BeforeSignIn.visit_id = LoginAttempt.visit_id
  AND BeforeSignIn.partition_date = LoginAttempt.partition_date
where page_name = 'home-unauth'
  AND message__category = 'Page View'
  AND BeforeSignIn.message__timestamp < LoginAttempt.message__timestamp
GROUP BY BeforeSignIn.partition_date;


----  {2} Are there more visits with a login?
----  For login attempt, (use p3='Logged In' to check for authentication)
----  Both:  trend, then trend visits vs. percent of visits authenticated
SELECT "\n\nNow Running {2} Are there more visits with a login?\n\n ";

INSERT OVERWRITE TABLE ${env:TMP_db}.net_asp_kpis_auth PARTITION(report_date)
SELECT
  'Visits' as metric,
  'Logged In' as submetric,
  count(*) as counts,
  SIZE(COLLECT_SET(visit__visit_id)) as count_visits,
  partition_date as report_date
from asp_v_net_events
where (partition_date BETWEEN '${env:START_DATE}' AND '${env:END_DATE}')
  AND (lower(state__view__current_page__page_id) RLIKE '.*referringpartner.*')
  AND visit__isp__status='Logged In'
GROUP BY
  partition_date
;

INSERT OVERWRITE TABLE ${env:TMP_db}.net_asp_kpis_auth PARTITION(report_date)
SELECT
  'Visits' as metric,
  'Not Logged In' as submetric,
  count(*) as counts,
  SIZE(COLLECT_SET(visit__visit_id)) as count_visits,
  partition_date as report_date
from asp_v_net_events
where (partition_date BETWEEN '${env:START_DATE}' AND '${env:END_DATE}')
  AND (lower(state__view__current_page__page_id) RLIKE '.*referringpartner.*')
  AND (visit__isp__status IS NULL or visit__isp__status <> 'Logged In')
GROUP BY
  partition_date
;

INSERT OVERWRITE TABLE ${env:TMP_db}.net_asp_kpis_auth PARTITION(report_date)
SELECT
  'Visits' as metric,
  'Visits' as submetric,
  count(*) as counts,
  SIZE(COLLECT_SET(visit__visit_id)) as count_visits,
  partition_date as report_date
  from asp_v_net_events
  where (partition_date BETWEEN '${env:START_DATE}' AND '${env:END_DATE}')
    AND (lower(state__view__current_page__page_id) RLIKE '.*referringpartner.*')
  GROUP BY partition_date
  ;

INSERT OVERWRITE TABLE ${env:TMP_db}.net_asp_kpis_auth PARTITION(report_date)
SELECT
  'Visits' as metric,
  'Per ten thousand that become authenticated' as submetric,
  ROUND(10000*AVG(auth.count_auth_visits / total.count_visits),0) as counts,
  NULL as count_visits,
  auth.report_date
FROM ${env:TMP_db}.net_asp_kpis_auth total
INNER JOIN( SELECT count_visits as count_auth_visits,
                   report_date,
                   metric
            FROM ${env:TMP_db}.net_asp_kpis_auth
            WHERE (report_date BETWEEN '${env:START_DATE}' AND '${env:END_DATE}')
            and metric = 'Visits' AND sub_metric = 'Logged In' ) auth
ON    total.report_date = auth.report_date
  AND total.metric = auth.metric
WHERE (total.report_date BETWEEN '${env:START_DATE}' AND '${env:END_DATE}')
and total.metric = 'Visits' AND total.sub_metric = 'Visits'
GROUP BY auth.report_date
;


----  {3} Are features on auth home page getting used more? (new pages not available till 30 Jan 2018)
SELECT "\n\nNow Running {3} Are features on auth home page getting used more?\n\n ";

----    A. Before/After OTP's
INSERT OVERWRITE TABLE ${env:TMP_db}.net_asp_kpis_one_time_payments PARTITION(report_date)
SELECT
  'One Time Payments' as metric,
  CASE
    WHEN state__view__current_page__name IN (
      'OneTime-noAutoPay-Credit-Confirm',
      'OneTime-noAutoPay-Checking-Confirm',
      'OneTime-wAutoPay-Credit-Confirm',
      'OneTime-wAutoPay-Checking-Confirm',
      'OneTime-noAutoPay-Savings-Confirm',
      'OneTime-wAutoPay-Savings-Confirm') THEN 'Old Pages'
    WHEN state__view__current_page__name IN (
      'pay-bill.onetime-confirmation',
      'pay-bill.onetime-confirmation-with-autopay-enrollment') THEN 'New Pages'
  END AS sub_metric,
  state__view__current_page__name as page_name,
  SIZE(COLLECT_SET(visit__visit_id)) as counts,
  partition_date as report_date
from asp_v_net_events
where (partition_date BETWEEN '${env:START_DATE}' AND '${env:END_DATE}')
  AND message__category='Page View'
  AND state__view__current_page__name IN (
      'OneTime-noAutoPay-Credit-Confirm',
      'OneTime-noAutoPay-Checking-Confirm',
      'OneTime-wAutoPay-Credit-Confirm',
      'OneTime-wAutoPay-Checking-Confirm',
      'OneTime-noAutoPay-Savings-Confirm',
      'OneTime-wAutoPay-Savings-Confirm',
      'pay-bill.onetime-confirmation',
      'pay-bill.onetime-confirmation-with-autopay-enrollment')
GROUP BY
  partition_date,
  CASE
    WHEN state__view__current_page__name IN (
      'OneTime-noAutoPay-Credit-Confirm',
      'OneTime-noAutoPay-Checking-Confirm',
      'OneTime-wAutoPay-Credit-Confirm',
      'OneTime-wAutoPay-Checking-Confirm',
      'OneTime-noAutoPay-Savings-Confirm',
      'OneTime-wAutoPay-Savings-Confirm') THEN 'Old Pages'
    WHEN state__view__current_page__name IN (
      'pay-bill.onetime-confirmation',
      'pay-bill.onetime-confirmation-with-autopay-enrollment') THEN 'New Pages'
  END,
  state__view__current_page__name
;

----    B. Before/After AP's
INSERT OVERWRITE TABLE ${env:TMP_db}.net_asp_kpis_auto_payments PARTITION(report_date)
SELECT
  'Auto Payments' as metric,
  CASE
    WHEN state__view__current_page__name IN (
      'AutoPay-wBalance-Credit-Confirm',
      'AutoPay-wBalance-Checking-Confirm',
      'AutoPay-noBalance-Credit-Confirm',
      'AutoPay-noBalance-Checking-Confirm',
      'AutoPay-wBalance-Savings-Confirm',
      'AutoPay-noBalance-Savings-Confirm') THEN 'Old Pages'
    WHEN state__view__current_page__name IN (
      'pay-bill.enroll-in-autopay-no-balance.credit',
      'pay-bill.enroll-in-autopay-no-balance.checking',
      'pay-bill.enroll-in-autopay-no-balance.savings',
      'pay-bill.enroll-in-autopay-with-balance.credit',
      'pay-bill.enroll-in-autopay-with-balance.checking',
      'pay-bill.enroll-in-autopay-with-balance.savings') THEN 'New Pages'
  END as sub_metric,
  state__view__current_page__name as page_name,
  SIZE(COLLECT_SET(visit__visit_id)) as counts,
  partition_date as report_date
from asp_v_net_events
where (partition_date BETWEEN '${env:START_DATE}' AND '${env:END_DATE}')
  AND message__category='Page View'
  AND state__view__current_page__name IN (
    'AutoPay-wBalance-Credit-Confirm',
    'AutoPay-wBalance-Checking-Confirm',
    'AutoPay-noBalance-Credit-Confirm',
    'AutoPay-noBalance-Checking-Confirm',
    'AutoPay-wBalance-Savings-Confirm',
    'AutoPay-noBalance-Savings-Confirm',
    'pay-bill.enroll-in-autopay-no-balance.credit',
    'pay-bill.enroll-in-autopay-no-balance.checking',
    'pay-bill.enroll-in-autopay-no-balance.savings',
    'pay-bill.enroll-in-autopay-with-balance.credit',
    'pay-bill.enroll-in-autopay-with-balance.checking',
    'pay-bill.enroll-in-autopay-with-balance.savings')
GROUP BY
  partition_date,
  CASE
    WHEN state__view__current_page__name IN (
      'AutoPay-wBalance-Credit-Confirm',
      'AutoPay-wBalance-Checking-Confirm',
      'AutoPay-noBalance-Credit-Confirm',
      'AutoPay-noBalance-Checking-Confirm',
      'AutoPay-wBalance-Savings-Confirm',
      'AutoPay-noBalance-Savings-Confirm') THEN 'Old Pages'
    WHEN state__view__current_page__name IN (
      'pay-bill.enroll-in-autopay-no-balance.credit',
      'pay-bill.enroll-in-autopay-no-balance.checking',
      'pay-bill.enroll-in-autopay-no-balance.savings',
      'pay-bill.enroll-in-autopay-with-balance.credit',
      'pay-bill.enroll-in-autopay-with-balance.checking',
      'pay-bill.enroll-in-autopay-with-balance.savings') THEN 'New Pages'
  END,
  state__view__current_page__name
;

---- {4} Are users clicking the upgrade links from the home page?
SELECT "\n\nNow Running {4} Are users clicking the upgrade links from the home page?\n\n ";
----     before and after upgrade link clicks
---- break down by -- p22 = post_prop22 ==mapped-to==> state__view__current_page__name
INSERT OVERWRITE TABLE ${env:TMP_db}.net_asp_kpis_upgrade_link_clicks PARTITION(report_date)
SELECT
  'Upgrade Link Clicks' as metric,
  message__name as sub_metric,
  state__view__current_page__name as detail,
  SUM(IF(message__category='Custom Link',1,0)) as counts,
  partition_date as report_date
from asp_v_net_events
where (partition_date BETWEEN '${env:START_DATE}' AND '${env:END_DATE}')
  AND message__name IN(
    'Upgrade-Voice-Banner',
    'Upgrade-TV-Banner',
    'Upgrade-Internet-Banner',
    'Upgrade-Overview-Banner')
GROUP BY
  partition_date,
  message__name,
  state__view__current_page__name
;

----  {5} Are there fewer **authenticated** searches for "bill pay" and fewer IVA and fewer calls?
SELECT "\n\nNow Running {5} Are there fewer **authenticated** searches for 'bill pay' and fewer IVA and fewer calls?\n\n ";
----      use p3='Logged In' to check for authentication
-- p51 = post_prop51 ==mapped-to==> operation__user_entry__enc_text
-- p52 = post_prop52 ==mapped-to==> state__view__modal__text

INSERT OVERWRITE TABLE ${env:TMP_db}.net_asp_kpis_bill_pay_search PARTITION(report_date)
SELECT
  'Bill Pay Search' as metric,
  CASE
    WHEN LOWER(operation__user_entry__enc_text) RLIKE '.*payment.*'
      OR LOWER(operation__user_entry__enc_text) RLIKE '.*auto pay.*'
      OR LOWER(operation__user_entry__enc_text) RLIKE '.*autopay.*'
      OR (LOWER(operation__user_entry__enc_text) RLIKE '.*pay.*' AND LOWER(operation__user_entry__enc_text) RLIKE '.*bill.*') THEN 'Bill Pay IVA Entry'
    WHEN LOWER(state__view__modal__text) RLIKE '.*payment.*'
      OR LOWER(state__view__modal__text) RLIKE '.*paymybill.*'
      OR LOWER(state__view__modal__text) RLIKE '.*pay your bill.*' THEN 'Bill Pay IVA Results'
    WHEN LOWER(state__view__current_page__search_text) RLIKE '.*pay.*'
      OR LOWER(state__view__current_page__search_text) RLIKE '.*bill.*' THEN 'Bill Pay Search Terms'
  END as sub_metric,
  operation__user_entry__enc_text as iva_entries,
  state__view__modal__text as iva_results,
  state__view__current_page__search_text as search_text,
  COUNT(*) as counts,
  partition_date as report_date
from asp_v_net_events
where (partition_date BETWEEN '${env:START_DATE}' AND '${env:END_DATE}')
  AND visit__isp__status='Logged In' -- tests for **authenticated**
  AND (
    (LOWER(operation__user_entry__enc_text) RLIKE '.*payment.*'
      OR LOWER(operation__user_entry__enc_text) RLIKE '.*auto pay.*'
      OR LOWER(operation__user_entry__enc_text) RLIKE '.*autopay.*'
      OR (LOWER(operation__user_entry__enc_text) RLIKE '.*pay.*' AND LOWER(operation__user_entry__enc_text) RLIKE '.*bill.*'))
    OR (LOWER(state__view__modal__text) RLIKE '.*payment.*'
      OR LOWER(state__view__modal__text) RLIKE '.*paymybill.*'
      OR LOWER(state__view__modal__text) RLIKE '.*pay your bill.*')
    OR (LOWER(state__view__current_page__search_text) RLIKE '.*pay.*'
      OR LOWER(state__view__current_page__search_text) RLIKE '.*bill.*')
    )
GROUP BY
  partition_date,
  CASE
    WHEN LOWER(operation__user_entry__enc_text) RLIKE '.*payment.*'
      OR LOWER(operation__user_entry__enc_text) RLIKE '.*auto pay.*'
      OR LOWER(operation__user_entry__enc_text) RLIKE '.*autopay.*'
      OR (LOWER(operation__user_entry__enc_text) RLIKE '.*pay.*' AND LOWER(operation__user_entry__enc_text) RLIKE '.*bill.*') THEN 'Bill Pay IVA Entry'
    WHEN LOWER(state__view__modal__text) RLIKE '.*payment.*'
      OR LOWER(state__view__modal__text) RLIKE '.*paymybill.*'
      OR LOWER(state__view__modal__text) RLIKE '.*pay your bill.*' THEN 'Bill Pay IVA Results'
    WHEN LOWER(state__view__current_page__search_text) RLIKE '.*pay.*'
      OR LOWER(state__view__current_page__search_text) RLIKE '.*bill.*' THEN 'Bill Pay Search Terms'
  END,
    operation__user_entry__enc_text,
  state__view__modal__text,
  state__view__current_page__search_text
;

----  {6} Are users getting to Email without problems with the new UI?
SELECT "\n\nNow Running {6} Are users getting to Email without problems with the new UI?\n\n ";
----      before and after search for email/voice
----      use p3='Logged In' to check for authentication
-- p52 = post_prop52 ==mapped-to==> state__view__modal__text (IVA response)
----      p52 contains "email"
-- p55 = post_prop55 ==mapped-to==> state__view__current_page__search_text (site search keyword)
----      p55 contains "email" AND not "voice"
INSERT OVERWRITE TABLE ${env:TMP_db}.net_asp_kpis_search_email_voice PARTITION(report_date)
SELECT
  'Search Email' as metric,
  CASE
    WHEN  LOWER(state__view__modal__text) RLIKE '.*email.*' THEN 'IVA'
    WHEN (LOWER(state__view__current_page__search_text) RLIKE '.*email.*' AND
          LOWER(state__view__current_page__search_text) NOT RLIKE '.*voice.*' )  THEN 'Search'
  END as sub_metric,
  COALESCE(state__view__modal__text,state__view__current_page__search_text) as search_terms,
  COUNT(*) as counts,
  partition_date as report_date
from asp_v_net_events
where (partition_date BETWEEN '${env:START_DATE}' AND '${env:END_DATE}')
  AND visit__isp__status='Logged In' -- tests for **authenticated**
  AND (LOWER(state__view__modal__text) RLIKE '.*email.*' OR
       (LOWER(state__view__current_page__search_text) RLIKE '.*email.*' AND
        LOWER(state__view__current_page__search_text) NOT RLIKE '.*voice.*' )
      )
GROUP BY
  partition_date,
  CASE
    WHEN  LOWER(state__view__modal__text) RLIKE '.*email.*' THEN 'IVA'
    WHEN (LOWER(state__view__current_page__search_text) RLIKE '.*email.*' AND
          LOWER(state__view__current_page__search_text) NOT RLIKE '.*voice.*' )  THEN 'Search'
  END,
  state__view__modal__text,
  state__view__current_page__search_text
;

---- {7} Are users still getting to Voice with the new UI?
SELECT "\n\nNow Running {7} Are users still getting to Voice with the new UI?\n\n ";
-- before and after IVA for email/voice
----      use p3='Logged In' to check for authentication
-- p52 = post_prop52 ==mapped-to==> state__view__modal__text (IVA classification response)
-- p55 = post_prop55 ==mapped-to==> state__view__current_page__search_text (site search keyword)
INSERT OVERWRITE TABLE ${env:TMP_db}.net_asp_kpis_iva_email_voice PARTITION(report_date)
SELECT
  'Search Voice' as metric,
  CASE
    WHEN LOWER(state__view__modal__text) RLIKE '.*voice.*' THEN 'IVA'
    WHEN LOWER(state__view__current_page__search_text) RLIKE '.*voice.*' THEN 'Search'
  END as sub_metric,
  COALESCE(state__view__modal__text,state__view__current_page__search_text) as search_terms,
  COUNT(*) as counts,
  partition_date as report_date
from asp_v_net_events
where (partition_date BETWEEN '${env:START_DATE}' AND '${env:END_DATE}')
  AND visit__isp__status='Logged In' -- tests for **authenticated**
  AND (LOWER(state__view__modal__text) RLIKE '.*voice.*'
    OR LOWER(state__view__current_page__search_text) RLIKE '.*voice.*')
GROUP BY
  partition_date,
  CASE
    WHEN LOWER(state__view__modal__text) RLIKE '.*voice.*' THEN 'IVA'
    WHEN LOWER(state__view__current_page__search_text) RLIKE '.*voice.*' THEN 'Search'
  END,
  state__view__modal__text,
  state__view__current_page__search_text
;


----  {8} Are users getting to bill pay from card on home page or from local nav?
SELECT "\n\nNow Running {8} Are users getting to bill pay from card on home page or from local nav?\n\n ";
----  p22 = post_prop22 ==mapped-to==> state__view__current_page__name
----  ratio of clicks of (custom link = "localNav-billing" AND prop22 = "home-authenticated")
----                 vs. (custom link = "pay-bill.make-a-payment-hp" OR
----                                    "pay-bill.enroll-in-autopay-hp" OR
----                                    "pay-bill.view-bill-details-hp")
INSERT OVERWRITE TABLE ${env:TMP_db}.net_asp_kpis_bpln PARTITION(report_date)
SELECT 'Bill Pay Card or Local Nav' as metric,
   CASE WHEN message__name = "localNav-billing" THEN "Local Nav"
        WHEN message__name RLIKE "pay-bill.*" THEN "Bill Pay Card"
        ELSE "message__name not yet mapped"
   END as sub_metric,
   count(message__name) as counts,
   null as count_visits,
   partition_date as report_date
FROM asp_v_net_events
WHERE ( partition_date BETWEEN '${env:START_DATE}' AND '${env:END_DATE}')
AND (message__name in ( "pay-bill.make-a-payment-hp",
                        "pay-bill.enroll-in-autopay-hp",
                        "pay-bill.view-bill-details-hp")
    OR  message__name =  "localNav-billing" AND state__view__current_page__name = "home-authenticated")
GROUP BY CASE WHEN message__name = "localNav-billing" THEN "Local Nav"
              WHEN message__name RLIKE "pay-bill.*" THEN "Bill Pay Card"
              ELSE "message__name not yet mapped"
         END,
         partition_date
;

-- Insert final metrics to Aggs
INSERT OVERWRITE TABLE ${env:TMP_db}.asp_kpis_metrics_agg PARTITION(report_date)
SELECT
  metric as metric,
  sub_metric as sub_metric,
  CAST(NULL AS STRING) as detail,
  SUM(counts) as counts,
  SUM(count_visits) as count_visits,
  report_date as report_date
FROM ${env:TMP_db}.net_asp_kpis_auth
GROUP BY
  metric, sub_metric, report_date
;
INSERT OVERWRITE TABLE ${env:TMP_db}.asp_kpis_metrics_agg PARTITION(report_date)
SELECT
  metric as metric,
  sub_metric as sub_metric,
  page_name as detail,
  SUM(counts) as counts,
  CAST(NULL AS INT) as count_visits,
  report_date as report_date
FROM ${env:TMP_db}.net_asp_kpis_one_time_payments
GROUP BY
  metric, sub_metric, report_date, page_name
;
INSERT OVERWRITE TABLE ${env:TMP_db}.asp_kpis_metrics_agg PARTITION(report_date)
SELECT
  metric as metric,
  sub_metric as sub_metric,
  page_name as detail,
  SUM(counts) as counts,
  CAST(NULL AS INT) as count_visits,
  report_date as report_date
FROM ${env:TMP_db}.net_asp_kpis_auto_payments
GROUP BY
  metric, sub_metric, report_date, page_name
;
INSERT OVERWRITE TABLE ${env:TMP_db}.asp_kpis_metrics_agg PARTITION(report_date)
SELECT
  metric as metric,
  sub_metric as sub_metric,
  CAST(NULL AS STRING) as detail,
  SUM(counts) as counts,
  CAST(NULL AS INT) as count_visits,
  report_date as report_date
FROM ${env:TMP_db}.net_asp_kpis_bill_pay_search
GROUP BY
  metric, sub_metric, report_date
;
INSERT OVERWRITE TABLE ${env:TMP_db}.asp_kpis_metrics_agg PARTITION(report_date)
SELECT
  metric as metric,
  sub_metric as sub_metric,
  CAST(NULL AS STRING) as detail,
  SUM(counts) as counts,
  CAST(NULL AS INT) as count_visits,
  report_date as report_date
FROM ${env:TMP_db}.net_asp_kpis_upgrade_link_clicks
GROUP BY
  metric, sub_metric, report_date
;
INSERT OVERWRITE TABLE ${env:TMP_db}.asp_kpis_metrics_agg PARTITION(report_date)
SELECT
  metric as metric,
  sub_metric as sub_metric,
  CAST(NULL AS STRING) as detail,
  SUM(counts) as counts,
  CAST(NULL AS INT) as count_visits,
  report_date as report_date
FROM ${env:TMP_db}.net_asp_kpis_search_email_voice
GROUP BY
  metric, sub_metric, report_date
;
INSERT OVERWRITE TABLE ${env:TMP_db}.asp_kpis_metrics_agg PARTITION(report_date)
SELECT
  metric as metric,
  sub_metric as sub_metric,
  CAST(NULL AS STRING) as detail,
  SUM(counts) as counts,
  CAST(NULL AS INT) as count_visits,
  report_date as report_date
FROM ${env:TMP_db}.net_asp_kpis_iva_email_voice
GROUP BY
  metric, sub_metric, report_date
;

INSERT OVERWRITE TABLE ${env:TMP_db}.asp_kpis_metrics_agg PARTITION(report_date)
SELECT
  metric as metric,
  sub_metric as sub_metric,
  CAST(NULL AS STRING) as detail,
  SUM(counts) as counts,
  CAST(NULL AS INT) as count_visits,
  report_date as report_date
FROM ${env:TMP_db}.net_asp_kpis_bpln
GROUP BY
  metric, sub_metric, report_date
;

INSERT OVERWRITE TABLE asp_kpi_metrics_agg_daily PARTITION(partition_date)
SELECT
  lkp.metric as metric,
  lkp.sub_metric as sub_metric,
  agg.detail as detail,
  SUM(counts) as counts,
  SUM(count_visits) as count_visits,
  agg.report_date as partition_date
FROM ${env:LKP_db}.net_asp_kpis_metrics lkp
LEFT JOIN ${env:TMP_db}.asp_kpis_metrics_agg agg
ON lkp.metric = agg.metric AND lkp.sub_metric = agg.sub_metric
WHERE agg.report_date BETWEEN '${env:START_DATE}' AND '${env:END_DATE}'
GROUP BY
  agg.report_date,
  lkp.metric,
  lkp.sub_metric,
  agg.detail
;

SELECT '

***** BEGIN LOGIN ATTEMPT TIMING PTILE METRICS *****

'
;

DROP TABLE IF EXISTS ${env:TMP_db}.asp_login_attempt_timing PURGE;
CREATE TABLE IF NOT EXISTS ${env:TMP_db}.asp_login_attempt_timing AS
SELECT  state__view__current_page__name as page_name,
        visit__visit_id as visit_id,
        min(message__timestamp) AS message__timestamp,
        message__category,
        message__name,
        visit__isp__status AS login_status,
        partition_date
FROM asp_v_net_events
WHERE ( partition_date > '2017-10-20' )
AND (state__view__current_page__page_id IS NULL OR
     state__view__current_page__page_id NOT LIKE "%https://www.spectrum.net/login/?ReferringPartner=%")
AND ( (state__view__current_page__name = 'home'
        AND visit__isp__status='Logged Out'
        AND message__category='Page View')
          OR
      (state__view__current_page__name = 'home-unauth'
        AND message__category='Page View')
          OR
      (state__view__current_page__name in('login', 'home-unauth')
              AND message__category = 'Custom Link'
              AND message__name RLIKE 'Sign-| In' )
    )
GROUP BY  state__view__current_page__name,
          visit__visit_id,
          message__category,
          message__name,
          visit__isp__status,
          partition_date
;

INSERT OVERWRITE TABLE asp_login_attempt_timing_agg PARTITION (partition_date)
-- AFTER Homepage Change
SELECT  BeforeSignIn.visit_id,
        BeforeSignIn.message__timestamp as tBeforeSignIn,
        LoginAttempt.message__timestamp AS tLoginAttempt,
        LoginAttempt.message__timestamp - BeforeSignIn.message__timestamp AS difference,
        'after' AS home_page_status,
        BeforeSignIn.partition_date
FROM ${env:TMP_db}.asp_login_attempt_timing BeforeSignIn
INNER JOIN (SELECT  message__timestamp,
                    visit_id,
                    partition_date
            FROM ${env:TMP_db}.asp_login_attempt_timing
            WHERE page_name = 'home-unauth'
              AND message__category = 'Custom Link'
              AND message__name RLIKE 'Sign-| In'
              AND partition_date > '2017-10-20'
            ) LoginAttempt
ON BeforeSignIn.visit_id = LoginAttempt.visit_id
  AND BeforeSignIn.partition_date = LoginAttempt.partition_date
WHERE page_name = 'home-unauth'
  AND message__category = 'Page View'
  AND BeforeSignIn.visit_id IS NOT NULL
  AND BeforeSignIn.message__timestamp < LoginAttempt.message__timestamp
  AND BeforeSignIn.partition_date > '2017-10-20'
;


INSERT OVERWRITE TABLE asp_login_attempt_timing_agg PARTITION (partition_date)
-- BEFORE Homepage Change
SELECT  BeforeSignIn.visit_id,
        BeforeSignIn.message__timestamp as tBeforeSignIn,
        LoginAttempt.message__timestamp AS tLoginAttempt,
        LoginAttempt.message__timestamp - BeforeSignIn.message__timestamp AS difference,
        'before' AS home_page_status,
        BeforeSignIn.partition_date
FROM ${env:TMP_db}.asp_login_attempt_timing BeforeSignIn
INNER JOIN (SELECT  message__timestamp,
                    visit_id,
                    partition_date
            FROM ${env:TMP_db}.asp_login_attempt_timing
            WHERE page_name = 'login'
              AND message__category = 'Custom Link'
              AND message__name RLIKE 'Sign-| In'
              AND partition_date > '2017-10-20'
            ) LoginAttempt
ON BeforeSignIn.visit_id = LoginAttempt.visit_id
  AND BeforeSignIn.partition_date = LoginAttempt.partition_date
WHERE login_status = 'Logged Out' -- prop3
  AND message__category = 'Page View'
  AND page_name = 'home'
  AND BeforeSignIn.message__timestamp < LoginAttempt.message__timestamp
  AND BeforeSignIn.partition_date > '2017-10-20'
;

DROP TABLE IF EXISTS asp_login_attempt_timing_agg_ptile PURGE;

CREATE TABLE IF NOT EXISTS asp_login_attempt_timing_agg_ptile
AS
SELECT
partition_date AS partition_date,
home_page_status AS home_page_status,
SIZE(COLLECT_SET(visit_id)) as unique_visits,
PERCENTILE(difference,0.05) as 05_PERCENTILE,
PERCENTILE(difference,0.10) as 10_PERCENTILE,
PERCENTILE(difference,0.15) as 15_PERCENTILE,
PERCENTILE(difference,0.20) as 20_PERCENTILE,
PERCENTILE(difference,0.25) as 25_PERCENTILE,
PERCENTILE(difference,0.30) as 30_PERCENTILE,
PERCENTILE(difference,0.35) as 35_PERCENTILE,
PERCENTILE(difference,0.40) as 40_PERCENTILE,
PERCENTILE(difference,0.45) as 45_PERCENTILE,
PERCENTILE(difference,0.50) as 50_PERCENTILE,
PERCENTILE(difference,0.55) as 55_PERCENTILE,
PERCENTILE(difference,0.60) as 60_PERCENTILE,
PERCENTILE(difference,0.65) as 65_PERCENTILE,
PERCENTILE(difference,0.70) as 70_PERCENTILE,
PERCENTILE(difference,0.75) as 75_PERCENTILE,
PERCENTILE(difference,0.80) as 80_PERCENTILE,
PERCENTILE(difference,0.85) as 85_PERCENTILE,
PERCENTILE(difference,0.90) as 90_PERCENTILE,
PERCENTILE(difference,0.95) as 95_PERCENTILE,
PERCENTILE(difference,0.99) as 99_PERCENTILE
FROM
asp_login_attempt_timing_agg
GROUP BY
partition_date,
home_page_status
ORDER BY
partition_date,
home_page_status
;

SELECT '

***** END LOGIN ATTEMPT TIMING PTILE METRICS *****

'
;
