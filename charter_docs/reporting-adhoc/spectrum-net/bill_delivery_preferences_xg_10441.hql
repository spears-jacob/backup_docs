--bill_delivery_preferences_xg_10441



SELECt " The start and end dates are used to set the time frame for the query in a single place.

Usage:          BETWEEN ${hiveconf:start_date} AND ${hiveconf:end_date}
  instead of:   BETWEEN '2018-01-01' AND '2018-04-30'

" ;

SET start_date='2018-01-01' ;
SET end_date='2018-04-30';


--------------------------------------------
---------------- DAILY AGGS ----------------
--------------------------------------------

--RESI INSTANCES--
--This pulls daily instance counts for each resi metric
DROP TABLE IF EXISTS dev.asp_adhoc_bill_del_instances PURGE;
CREATE TABLE IF NOT EXISTS dev.asp_adhoc_bill_del_instances AS
SELECT
    COUNT(*) as value,
    'instances' AS met_type,
    CASE
      WHEN message__category = 'Page View'
        AND visit__settings['post_prop48'] = 'https://www.spectrum.net/billing-and-transactions/bill-delivery/'
      THEN 'view_bill_delivery'
      WHEN message__category = 'Custom Link'
        AND message__name = 'Bill Delivery Submit'
        AND visit__settings['post_prop24'] = 'Bill Delivery'
      THEN 'click_bill_del_save'
      WHEN message__category = 'Custom Link'
        AND message__name = 'pay-bill.manage-paperless-billing'
        AND visit__settings['post_prop25'] = 'bill-delivery-settings'
      THEN 'click_manage_bill_del_opt'
      ELSE null
    END AS metric,
    partition_date
FROM asp_v_net_events
WHERE (partition_date BETWEEN ${hiveconf:start_date} AND ${hiveconf:end_date})
GROUP BY
partition_date,
CASE
  WHEN message__category = 'Page View'
    AND visit__settings['post_prop48'] = 'https://www.spectrum.net/billing-and-transactions/bill-delivery/'
  THEN 'view_bill_delivery'
  WHEN message__category = 'Custom Link'
    AND message__name = 'Bill Delivery Submit'
    AND visit__settings['post_prop24'] = 'Bill Delivery'
  THEN 'click_bill_del_save'
  WHEN message__category = 'Custom Link'
    AND message__name = 'pay-bill.manage-paperless-billing'
    AND visit__settings['post_prop25'] = 'bill-delivery-settings'
  THEN 'click_manage_bill_del_opt'
  ELSE null END
ORDER BY
partition_date
;

--RESI VISITS--
--This pulls daily visit counts for each resi metric
DROP TABLE IF EXISTS dev.asp_adhoc_bill_del_visits PURGE;
CREATE TABLE IF NOT EXISTS dev.asp_adhoc_bill_del_visits AS
SELECT
    COUNT(DISTINCT(visit__visit_id)) as value,
    'visits' AS met_type,
    CASE
      WHEN message__category = 'Page View'
        AND visit__settings['post_prop48'] = 'https://www.spectrum.net/billing-and-transactions/bill-delivery/'
      THEN 'view_bill_delivery'
      WHEN message__category = 'Custom Link'
        AND message__name = 'Bill Delivery Submit'
        AND visit__settings['post_prop24'] = 'Bill Delivery'
      THEN 'click_bill_del_save'
      WHEN message__category = 'Custom Link'
        AND message__name = 'pay-bill.manage-paperless-billing'
        AND visit__settings['post_prop25'] = 'bill-delivery-settings'
      THEN 'click_manage_bill_del_opt'
      ELSE null
    END AS metric,
    partition_date
FROM asp_v_net_events
WHERE (partition_date BETWEEN ${hiveconf:start_date} AND ${hiveconf:end_date})
GROUP BY
partition_date,
CASE
  WHEN message__category = 'Page View'
    AND visit__settings['post_prop48'] = 'https://www.spectrum.net/billing-and-transactions/bill-delivery/'
  THEN 'view_bill_delivery'
  WHEN message__category = 'Custom Link'
    AND message__name = 'Bill Delivery Submit'
    AND visit__settings['post_prop24'] = 'Bill Delivery'
  THEN 'click_bill_del_save'
  WHEN message__category = 'Custom Link'
    AND message__name = 'pay-bill.manage-paperless-billing'
    AND visit__settings['post_prop25'] = 'bill-delivery-settings'
  THEN 'click_manage_bill_del_opt'
  ELSE null END
ORDER BY
partition_date
;

--RESI ACCOUNTS--
--This pulls daily account counts for each resi metric
DROP TABLE IF EXISTS dev.asp_adhoc_bill_del_accounts PURGE;
CREATE TABLE IF NOT EXISTS dev.asp_adhoc_bill_del_accounts AS
SELECT
    COUNT(DISTINCT(visit__account__account_number)) AS value,
    'accounts' AS met_type,
    CASE
      WHEN message__category = 'Page View'
        AND visit__settings['post_prop48'] = 'https://www.spectrum.net/billing-and-transactions/bill-delivery/'
      THEN 'view_bill_delivery'
      WHEN message__category = 'Custom Link'
        AND message__name = 'Bill Delivery Submit'
        AND visit__settings['post_prop24'] = 'Bill Delivery'
      THEN 'click_bill_del_save'
      WHEN message__category = 'Custom Link'
        AND message__name = 'pay-bill.manage-paperless-billing'
        AND visit__settings['post_prop25'] = 'bill-delivery-settings'
      THEN 'click_manage_bill_del_opt'
      ELSE null
    END AS metric,
    partition_date
FROM asp_v_net_events
WHERE (partition_date BETWEEN ${hiveconf:start_date} AND ${hiveconf:end_date})
GROUP BY
partition_date,
CASE
  WHEN message__category = 'Page View'
    AND visit__settings['post_prop48'] = 'https://www.spectrum.net/billing-and-transactions/bill-delivery/'
  THEN 'view_bill_delivery'
  WHEN message__category = 'Custom Link'
    AND message__name = 'Bill Delivery Submit'
    AND visit__settings['post_prop24'] = 'Bill Delivery'
  THEN 'click_bill_del_save'
  WHEN message__category = 'Custom Link'
    AND message__name = 'pay-bill.manage-paperless-billing'
    AND visit__settings['post_prop25'] = 'bill-delivery-settings'
  THEN 'click_manage_bill_del_opt'
  ELSE null END
ORDER BY
partition_date
;

--SB INSTANCES--
--This pulls daily instance counts for each SB metric
DROP TABLE IF EXISTS dev.asp_adhoc_sb_bill_del_instances PURGE;
CREATE TABLE IF NOT EXISTS dev.asp_adhoc_sb_bill_del_instances AS
SELECT
    COUNT(*) as value,
    'instances' AS met_type,
    CASE
      WHEN message__category = 'Custom Link'
        AND message__name = 'Toggle Paperless billing'
        AND visit__settings['post_prop75'] = 'https://www.spectrumbusiness.net/profile-and-settings'
      THEN 'click_toggle_paperless_bill'
      WHEN message__category = 'Custom Link'
        AND message__name = 'pay-bill.manage-billing-notifications'
        AND visit__settings['post_prop75'] = 'https://www.spectrumbusiness.net/billing/pay-bill'
      THEN 'click_manage_bill_notifications'
      ELSE null
    END AS metric,
    partition_date_utc
FROM asp_v_sbnet_events
WHERE (partition_date_utc BETWEEN ${hiveconf:start_date} AND ${hiveconf:end_date})
GROUP BY
partition_date_utc,
CASE
  WHEN message__category = 'Custom Link'
    AND message__name = 'Toggle Paperless billing'
    AND visit__settings['post_prop75'] = 'https://www.spectrumbusiness.net/profile-and-settings'
  THEN 'click_toggle_paperless_bill'
  WHEN message__category = 'Custom Link'
    AND message__name = 'pay-bill.manage-billing-notifications'
    AND visit__settings['post_prop75'] = 'https://www.spectrumbusiness.net/billing/pay-bill'
  THEN 'click_manage_bill_notifications'
  ELSE null END
ORDER BY
partition_date_utc
;

--SB VISITS--
--This pulls daily visit counts for each SB metric
DROP TABLE IF EXISTS dev.asp_adhoc_sb_bill_del_visits PURGE;
CREATE TABLE IF NOT EXISTS dev.asp_adhoc_sb_bill_del_visits AS
SELECT
    COUNT(DISTINCT(visit__visit_id)) as value,
    'visits' AS met_type,
    CASE
      WHEN message__category = 'Custom Link'
        AND message__name = 'Toggle Paperless billing'
        AND visit__settings['post_prop75'] = 'https://www.spectrumbusiness.net/profile-and-settings'
      THEN 'click_toggle_paperless_bill'
      WHEN message__category = 'Custom Link'
        AND message__name = 'pay-bill.manage-billing-notifications'
        AND visit__settings['post_prop75'] = 'https://www.spectrumbusiness.net/billing/pay-bill'
      THEN 'click_manage_bill_notifications'
      ELSE null
    END AS metric,
    partition_date_utc
FROM asp_v_sbnet_events
WHERE (partition_date_utc BETWEEN ${hiveconf:start_date} AND ${hiveconf:end_date})
GROUP BY
partition_date_utc,
CASE
  WHEN message__category = 'Custom Link'
    AND message__name = 'Toggle Paperless billing'
    AND visit__settings['post_prop75'] = 'https://www.spectrumbusiness.net/profile-and-settings'
  THEN 'click_toggle_paperless_bill'
  WHEN message__category = 'Custom Link'
    AND message__name = 'pay-bill.manage-billing-notifications'
    AND visit__settings['post_prop75'] = 'https://www.spectrumbusiness.net/billing/pay-bill'
  THEN 'click_manage_bill_notifications'
  ELSE null END
ORDER BY
partition_date_utc
;

--SB ACCOUNTS--
--This pulls daily account counts for each SB metric
DROP TABLE IF EXISTS dev.asp_adhoc_sb_bill_del_accounts PURGE;
CREATE TABLE IF NOT EXISTS dev.asp_adhoc_sb_bill_del_accounts AS
SELECT
    COUNT(DISTINCT(visit__account__account_number)) AS value,
    'accounts' AS met_type,
    CASE
      WHEN message__category = 'Custom Link'
        AND message__name = 'Toggle Paperless billing'
        AND visit__settings['post_prop75'] = 'https://www.spectrumbusiness.net/profile-and-settings'
      THEN 'click_toggle_paperless_bill'
      WHEN message__category = 'Custom Link'
        AND message__name = 'pay-bill.manage-billing-notifications'
        AND visit__settings['post_prop75'] = 'https://www.spectrumbusiness.net/billing/pay-bill'
      THEN 'click_manage_bill_notifications'
      ELSE null
    END AS metric,
    partition_date_utc
FROM asp_v_sbnet_events
WHERE (partition_date_utc BETWEEN ${hiveconf:start_date} AND ${hiveconf:end_date})
GROUP BY
partition_date_utc,
CASE
  WHEN message__category = 'Custom Link'
    AND message__name = 'Toggle Paperless billing'
    AND visit__settings['post_prop75'] = 'https://www.spectrumbusiness.net/profile-and-settings'
  THEN 'click_toggle_paperless_bill'
  WHEN message__category = 'Custom Link'
    AND message__name = 'pay-bill.manage-billing-notifications'
    AND visit__settings['post_prop75'] = 'https://www.spectrumbusiness.net/billing/pay-bill'
  THEN 'click_manage_bill_notifications'
  ELSE null END
ORDER BY
partition_date_utc
;


--------------------------------------------
---------------- DAILY VIEWS ---------------
--------------------------------------------


--RESI DAILY COMBINED VIEW
--Combines the Resi metrics into one view
DROP VIEW IF EXISTS dev.asp_adhoc_bill_del_combined;
CREATE VIEW IF NOT EXISTS dev.asp_adhoc_bill_del_combined AS
SELECT
value,
met_type,
metric,
partition_date
FROM dev.asp_adhoc_bill_del_instances
  UNION ALL
  SELECT
  value,
  met_type,
  metric,
  partition_date
  FROM dev.asp_adhoc_bill_del_visits
    UNION ALL
    SELECT
    value,
    met_type,
    metric,
    partition_date
    FROM dev.asp_adhoc_bill_del_accounts
;

--SB DAILY COMBINED VIEW
--Combines the SB metrics into one view
DROP VIEW IF EXISTS dev.asp_adhoc_sb_bill_del_combined;
CREATE VIEW IF NOT EXISTS dev.asp_adhoc_sb_bill_del_combined AS
SELECT
value,
met_type,
metric,
partition_date_utc
FROM dev.asp_adhoc_sb_bill_del_instances
  UNION ALL
  SELECT
  value,
  met_type,
  metric,
  partition_date_utc
  FROM dev.asp_adhoc_sb_bill_del_visits
    UNION ALL
    SELECT
    value,
    met_type,
    metric,
    partition_date_utc
    FROM dev.asp_adhoc_sb_bill_del_accounts
;


--------------------------------------------
--------------- MONTHLY AGGS ---------------
--------------------------------------------


--RESI MONTHLY INSTANCES--
--This pulls monthly instance counts for each resi metric
DROP TABLE IF EXISTS dev.asp_adhoc_bill_del_mo_instances PURGE;
CREATE TABLE IF NOT EXISTS dev.asp_adhoc_bill_del_mo_instances AS
SELECT
    COUNT(*) as value,
    'instances' AS met_type,
    CASE
      WHEN message__category = 'Page View'
        AND visit__settings['post_prop48'] = 'https://www.spectrum.net/billing-and-transactions/bill-delivery/'
      THEN 'view_bill_delivery'
      WHEN message__category = 'Custom Link'
        AND message__name = 'Bill Delivery Submit'
        AND visit__settings['post_prop24'] = 'Bill Delivery'
      THEN 'click_bill_del_save'
      WHEN message__category = 'Custom Link'
        AND message__name = 'pay-bill.manage-paperless-billing'
        AND visit__settings['post_prop25'] = 'bill-delivery-settings'
      THEN 'click_manage_bill_del_opt'
      ELSE null
    END AS metric,
    SUBSTRING(partition_date,1,7) AS year_month
FROM asp_v_net_events
WHERE (partition_date BETWEEN ${hiveconf:start_date} AND ${hiveconf:end_date})
GROUP BY
SUBSTRING(partition_date,1,7),
CASE
  WHEN message__category = 'Page View'
    AND visit__settings['post_prop48'] = 'https://www.spectrum.net/billing-and-transactions/bill-delivery/'
  THEN 'view_bill_delivery'
  WHEN message__category = 'Custom Link'
    AND message__name = 'Bill Delivery Submit'
    AND visit__settings['post_prop24'] = 'Bill Delivery'
  THEN 'click_bill_del_save'
  WHEN message__category = 'Custom Link'
    AND message__name = 'pay-bill.manage-paperless-billing'
    AND visit__settings['post_prop25'] = 'bill-delivery-settings'
  THEN 'click_manage_bill_del_opt'
  ELSE null END
ORDER BY
year_month
;

--RESI MONTHLY VISITS--
--This pulls monthly visit counts for each resi metric
DROP TABLE IF EXISTS dev.asp_adhoc_bill_del_mo_visits PURGE;
CREATE TABLE IF NOT EXISTS dev.asp_adhoc_bill_del_mo_visits AS
SELECT
    COUNT(DISTINCT(visit__visit_id)) as value,
    'visits' AS met_type,
    CASE
      WHEN message__category = 'Page View'
        AND visit__settings['post_prop48'] = 'https://www.spectrum.net/billing-and-transactions/bill-delivery/'
      THEN 'view_bill_delivery'
      WHEN message__category = 'Custom Link'
        AND message__name = 'Bill Delivery Submit'
        AND visit__settings['post_prop24'] = 'Bill Delivery'
      THEN 'click_bill_del_save'
      WHEN message__category = 'Custom Link'
        AND message__name = 'pay-bill.manage-paperless-billing'
        AND visit__settings['post_prop25'] = 'bill-delivery-settings'
      THEN 'click_manage_bill_del_opt'
      ELSE null
    END AS metric,
    SUBSTRING(partition_date,1,7) AS year_month
FROM asp_v_net_events
WHERE (partition_date BETWEEN ${hiveconf:start_date} AND ${hiveconf:end_date})
GROUP BY
SUBSTRING(partition_date,1,7),
CASE
  WHEN message__category = 'Page View'
    AND visit__settings['post_prop48'] = 'https://www.spectrum.net/billing-and-transactions/bill-delivery/'
  THEN 'view_bill_delivery'
  WHEN message__category = 'Custom Link'
    AND message__name = 'Bill Delivery Submit'
    AND visit__settings['post_prop24'] = 'Bill Delivery'
  THEN 'click_bill_del_save'
  WHEN message__category = 'Custom Link'
    AND message__name = 'pay-bill.manage-paperless-billing'
    AND visit__settings['post_prop25'] = 'bill-delivery-settings'
  THEN 'click_manage_bill_del_opt'
  ELSE null END
ORDER BY
year_month
;

--RESI MONTHLY ACCOUNTS--
--This pulls monthly account counts for each resi metric
DROP TABLE IF EXISTS dev.asp_adhoc_bill_del_mo_accounts PURGE;
CREATE TABLE IF NOT EXISTS dev.asp_adhoc_bill_del_mo_accounts AS
SELECT
    COUNT(DISTINCT(visit__account__account_number)) AS value,
    'accounts' AS met_type,
    CASE
      WHEN message__category = 'Page View'
        AND visit__settings['post_prop48'] = 'https://www.spectrum.net/billing-and-transactions/bill-delivery/'
      THEN 'view_bill_delivery'
      WHEN message__category = 'Custom Link'
        AND message__name = 'Bill Delivery Submit'
        AND visit__settings['post_prop24'] = 'Bill Delivery'
      THEN 'click_bill_del_save'
      WHEN message__category = 'Custom Link'
        AND message__name = 'pay-bill.manage-paperless-billing'
        AND visit__settings['post_prop25'] = 'bill-delivery-settings'
      THEN 'click_manage_bill_del_opt'
      ELSE null
    END AS metric,
    SUBSTRING(partition_date,1,7) AS year_month
FROM asp_v_net_events
WHERE (partition_date BETWEEN ${hiveconf:start_date} AND ${hiveconf:end_date})
GROUP BY
SUBSTRING(partition_date,1,7),
CASE
  WHEN message__category = 'Page View'
    AND visit__settings['post_prop48'] = 'https://www.spectrum.net/billing-and-transactions/bill-delivery/'
  THEN 'view_bill_delivery'
  WHEN message__category = 'Custom Link'
    AND message__name = 'Bill Delivery Submit'
    AND visit__settings['post_prop24'] = 'Bill Delivery'
  THEN 'click_bill_del_save'
  WHEN message__category = 'Custom Link'
    AND message__name = 'pay-bill.manage-paperless-billing'
    AND visit__settings['post_prop25'] = 'bill-delivery-settings'
  THEN 'click_manage_bill_del_opt'
  ELSE null END
ORDER BY
year_month
;

--SB MONTHLY INSTANCES--
--This pulls monthly instance counts for each SB metric
DROP TABLE IF EXISTS dev.asp_adhoc_sb_bill_del_mo_instances PURGE;
CREATE TABLE IF NOT EXISTS dev.asp_adhoc_sb_bill_del_mo_instances AS
SELECT
    COUNT(*) as value,
    'instances' AS met_type,
    CASE
      WHEN message__category = 'Custom Link'
        AND message__name = 'Toggle Paperless billing'
        AND visit__settings['post_prop75'] = 'https://www.spectrumbusiness.net/profile-and-settings'
      THEN 'click_toggle_paperless_bill'
      WHEN message__category = 'Custom Link'
        AND message__name = 'pay-bill.manage-billing-notifications'
        AND visit__settings['post_prop75'] = 'https://www.spectrumbusiness.net/billing/pay-bill'
      THEN 'click_manage_bill_notifications'
      ELSE null
    END AS metric,
    SUBSTRING(partition_date_utc,1,7) AS year_month
FROM asp_v_sbnet_events
WHERE (partition_date_utc BETWEEN ${hiveconf:start_date} AND ${hiveconf:end_date})
GROUP BY
SUBSTRING(partition_date_utc,1,7),
CASE
  WHEN message__category = 'Custom Link'
    AND message__name = 'Toggle Paperless billing'
    AND visit__settings['post_prop75'] = 'https://www.spectrumbusiness.net/profile-and-settings'
  THEN 'click_toggle_paperless_bill'
  WHEN message__category = 'Custom Link'
    AND message__name = 'pay-bill.manage-billing-notifications'
    AND visit__settings['post_prop75'] = 'https://www.spectrumbusiness.net/billing/pay-bill'
  THEN 'click_manage_bill_notifications'
  ELSE null END
ORDER BY
year_month
;

--SB MONTHLY VISITS--
--This pulls monthly visit counts for each SB metric
DROP TABLE IF EXISTS dev.asp_adhoc_sb_bill_del_mo_visits PURGE;
CREATE TABLE IF NOT EXISTS dev.asp_adhoc_sb_bill_del_mo_visits AS
SELECT
    COUNT(DISTINCT(visit__visit_id)) as value,
    'visits' AS met_type,
    CASE
      WHEN message__category = 'Custom Link'
        AND message__name = 'Toggle Paperless billing'
        AND visit__settings['post_prop75'] = 'https://www.spectrumbusiness.net/profile-and-settings'
      THEN 'click_toggle_paperless_bill'
      WHEN message__category = 'Custom Link'
        AND message__name = 'pay-bill.manage-billing-notifications'
        AND visit__settings['post_prop75'] = 'https://www.spectrumbusiness.net/billing/pay-bill'
      THEN 'click_manage_bill_notifications'
      ELSE null
    END AS metric,
    SUBSTRING(partition_date_utc,1,7) AS year_month
FROM asp_v_sbnet_events
WHERE (partition_date_utc BETWEEN ${hiveconf:start_date} AND ${hiveconf:end_date})
GROUP BY
SUBSTRING(partition_date_utc,1,7),
CASE
  WHEN message__category = 'Custom Link'
    AND message__name = 'Toggle Paperless billing'
    AND visit__settings['post_prop75'] = 'https://www.spectrumbusiness.net/profile-and-settings'
  THEN 'click_toggle_paperless_bill'
  WHEN message__category = 'Custom Link'
    AND message__name = 'pay-bill.manage-billing-notifications'
    AND visit__settings['post_prop75'] = 'https://www.spectrumbusiness.net/billing/pay-bill'
  THEN 'click_manage_bill_notifications'
  ELSE null END
ORDER BY
year_month
;

--SB MONTHLY ACCOUNTS--
--This pulls monthly account counts for each SB metric
DROP TABLE IF EXISTS dev.asp_adhoc_sb_bill_del_mo_accounts PURGE;
CREATE TABLE IF NOT EXISTS dev.asp_adhoc_sb_bill_del_mo_accounts AS
SELECT
    COUNT(DISTINCT(visit__account__account_number)) AS value,
    'accounts' AS met_type,
    CASE
      WHEN message__category = 'Custom Link'
        AND message__name = 'Toggle Paperless billing'
        AND visit__settings['post_prop75'] = 'https://www.spectrumbusiness.net/profile-and-settings'
      THEN 'click_toggle_paperless_bill'
      WHEN message__category = 'Custom Link'
        AND message__name = 'pay-bill.manage-billing-notifications'
        AND visit__settings['post_prop75'] = 'https://www.spectrumbusiness.net/billing/pay-bill'
      THEN 'click_manage_bill_notifications'
      ELSE null
    END AS metric,
    SUBSTRING(partition_date_utc,1,7) AS year_month
FROM asp_v_sbnet_events
WHERE (partition_date_utc BETWEEN ${hiveconf:start_date} AND ${hiveconf:end_date})
GROUP BY
SUBSTRING(partition_date_utc,1,7),
CASE
  WHEN message__category = 'Custom Link'
    AND message__name = 'Toggle Paperless billing'
    AND visit__settings['post_prop75'] = 'https://www.spectrumbusiness.net/profile-and-settings'
  THEN 'click_toggle_paperless_bill'
  WHEN message__category = 'Custom Link'
    AND message__name = 'pay-bill.manage-billing-notifications'
    AND visit__settings['post_prop75'] = 'https://www.spectrumbusiness.net/billing/pay-bill'
  THEN 'click_manage_bill_notifications'
  ELSE null END
ORDER BY
year_month
;


--------------------------------------------
--------------- MONTHLY VIEWS --------------
--------------------------------------------


--RESI MONTHLY COMBINED VIEW
--Combines the Resi metrics into one view
DROP VIEW IF EXISTS dev.asp_adhoc_bill_del_mo_combined;
CREATE VIEW IF NOT EXISTS dev.asp_adhoc_bill_del_mo_combined AS
SELECT
value,
met_type,
metric,
year_month
FROM dev.asp_adhoc_bill_del_mo_instances
  UNION ALL
  SELECT
  value,
  met_type,
  metric,
  year_month
  FROM dev.asp_adhoc_bill_del_mo_visits
    UNION ALL
    SELECT
    value,
    met_type,
    metric,
    year_month
    FROM dev.asp_adhoc_bill_del_mo_accounts
;

--SB MONTHLY COMBINED VIEW
--Combines the SB metrics into one view
DROP VIEW IF EXISTS dev.asp_adhoc_sb_bill_del_mo_combined;
CREATE VIEW IF NOT EXISTS dev.asp_adhoc_sb_bill_del_mo_combined AS
SELECT
value,
met_type,
metric,
year_month
FROM dev.asp_adhoc_sb_bill_del_mo_instances
  UNION ALL
  SELECT
  value,
  met_type,
  metric,
  year_month
  FROM dev.asp_adhoc_sb_bill_del_mo_visits
    UNION ALL
    SELECT
    value,
    met_type,
    metric,
    year_month
    FROM dev.asp_adhoc_sb_bill_del_mo_accounts
;
