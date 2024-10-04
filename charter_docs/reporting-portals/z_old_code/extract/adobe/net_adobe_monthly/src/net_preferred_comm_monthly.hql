-------------------------------------------------------------------------------

USE ${env:ENVIRONMENT};

-------------------------------------------------------------------------------
-- Begin L-CHTR net preferred communications --

INSERT INTO net_preferred_comm 
SELECT
date_yearmonth(partition_date) AS report_date,
COUNT(CASE
WHEN (message__name='Save' AND 
(state__view__current_page__elements__name='Phone' OR
state__view__current_page__elements__name='Email' OR
state__view__current_page__elements__name='Address')) THEN '1'
END) AS contact_info_updated,
COUNT(CASE
WHEN (message__name='Save' AND
(state__view__current_page__elements__name='Billing Notifications' OR
state__view__current_page__elements__name='Account Notifications' OR
state__view__current_page__elements__name='Service Alerts' OR
state__view__current_page__elements__name='Appointment Reminders')) THEN '1'
END) AS preferences_set,
COUNT(distinct(case
WHEN (message__name='Bill Delivery Submit' AND
operation__type='Manage Your Account Online') THEN visit__account__enc_account_number
END)) AS enrolled_paperlessbilling
FROM net_events
WHERE partition_date BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
GROUP BY
date_yearmonth(partition_date);