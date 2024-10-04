
select 
  *
from
  prod.net_events
where 
  `date` = '2016-06-23'
  and state__view__current_page__name = 'AutoPay-manage-saved'
limit 100




'AutoPay-manage-saved'
AutoPay-manage-saved

Please supply an output from Adobe Analytics that includes all visitors to the Auto Pay payment method edit confirmation page (listed below) from June-September 2016.
Include the following fields:
•Acc’t number
•Date/time
•Username
•Page name (AutoPay-manage-saved)
•Value of the AutoPay-Payment-Method (should be included for all of these pages views and have a value of "Credit/Debit" or "Checking and Savings.")


select distinct
  net_events.visit__referrer_link,
  net_events.message__category,
  net_events.message__name,
  net_events.state__view__current_page__name,
  net_events.state__view__current_page__section,
  net_events.state__view__current_page__sub_section,
  net_events.state__view__previous_page__name,
  net_events.state__view__previous_page__section,
  net_events.state__view__previous_page__page_id,
  net_events.operation__type,
  net_events.operation__user_entry__type
from
  prod.net_events
where 
  `date` = '2016-06-23'
  -- and state__view__current_page__name = 'AutoPay-manage-saved'
  and operation__type in ('Credit/Debit', 'Checking and Savings')
-- limit 100



select distinct
  message__category,
  message__name,
  state__view__current_page__name,
  state__view__current_page__section,
  state__view__current_page__sub_section,
  state__view__previous_page__name,
  state__view__previous_page__section,
  operation__type,
  operation__user_entry__type
from
  prod.net_events
where 
  `date` = '2016-06-23'
  -- and state__view__current_page__name = 'AutoPay-manage-saved'
  and (
    lower(operation__type) like '%checking%'
    or lower(operation__type) like '%saving%'
    or lower(operation__type) like '%credit%'
  )
-- limit 100



---------------------------------------------------
-- XGANALYTIC-4410
-- page views for 'AutoPay-manage-saved'
--   checking visit id for a preceding payment type custom link (payment type not on confirmation event)
--   
--    • Acc’t number
--    • Date/time
--    • Username
--    • Page name (AutoPay-manage-saved)
--    • Value of the AutoPay-Payment-Method (should be included for all of these pages views and have a value of "Credit/Debit" or "Checking and Savings.")
--   https://jira.charter.com/browse/XGANALYTIC-4410
SELECT 
  -- b.`date`, 
  b.date_time as date_time,
  b.account_number as account_number,
  b.username as username,

  -- b.message__name,
  b.state__view__current_page__name as page_name,
  b.state__view__current_page__section as page_section,
  -- b.operation__type,

  b.prev_message_name as previous_custom_link_name,
  b.prev_page as previous_page,
  b.prev_sec as previous_section,
  b.prev_operation__type as previous_additional_info,
  
  COUNT(*) as count
FROM (
  SELECT 
    `date`,
    account_number,
    date_time,
    username,

    state__view__current_page__name,
    state__view__current_page__section,
    operation__type,
    message__name,
    message__category,

    LAG(a.message__name) 
      OVER (PARTITION BY a.visit__visit_id 
        ORDER BY a.message__timestamp)
      AS prev_message_name,
    LAG(a.state__view__current_page__name) 
      OVER (PARTITION BY a.visit__visit_id 
        ORDER BY a.message__timestamp)
      AS prev_page,
    LAG(a.state__view__current_page__section) 
      OVER (PARTITION BY a.visit__visit_id 
        ORDER BY a.message__timestamp)
      AS prev_sec,
    LAG(a.operation__type) 
      OVER (PARTITION BY a.visit__visit_id 
        ORDER BY a.message__timestamp)
      AS prev_operation__type
  FROM (
    SELECT 
      `date`,
      prod.aes_decrypt(visit__account__account_number) as account_number,
      from_unixtime(message__timestamp) as date_time,
      prod.aes_decrypt(visit__user__id) as username,
      visit__visit_id,
      state__view__current_page__name,
      state__view__current_page__section,
      operation__type,
      message__name,
      message__timestamp,
      message__category
    FROM prod.net_events 
    WHERE 
      -- `date` >= '2016-06-01'
      `date` between '2016-06-01' and '2016-06-30'
      -- `date` = '2016-07-11'
      AND (
        -- message__category = 'Page View'
        state__view__current_page__name = 'AutoPay-manage-saved'
        OR (
          lower(operation__type) like '%checking%'
          OR lower(operation__type) like '%saving%'
          OR lower(operation__type) like '%credit%'
        )
      )
    ORDER BY message__timestamp 
  ) a -- grab all page views or payments
) b -- list all page fields with previous section
WHERE 
  state__view__current_page__name = 'AutoPay-manage-saved'
  AND message__category = 'Page View'
  AND (
    lower(prev_operation__type) like '%checking%'
    OR lower(prev_operation__type) like '%saving%'
    OR lower(prev_operation__type) like '%credit%'
  )
GROUP BY 
  -- b.`date`, 
  b.account_number,
  b.date_time,
  b.username,

  b.message__name,
  b.state__view__current_page__name,
  b.state__view__current_page__section,
  b.operation__type,

  b.prev_message_name,
  b.prev_page, 
  b.prev_sec,
  b.prev_operation__type
;





