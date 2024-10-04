-- Apr - Oct
-- My_Account_Support_Login_DeviceID_UniqueSessions_Monthly_201604-201610
SELECT
  CONCAT(YEAR(FROM_UNIXTIME(message__timestamp)), '-', lpad(MONTH(FROM_UNIXTIME(message__timestamp)), 2, 0)) AS year_month,
  CASE
    WHEN visit__account__account_number is NOT null then prod.aes_decrypt(visit__account__account_number)
    ELSE 'Unauthenticated'
  END AS account_number,
  visit__device__uuid as device_id,
  state__view__current_page__section as site_section,
  CASE
    WHEN state__view__current_page__name LIKE 'my-account.payment%' THEN 'my-account.payment'
    WHEN state__view__current_page__name LIKE 'billing-and-transactions%' THEN 'billing-and-transactions'
    WHEN state__view__current_page__name LIKE 'Cablebox-Reset%' THEN 'Cablebox-Reset'
    WHEN state__view__current_page__name LIKE 'my-account.claim-gift-coupon%' THEN 'my-account.claim-gift-coupon'
    WHEN state__view__current_page__name LIKE 'signaling-cablebox%' THEN 'signaling-cablebox'
    WHEN state__view__current_page__name LIKE 'Test-Your-Internet-Connection%' THEN 'Test-Your-Internet-Connection'
    WHEN state__view__current_page__name LIKE 'Test-Your-Phone-Connection%' THEN 'Test-Your-Phone-Connection'
    WHEN state__view__current_page__name LIKE '%STVA' THEN split(state__view__current_page__name, '_')[0]
    ELSE state__view__current_page__name
  end as page_name,
  count(distinct visit__visit_id) as unique_session_counts
FROM 
  prod.net_events 
WHERE 
  `date` BETWEEN '2016-04-01' AND '2016-10-31'
  and message__category = 'Page View'
  and (state__view__current_page__section = 'Support' 
    or state__view__current_page__section = 'support'
    or state__view__current_page__section = 'My Account' 
    or state__view__current_page__section = 'Login')
  AND message__category = 'Page View'
  AND (state__view__current_page__section = 'Support' or state__view__current_page__section = 'My Account')
GROUP BY
  CONCAT(YEAR(FROM_UNIXTIME(message__timestamp)), '-', lpad(MONTH(FROM_UNIXTIME(message__timestamp)), 2, 0)),
  CASE
    WHEN visit__account__account_number IS NOT null THEN prod.aes_decrypt(visit__account__account_number)
    ELSE 'Unauthenticated'
  END,
  visit__device__uuid,
  CASE
    WHEN state__view__current_page__name LIKE 'my-account.payment%' THEN 'my-account.payment'
    WHEN state__view__current_page__name LIKE 'billing-and-transactions%' THEN 'billing-and-transactions'
    WHEN state__view__current_page__name LIKE 'Cablebox-Reset%' THEN 'Cablebox-Reset'
    WHEN state__view__current_page__name LIKE 'my-account.claim-gift-coupon%' THEN 'my-account.claim-gift-coupon'
    WHEN state__view__current_page__name LIKE 'signaling-cablebox%' THEN 'signaling-cablebox'
    WHEN state__view__current_page__name LIKE 'Test-Your-Internet-Connection%' THEN 'Test-Your-Internet-Connection'
    WHEN state__view__current_page__name LIKE 'Test-Your-Phone-Connection%' THEN 'Test-Your-Phone-Connection'
    WHEN state__view__current_page__name LIKE '%STVA' THEN split(state__view__current_page__name, '_')[0]
    ELSE state__view__current_page__name
  end,
  state__view__current_page__section

--Sept - Oct
--My_Account_Support_Login_DeviceID_Monthly_Sessions_PageViews
SELECT
  CONCAT(YEAR(FROM_UNIXTIME(message__timestamp)), '-', lpad(MONTH(FROM_UNIXTIME(message__timestamp)), 2, 0)) AS year_month,
  CASE
    WHEN prod.aes_decrypt(visit__account__account_number) IS NOT null THEN prod.aes_decrypt(visit__account__account_number)
    ELSE 'Unauthenticated'
  END AS account_number,
  visit__device__uuid as device_id,
  state__view__current_page__section as site_section,
  CASE
    WHEN state__view__current_page__name LIKE 'my-account.payment%' THEN 'my-account.payment'
    WHEN state__view__current_page__name LIKE 'billing-and-transactions%' THEN 'billing-and-transactions'
    WHEN state__view__current_page__name LIKE 'Cablebox-Reset%' THEN 'Cablebox-Reset'
    WHEN state__view__current_page__name LIKE 'my-account.claim-gift-coupon%' THEN 'my-account.claim-gift-coupon'
    WHEN state__view__current_page__name LIKE 'signaling-cablebox%' THEN 'signaling-cablebox'
    WHEN state__view__current_page__name LIKE 'Test-Your-Internet-Connection%' THEN 'Test-Your-Internet-Connection'
    WHEN state__view__current_page__name LIKE 'Test-Your-Phone-Connection%' THEN 'Test-Your-Phone-Connection'
    WHEN state__view__current_page__name LIKE '%STVA' THEN split(state__view__current_page__name, '_')[0]
    ELSE state__view__current_page__name
  end as page_name,
  count(distinct visit__visit_id) as unique_session_counts,
  count(*) as total_page_view_counts

FROM 
  prod.net_events 
WHERE visit__device__uuid is not null
  and `date` BETWEEN '2016-09-01' AND '2016-10-31'
  and message__category = 'Page View'
  and (state__view__current_page__section = 'Support' 
    or state__view__current_page__section = 'support'
    or state__view__current_page__section = 'My Account' 
    or state__view__current_page__section = 'Login')

GROUP BY
  CONCAT(YEAR(FROM_UNIXTIME(message__timestamp)), '-', lpad(MONTH(FROM_UNIXTIME(message__timestamp)), 2, 0)),
  CASE
    WHEN prod.aes_decrypt(visit__account__account_number) IS NOT null THEN prod.aes_decrypt(visit__account__account_number)
    ELSE 'Unauthenticated'
  END,
  visit__device__uuid,
  CASE
    WHEN state__view__current_page__name LIKE 'my-account.payment%' THEN 'my-account.payment'
    WHEN state__view__current_page__name LIKE 'billing-and-transactions%' THEN 'billing-and-transactions'
    WHEN state__view__current_page__name LIKE 'Cablebox-Reset%' THEN 'Cablebox-Reset'
    WHEN state__view__current_page__name LIKE 'my-account.claim-gift-coupon%' THEN 'my-account.claim-gift-coupon'
    WHEN state__view__current_page__name LIKE 'signaling-cablebox%' THEN 'signaling-cablebox'
    WHEN state__view__current_page__name LIKE 'Test-Your-Internet-Connection%' THEN 'Test-Your-Internet-Connection'
    WHEN state__view__current_page__name LIKE 'Test-Your-Phone-Connection%' THEN 'Test-Your-Phone-Connection'
    WHEN state__view__current_page__name LIKE '%STVA' THEN split(state__view__current_page__name, '_')[0]
    ELSE state__view__current_page__name
  end,
  state__view__current_page__section;
