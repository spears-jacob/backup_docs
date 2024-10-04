-- Daily June 1-7
select
`date`,
case
	when visit__account__account_number is not null then prod.aes_decrypt(visit__account__account_number)
	else 'Unauthenticated'
end as account_number,
visit__device__uuid as device_id,
state__view__current_page__section as site_section,
case
	when state__view__current_page__name like 'my-account.payment%' then 'my-account.payment'
	when state__view__current_page__name like 'billing-and-transactions%' then 'billing-and-transactions'
	when state__view__current_page__name like 'Cablebox-Reset%' then 'Cablebox-Reset'
	when state__view__current_page__name like 'my-account.claim-gift-coupon%' then 'my-account.claim-gift-coupon'
	when state__view__current_page__name like 'signaling-cablebox%' then 'signaling-cablebox'
	when state__view__current_page__name like 'Test-Your-Internet-Connection%' then 'Test-Your-Internet-Connection'
	when state__view__current_page__name like 'Test-Your-Phone-Connection%' then 'Test-Your-Phone-Connection'
	when state__view__current_page__name like '%STVA' then split(state__view__current_page__name, '_')[0]
	else state__view__current_page__name
end as page_name,
count(distinct visit__visit_id) as unique_session_counts

from prod.net_events 
where `date` between '2016-06-01' and '2016-06-07'
	and message__category = 'Page View'
  and (state__view__current_page__section = 'Support' 
		or state__view__current_page__section = 'support'
		or state__view__current_page__section = 'My Account' 
		or state__view__current_page__section = 'Login')

group by
`date`,
case
	when visit__account__account_number is not null then prod.aes_decrypt(visit__account__account_number)
	else 'Unauthenticated'
end,
visit__device__uuid,
case
	when state__view__current_page__name like 'my-account.payment%' then 'my-account.payment'
	when state__view__current_page__name like 'billing-and-transactions%' then 'billing-and-transactions'
	when state__view__current_page__name like 'Cablebox-Reset%' then 'Cablebox-Reset'
	when state__view__current_page__name like 'my-account.claim-gift-coupon%' then 'my-account.claim-gift-coupon'
	when state__view__current_page__name like 'signaling-cablebox%' then 'signaling-cablebox'
	when state__view__current_page__name like 'Test-Your-Internet-Connection%' then 'Test-Your-Internet-Connection'
	when state__view__current_page__name like 'Test-Your-Phone-Connection%' then 'Test-Your-Phone-Connection'
	when state__view__current_page__name like '%STVA' then split(state__view__current_page__name, '_')[0]
	else state__view__current_page__name
end,
state__view__current_page__section


--
-- Monthly Apr - Oct

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
  state__view__current_page__section

--
-- device ID / account # pairing
SELECT
  prod.aes_decrypt(visit__account__account_number) as account_number,
  visit__device__uuid as device_id,
  count(distinct visit__visit_id) as unique_session_counts

FROM prod.net_events
WHERE `date` >= '2016-01-01'
  AND prod.aes_decrypt(visit__account__account_number) is not null

GROUP BY
  visit__account__account_number,
  visit__device__uuid

