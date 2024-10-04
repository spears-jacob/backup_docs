--- XGANALYTIC-9190
--- This extract shows account numbers who viewed support content in the support site section, the page viewed and the search string. 
select
'L-CHTR' as legacy_company,
'Residential' as custommer_type,
'2017-10-15' as report_begin_date,
'2017-10-25' as report_end_date,
date(partition_date) as event_date,
prod.epoch_timestamp(message__timestamp*1000,'America/Denver') as event_timestamp,
prod.aes_decrypt(visit__account__account_number) as account_number,
CASE
WHEN state__view__current_page__name = 'search-results.spectrum' OR state__view__current_page__name = 'search-results.google' THEN 'Support Search'
ELse 'Support Page Article'
End as event_type,
state__view__current_page__name as page_name,
state__view__current_page__search_text as search
from prod.net_events 
where partition_date between '2017-10-15' and '2017-10-25'
and message__category = 'Page View'
and (state__view__current_page__section = 'Support' or state__view__current_page__section = 'support')
and prod.aes_decrypt(visit__account__account_number) is not null
group by
'L-CHTR',
'Residential',
'2017-10-15',
'2017-10-25',
date(partition_date),
prod.epoch_timestamp(message__timestamp*1000,'America/Denver'),
prod.aes_decrypt(visit__account__account_number),
CASE
WHEN state__view__current_page__name = 'search-results.spectrum' OR state__view__current_page__name = 'search-results.google' THEN 'Support Search'
ELse 'Support Page Article'
End,
state__view__current_page__name,
state__view__current_page__search_text