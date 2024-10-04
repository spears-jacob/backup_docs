select
prod.aes_decrypt(visit__user__id) as username,
string(prod.aes_decrypt(visit__account__account_number)) as account_number,
`date`,
state__view__current_page__name as page_name,
state__view__current_page__section as page_section,
count(*) as counts

from prod.sbnet_events 
where `date` between '2016-01-01' and '2016-10-31'
	and message__category = 'Page View'
	and state__view__current_page__section = 'Support'
	and prod.aes_decrypt(visit__user__id) is not null
	and prod.aes_decrypt(visit__account__account_number) is not null

group by
visit__user__id,
visit__account__account_number,
`date`,
state__view__current_page__name,
state__view__current_page__section
