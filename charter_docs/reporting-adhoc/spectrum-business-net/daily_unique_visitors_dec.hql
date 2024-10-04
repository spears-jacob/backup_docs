-- L-CHTR SMB SpectrumBusiness.net
select
	customer as company,
	`date`,
	count(distinct visit__device__uuid) as daily_unique_visitors
from prod.sbnet_events
where `date` between '2016-12-01' and '2016-12-31'
	and visit__user__role LIKE 'Logged In%'
group by 
	customer,
	`date`


-- L-TWC SMB myaccount.timewarnercable.com
union all
select
	customer as company,
	`date`,
	CASE
		WHEN visit__account__account_number = 'HashedUA not set' or visit__account__account_number is null then 'Unauthenticated'
		ELSE 'authenticated'
	END as authentication,
	count(distinct visit__device__uuid) as daily_unique_visitors
from prod.twcmyacct_events
where `date` between '2016-12-01' and '2016-12-31'
group by 
	customer,
	`date`,
	CASE
		WHEN visit__account__account_number = 'HashedUA not set' or visit__account__account_number is null then 'Unauthenticated'
		ELSE 'authenticated'
	END

-- L-BHN SMB 
union all
select
	customer as company,
	`date`,
	count(distinct visit__device__uuid) as daily_unique_visitors
from prod.bhnmyservices_events
where `date` between '2016-12-01' and '2016-12-31'
group by 
	customer,
	`date`