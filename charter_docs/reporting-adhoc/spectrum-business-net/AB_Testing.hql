-- DAILY
select
	partition_date,
	CASE
		WHEN message__feature__name = '_Prod_POC_login_page(8046130161):Original(8024400873):treatment' THEN 'Original'
		WHEN message__feature__name = '_Prod_POC_login_page(8046130161):Words_Only(8014012207):treatment' THEN 'Words Only'
		WHEN message__feature__name = '_Prod_POC_login_page(8046130161):Words_and_Numbers(8025380929):treatment' THEN 'Words and Numbers'
		ELSE message__feature__name
	END as AB_Testing_Group,
	sum(if(message__name = 'OptimizelyLayerDecision',1,0)) as optimizely_events,
	sum(if(message__name = 'CB Products',1,0)) as conversions

from prod.sbnet_events
where partition_date >= '2016-12-21'
	--and state__view__current_page__page_name = 'Login'

group by 
	partition_date,
	CASE
		WHEN message__feature__name = '_Prod_POC_login_page(8046130161):Original(8024400873):treatment' THEN 'Original'
		WHEN message__feature__name = '_Prod_POC_login_page(8046130161):Words_Only(8014012207):treatment' THEN 'Words Only'
		WHEN message__feature__name = '_Prod_POC_login_page(8046130161):Words_and_Numbers(8025380929):treatment' THEN 'Words and Numbers'
		ELSE message__feature__name
	END

-- Split by visit id etc.
select
	partition_date,
	CASE
		WHEN message__feature__name = '_Prod_POC_login_page(8046130161):Original(8024400873):treatment' THEN 'Original'
		WHEN message__feature__name = '_Prod_POC_login_page(8046130161):Words_Only(8014012207):treatment' THEN 'Words Only'
		WHEN message__feature__name = '_Prod_POC_login_page(8046130161):Words_and_Numbers(8025380929):treatment' THEN 'Words and Numbers'
		ELSE message__feature__name
	END as AB_Testing_Group,
	message__name as event,
	state__view__current_page__page_name as page_name,
	visit__user__role as status,
	size(collect_set(visit__visit_id)) as visit_counts,
	size(collect_set(visit__device__uuid)) as visitor_counts,
	count(*) as total_counts

from prod.sbnet_events
where partition_date >= '2016-12-21'
	and (message__name = 'OptimizelyLayerDecision' or message__name = 'CB Products')

group by 
	partition_date,
	CASE
		WHEN message__feature__name = '_Prod_POC_login_page(8046130161):Original(8024400873):treatment' THEN 'Original'
		WHEN message__feature__name = '_Prod_POC_login_page(8046130161):Words_Only(8014012207):treatment' THEN 'Words Only'
		WHEN message__feature__name = '_Prod_POC_login_page(8046130161):Words_and_Numbers(8025380929):treatment' THEN 'Words and Numbers'
		ELSE message__feature__name
	END,
	message__name,
	state__view__current_page__page_name,
	visit__user__role

---------------------------------------------------------------------------------------------------------------------------------------
-- DAILY RAW
select
	partition_date,
	CASE
		WHEN evar21 = '_Prod_POC_login_page(8046130161):Original(8024400873):treatment' THEN 'Original'
		WHEN evar21 = '_Prod_POC_login_page(8046130161):Words_Only(8014012207):treatment' THEN 'Words Only'
		WHEN evar21 = '_Prod_POC_login_page(8046130161):Words_and_Numbers(8025380929):treatment' THEN 'Words and Numbers'
		ELSE evar21
	END as AB_Testing_Group,
	CASE
		WHEN post_evar21 = '_Prod_POC_login_page(8046130161):Original(8024400873):treatment' THEN 'Original'
		WHEN post_evar21 = '_Prod_POC_login_page(8046130161):Words_Only(8014012207):treatment' THEN 'Words Only'
		WHEN post_evar21 = '_Prod_POC_login_page(8046130161):Words_and_Numbers(8025380929):treatment' THEN 'Words and Numbers'
		ELSE post_evar21
	END as post_AB_Testing_Group,
	sum(if(page_event_var2 = 'OptimizelyLayerDecision',1,0)) as optimizely_events,
	sum(if(post_page_event_var2 = 'OptimizelyLayerDecision',1,0)) as post_optimizely_events,
	sum(if(page_event_var2 = 'CB Products',1,0)) as conversions,
	sum(if(post_page_event_var2 = 'CB Products',1,0)) as post_conversions

from prod.sbnet_raw_history
where partition_date >= '2016-12-21'

group by
	partition_date,
	CASE
		WHEN evar21 = '_Prod_POC_login_page(8046130161):Original(8024400873):treatment' THEN 'Original'
		WHEN evar21 = '_Prod_POC_login_page(8046130161):Words_Only(8014012207):treatment' THEN 'Words Only'
		WHEN evar21 = '_Prod_POC_login_page(8046130161):Words_and_Numbers(8025380929):treatment' THEN 'Words and Numbers'
		ELSE evar21
	END,
	CASE
		WHEN post_evar21 = '_Prod_POC_login_page(8046130161):Original(8024400873):treatment' THEN 'Original'
		WHEN post_evar21 = '_Prod_POC_login_page(8046130161):Words_Only(8014012207):treatment' THEN 'Words Only'
		WHEN post_evar21 = '_Prod_POC_login_page(8046130161):Words_and_Numbers(8025380929):treatment' THEN 'Words and Numbers'
		ELSE post_evar21
	END

---------------------------------------------------------------------------------------------------------------------------------------

-- TOTAL
select
	CASE
		WHEN message__feature__name = '_Prod_POC_login_page(8046130161):Original(8024400873):treatment' THEN 'Original'
		WHEN message__feature__name = '_Prod_POC_login_page(8046130161):Words_Only(8014012207):treatment' THEN 'Words Only'
		WHEN message__feature__name = '_Prod_POC_login_page(8046130161):Words_and_Numbers(8025380929):treatment' THEN 'Words and Numbers'
		ELSE message__feature__name
	END as AB_Testing_Variable,
	message__name as event,
	count(distinct visit__visit_id) as unique_visits,
	count(distinct visit__device__uuid) as unique_visitors,
	sum(if(message__category = 'Page View',1,0)) as page_views,
	sum(if(message__category = 'Custom Link',1,0)) as link_clicks,
	count(*) as instances

from prod.sbnet_events
where partition_date BETWEEN '2017-01-01' AND '2017-01-26'
	and state__view__current_page__page_name = 'Login'

group by 
	CASE
		WHEN message__feature__name = '_Prod_POC_login_page(8046130161):Original(8024400873):treatment' THEN 'Original'
		WHEN message__feature__name = '_Prod_POC_login_page(8046130161):Words_Only(8014012207):treatment' THEN 'Words Only'
		WHEN message__feature__name = '_Prod_POC_login_page(8046130161):Words_and_Numbers(8025380929):treatment' THEN 'Words and Numbers'
		ELSE message__feature__name
	END,
	message__name

---------------------------------------------------------------------------------------------------------------------------------------

-- Daily - Ids ungrouped
select
	partition_date,
        CASE
		WHEN message__feature__name = '_Prod_POC_login_page(8046130161):Original(8024400873):treatment' THEN 'Original'
		WHEN message__feature__name = '_Prod_POC_login_page(8046130161):Words_Only(8014012207):treatment' THEN 'Words Only'
		WHEN message__feature__name = '_Prod_POC_login_page(8046130161):Words_and_Numbers(8025380929):treatment' THEN 'Words and Numbers'
		ELSE message__feature__name
	END as AB_Testing_Variable,
	message__name as link_name,
	state__view__current_page__page_name as page_name,
	visit__visit_id as visit_id,
	visit__device__uuid as device_id,
	sum(if(message__category = 'Page View',1,0)) as page_views,
	sum(if(message__category = 'Custom Link',1,0)) as link_clicks,
	count(*) as instances

from prod.sbnet_events
where partition_date BETWEEN '2016-12-21' AND '2017-01-26'
	and (state__view__current_page__page_name = 'Login'
		or message__name like 'CB%'
		or message__name like '%Optimizely%')

group by 
	partition_date,
        CASE
		WHEN message__feature__name = '_Prod_POC_login_page(8046130161):Original(8024400873):treatment' THEN 'Original'
		WHEN message__feature__name = '_Prod_POC_login_page(8046130161):Words_Only(8014012207):treatment' THEN 'Words Only'
		WHEN message__feature__name = '_Prod_POC_login_page(8046130161):Words_and_Numbers(8025380929):treatment' THEN 'Words and Numbers'
		ELSE message__feature__name
	END,
	message__name,
	state__view__current_page__page_name,
	visit__visit_id,
	visit__device__uuid