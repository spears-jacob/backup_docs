SELECT LAST_DAY(partition_date) AS month_of,
	search.message__name,
	COUNT(*) AS count_of_events
FROM
(
SELECT partition_date,
	visit__device__uuid,
	message__name 
FROM

(
SELECT partition_date,
	message__timestamp,
	message__name,
	visit__device__uuid,
	state__view__current_page__search_text,
	LEAD(state__view__current_page__search_text) 
		OVER (PARTITION BY visit__device__uuid ORDER BY message__timestamp) AS next_result 
FROM prod.sg_p2_events
WHERE partition_date BETWEEN '2017-01-01' AND '2017-02-28'
AND message__name IN ('Search Selected', 'Search Results', 'No Search Results')
AND state__view__current_page__search_text IS NOT NULL
ORDER BY partition_date,
	visit__device__uuid,
	message__timestamp ) search

WHERE next_result IS NULL ) search

JOIN

  (SELECT run_date,
          account__mac_id_aes256,
          account__number_aes256,
          system__kma_desc AS region
   FROM prod_lkp.SG_PHASEII_DEPLOYED_MACS_ALL_HISTORY
   WHERE account__type='SUBSCRIBER'
   	 AND customer__type = 'Residential'
     AND run_date BETWEEN '2017-01-01' AND '2017-02-28') deployed
ON search.visit__device__uuid = deployed.account__mac_id_aes256
AND search.partition_date = deployed.run_date

GROUP BY LAST_DAY(partition_date),
	search.message__name;
