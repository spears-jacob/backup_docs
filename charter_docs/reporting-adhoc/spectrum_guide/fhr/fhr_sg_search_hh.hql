
SELECT LAST_DAY(partition_date) AS month_of,
       SIZE(COLLECT_SET(account__number_aes256)) AS households
FROM
  (SELECT partition_date,
          visit__device__uuid AS MAC_ID
   FROM prod.sg_p2_events
   WHERE partition_date BETWEEN '2017-01-01' AND '2017-02-28'
     AND message__name = 'Search Screen') a
JOIN
  (SELECT run_date,
          account__mac_id_aes256,
          account__number_aes256,
          system__kma_desc AS region
   FROM prod_lkp.SG_PHASEII_DEPLOYED_MACS_ALL_HISTORY
   WHERE account__type='SUBSCRIBER'
         AND customer__type = 'Residential'
     AND run_date BETWEEN '2017-01-01' AND '2017-02-28') b 
ON a.MAC_ID = b.account__mac_id_aes256
AND a.partition_date = b.run_date
GROUP BY LAST_DAY(partition_date);
