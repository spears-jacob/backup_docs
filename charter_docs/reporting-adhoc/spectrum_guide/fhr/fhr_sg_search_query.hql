
SELECT year_month,
    sum(if(application__api__response_code NOT IN
        ('200','201','202','203','204','205','206','207','208','209','210','226'), 1, 0)) AS api_search_error_count,
    avg(application__api__response_time_in_ms) as average_search_response_time_in_ms
FROM
(
SELECT substring(prod.epoch_converter(message__timestamp, 'America/Denver'),0,7) as year_month,
        prod.epoch_converter(message__timestamp, 'America/Denver') AS partition_date,
        visit__device__uuid,
        application__api__response_code,
        application__api__response_time_in_ms
FROM prod.sg_p2_events_utc
WHERE application__api__response_code is not null
  AND application__api__response_code != ''
  AND application__api__response_time_in_ms is not null
  AND application__api__api_name = 'search'
  AND partition_date_utc >= '2017-03-01' ) events
JOIN
  ( SELECT run_date,
   account__mac_id_aes256,
   account__number_aes256,
   system__kma_desc,
   account__category,
   sg_deployed_type,
   sg_ACCOUNT_DEPLOYED_TIMESTAMP
  FROM prod_lkp.SG_PHASEII_DEPLOYED_MACS_ALL_HISTORY
  WHERE account__type='SUBSCRIBER'
  AND run_date >= '2017-03-01'
) deployed
ON deployed.run_date = events.partition_date
  AND events.visit__device__uuid = deployed.account__mac_id_aes256
GROUP BY year_month
