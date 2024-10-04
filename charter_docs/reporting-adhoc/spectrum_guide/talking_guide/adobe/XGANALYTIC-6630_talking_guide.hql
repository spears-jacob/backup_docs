--See ticket XGANALYTIC-6630


--Query to see Trending Rates.
SELECT partition_date,
        system__kma_desc,
        equipment__model,
        SIZE(COLLECT_SET(account__number_aes256)) AS hh_count
        
FROM
( SELECT
visit__device__uuid,
prod.epoch_converter(message__timestamp, 'America/Denver') as partition_date
FROM prod.sg_p2_events_utc
WHERE partition_date_utc BETWEEN '2017-04-24' AND '2017-05-08'
AND application__api__api_name = 'tts'
GROUP BY visit__device__uuid,
prod.epoch_converter(message__timestamp, 'America/Denver')) events

JOIN

(
 SELECT run_date, 
   account__mac_id_aes256,
   account__number_aes256,
   system__kma_desc,
   account__category,
   equipment__model,
   sg_deployed_type
  FROM prod_lkp.SG_PHASEII_DEPLOYED_MACS_ALL_HISTORY
  WHERE account__type='SUBSCRIBER'
  AND run_date BETWEEN '2017-04-24' AND '2017-05-08'
  AND equipment__model LIKE '%DCX3220E%'

) deployed   
   ON events.visit__device__uuid = deployed.account__mac_id_aes256
   AND events.partition_date = deployed.run_date 
 
GROUP BY partition_date,
        system__kma_desc,
        equipment__model
        
        
        
--- Query to pull decrypted account numbers

SELECT prod.aes_decrypt256(account__number_aes256) as account_num,
    system__kma_desc,
    equipment__model

        
FROM
( SELECT
visit__device__uuid,
prod.epoch_converter(message__timestamp, 'America/Denver') as partition_date
FROM prod.sg_p2_events_utc
WHERE partition_date_utc BETWEEN '2017-04-24' AND '2017-05-08'
AND application__api__api_name = 'tts'
GROUP BY visit__device__uuid,
prod.epoch_converter(message__timestamp, 'America/Denver')) events

JOIN

(
 SELECT run_date, 
   account__mac_id_aes256,
   account__number_aes256,
   system__kma_desc,
   account__category,
   equipment__model,
   sg_deployed_type
  FROM prod_lkp.SG_PHASEII_DEPLOYED_MACS_ALL_HISTORY
  WHERE account__type='SUBSCRIBER'
  AND run_date BETWEEN '2017-04-24' AND '2017-05-08'
  AND equipment__model LIKE '%DCX3220E%'

) deployed   
   ON events.visit__device__uuid = deployed.account__mac_id_aes256
   AND events.partition_date = deployed.run_date 
 
 GROUP BY prod.aes_decrypt256(account__number_aes256),
    system__kma_desc,
    equipment__model

