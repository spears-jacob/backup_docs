SELECT recording_type,
       keep_setting,
       offsetStart_setting_mins,
       offsetEnd_setting_mins,
       allowDuplicates_setting,
       RecordEpisodes_setting,
       COUNT(*) AS count_of
FROM
( SELECT  visit__device__uuid,
        prod.epoch_converter(message__timestamp, 'America/Denver') AS partition_date,
        message__name,
-- newOnly and allowDuplicates are settings for only Series, not episode.  The existence of this measurement allows us to see 
-- that the settings being edited are for series or episode.
        CASE WHEN operation__selected__view_element_name  LIKE '%newOnly%' THEN 'Series' ELSE 'Episode' END AS recording_type,
        operation__selected__view_element_name,
        REGEXP_EXTRACT(operation__selected__view_element_name, '.*keepUntil=(.*?)(;.*|$)',1) AS keep_setting,
-- units are stored in milliseconds we convert to mins
        CAST(REGEXP_EXTRACT(operation__selected__view_element_name, '.*offsetStart=(.*?)(;.*|$)',1) AS INT) / 60000 AS offsetStart_setting_mins,
        CAST(REGEXP_EXTRACT(operation__selected__view_element_name, '.*offsetEnd=(.*?)(;.*|$)',1)  AS INT) / 60000 AS offsetEnd_setting_mins,
        REGEXP_EXTRACT(operation__selected__view_element_name, '.*allowDuplicates=(.*?)(;.*|$)',1) AS allowDuplicates_setting,
        REGEXP_EXTRACT(operation__selected__view_element_name, '.*newOnly=(.*?)(;.*|$)',1) AS RecordEpisodes_setting,
        message__timestamp
FROM prod.sg_p2_events_utc
WHERE partition_date_utc BETWEEN '2017-02-01' AND '2017-05-03'
        AND message__name LIKE 'Save%'
-- This setting allows us to see if they are making a change to a DVR recording setting
       AND operation__selected__view_element_name LIKE '%keepUntil%') events

 JOIN

( SELECT run_date,
account__mac_id_aes256,
account__number_aes256,
system__kma_desc,
account__category,
sg_deployed_type
FROM prod_lkp.SG_PHASEII_DEPLOYED_MACS_ALL_HISTORY
WHERE account__type='SUBSCRIBER'
AND account__category = 'DVR'
AND run_date BETWEEN '2017-02-01' AND '2017-05-02'
) deployed
ON events.partition_date = deployed.run_date
AND events.visit__device__uuid = deployed.account__mac_id_aes256

GROUP BY recording_type, 
       keep_setting,
       offsetStart_setting_mins,
       offsetEnd_setting_mins,
       allowDuplicates_setting,
       RecordEpisodes_setting
