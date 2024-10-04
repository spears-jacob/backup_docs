CREATE TABLE AS test.sg_channel_favorites
--Disclaimer:  It is not always possible to see what channel the person is favoriting
SELECT  run_date,
        deployed.account__number_aes256,
        COUNT(*) AS count_of
FROM

( SELECT prod.epoch_converter(message__timestamp, 'America/Denver') as partition_date,
    visit__device__uuid,
    message__name,
    state__content__programmer__callsign,
    state__content__programmer__id,
    state__content__programmer__linear__channel_number
FROM prod.sg_p2_events_utc
WHERE partition_date_utc BETWEEN '2017-02-01' AND '2017-05-03'
--verified with front end engineering that this is the proper tagging
AND message__name ='Add to Favorites'
AND state__view__current_page__name != 'Error Screen: Unable to Save - Error: 6002') events

JOIN 
(
SELECT run_date,
account__mac_id_aes256,
account__number_aes256,
system__kma_desc,
account__category,
sg_deployed_type
FROM prod_lkp.SG_PHASEII_DEPLOYED_MACS_ALL_HISTORY
WHERE account__type='SUBSCRIBER'
AND run_date BETWEEN '2017-02-01' AND '2017-05-02'
) deployed 
ON events.partition_date = deployed.run_date
AND events.visit__device__uuid = deployed.account__mac_id_aes256

GROUP BY run_date, account__number_aes256;

--query to pull values for number of houesholds selecting favorites and households selecting more than 1 favorite.
SELECT COUNT(*) AS hhs_using_favorites,
       SUM(CASE WHEN count_of_favorites > 1 THEN 1 ELSE 0 END) AS hhs_having_more_than_one_favorite
       FROM (
       SELECT fav.account__number_aes256,
               SUM(count_of) AS count_of_favorites
               FROM test.sg_channel_favorites fav
               JOIN
               (SELECT account__number_aes256
               FROM prod_lkp.SG_PHASEII_DEPLOYED_MACS_ALL_HISTORY
               WHERE account__type='SUBSCRIBER'
               AND run_date = '2017-05-01'
               GROUP BY account__number_aes256) deployed 
               ON fav.account__number_aes256 = deployed.account__number_aes256
               GROUP BY fav.account__number_aes256 ) d

