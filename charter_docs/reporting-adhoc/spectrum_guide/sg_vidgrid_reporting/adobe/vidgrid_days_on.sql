CREATE TABLE TEST.vidgrid_days_on AS

SELECT month_of,
  day_count,
  SIZE(COLLECT_SET(account__mac_id_aes256)) AS stb_count


FROM
(
SELECT account__mac_id_aes256,
  month_of,
  SIZE(COLLECT_SET(partition_date)) AS day_count

FROM
( SELECT partition_date,
    LAST_DAY(partition_date) AS month_of,
        visit__device__uuid,
        COLLECT_SET(state__view__current_page__settings['VidGrid State']) as vidgrid_set
        FROM prod.sg_p2_events
WHERE partition_date >= '2017-01-01'
AND state__view__current_page__settings['VidGrid State'] IN ('In View', 'Not In View')
GROUP BY partition_date,
        visit__device__uuid
) events

JOIN
(
 SELECT run_date,
   account__mac_id_aes256,
   account__number_aes256,
   system__kma_desc,
   account__category,
   sg_deployed_type,
   sg_ACCOUNT_DEPLOYED_TIMESTAMP
  FROM LKP.SG_PHASEII_DEPLOYED_MACS_ALL_HISTORY
  WHERE account__type='SUBSCRIBER'
  AND run_date >= '2017-01-01'
) deployed

ON deployed.run_date = events.partition_date
AND events.visit__device__uuid = deployed.account__mac_id_aes256
-- Given that the set vidgrid_set contains 'In View' and has size one, then that household only had vidgrid views.
WHERE SIZE(vidgrid_set) = 1
AND array_contains(vidgrid_set, 'In View')
GROUP BY account__mac_id_aes256,
  month_of
  ) d
GROUP BY month_of,
  day_count;
