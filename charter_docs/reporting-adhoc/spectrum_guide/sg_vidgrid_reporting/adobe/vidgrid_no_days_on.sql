--Query is to calculate the number of households that used Sprectrum Guide in the Month,
--but had no day with only vidgrid views.
INSERT INTO TABLE TEST.VIDGRID_DAYS_ON
SELECT q.month_of,
        0 AS days_on,
       a.stb_count - q.stb_count
FROM
( SELECT month_of,
SUM(stb_count) AS stb_count

FROM TEST.vidgrid_days_on
WHERE month_of = '2017-01-31'
AND day_count != 0
GROUP BY month_of) q

JOIN

(
SELECT SIZE(COLLECT_SET(account__mac_id_aes256)) AS stb_count,
  month_of

FROM
( SELECT partition_date,
    LAST_DAY(partition_date) AS month_of,
        visit__device__uuid
      FROM prod.sg_p2_events
WHERE partition_date BETWEEN '2017-01-01' AND '2017-01-31'

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
  AND run_date BETWEEN '2017-01-01' AND '2017-01-31'
) deployed

ON deployed.run_date = events.partition_date
AND events.visit__device__uuid = deployed.account__mac_id_aes256

GROUP BY
  month_of ) a

 ON a.month_of = q.month_of;
