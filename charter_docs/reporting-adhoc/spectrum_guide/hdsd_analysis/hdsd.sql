SELECT month_of,
    stream,
    account__category,
    sg_deployed_type,
    SIZE(COLLECT_SET(account__mac_id_aes256)) AS stb_count,
    SIZE(COLLECT_SET(account__number_aes256)) AS hh_count

FROM
(

SELECT 
    events.month_of,
    stream,
    deployed.*
FROM

( SELECT month_of,
visit__device__uuid,
MAX(stream) AS stream
FROM

( SELECT partition_date,
LAST_DAY(partition_date) AS month_of,
visit__device__uuid,
MAX(state__content__details__hd) AS stream


FROM prod.sg_p2_events
WHERE partition_date >= '2017-01-01'
GROUP BY partition_date,
visit__device__uuid  ) preag
GROUP BY month_of,
        visit__device__uuid ) events


JOIN

( SELECT run_date,
    account__mac_id_aes256,
    account__number_aes256,
    system__kma_desc,
    account__category,
    sg_deployed_type
FROM PROD_LKP.SG_PHASEII_DEPLOYED_MACS_ALL_HISTORY
WHERE account__type='SUBSCRIBER'
    AND run_date >= '2017-01-01') deployed

ON events.month_of = deployed.run_date
AND events.visit__device__uuid = deployed.account__mac_id_aes256
) c
GROUP BY month_of,
    stream,
    account__category,
    sg_deployed_type
