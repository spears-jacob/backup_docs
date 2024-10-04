set mapreduce.input.fileinputformat.split.maxsize=5368709120;
set mapreduce.input.fileinputformat.split.minsize=5368709120;

---2/9 - 3/26 (45 Days)
WITH data AS (
  SELECT
    visit__device__uuid as device_id,
    received__timestamp as received_timestamp,
    CASE
      WHEN visit__account__details__mso = 'NONECAPTURED' THEN 'L-TWC'
      WHEN visit__account__details__mso = 'CHARTER' THEN 'L-CHTR'
      WHEN visit__account__details__mso = 'BH' THEN 'L-BHN'
      WHEN visit__account__details__mso = 'TWC' THEN 'L-TWC'
      ELSE visit__account__details__mso
    END AS mso,
    visit__connection__network_status as network
  FROM prod.venona_events
  WHERE
      ((partition_date = '2017-02-09' AND hour >= '07')
        OR (partition_date BETWEEN '2017-02-10' AND '2017-03-26')
        OR (partition_date = '2017-03-27' AND hour < '06'))
      AND (visit__application_details__application_type IN ('iOS', 'Android')
      AND message__name = 'loginStop'
      AND operation__success = TRUE
      AND ISNOTNULL(visit__account__account_number)
      AND ISNOTNULL(visit__device__uuid)
      AND ISNOTNULL(visit__account__details__mso)
      AND visit__account__details__mso <> ''
      AND TRIM(TRANSLATE(visit__device__uuid, '0', '')) <>'')
)
SELECT
  b.mso as mso,
  b.init_network as init_network,
  IF(b.num_network = 1, 'Stayed', 'Switched') as net_group,
  SIZE(COLLECT_SET(b.device_id)) AS number_devices
FROM
  (
  SELECT
    a.device_id,
    a.mso,
    a.init_network,
    SIZE(COLLECT_SET(network)) as num_network
  FROM
    (
    SELECT
      device_id,
      mso,
      network,
      FIRST_VALUE(network) OVER(PARTITION BY device_id ORDER BY received_timestamp) AS init_network
    FROM data
    ) a
  GROUP BY
    a.device_id,
    a.mso,
    a.init_network
  ) b
GROUP BY
  b.mso,
  b.init_network,
  IF(b.num_network = 1, 'Stayed', 'Switched');
