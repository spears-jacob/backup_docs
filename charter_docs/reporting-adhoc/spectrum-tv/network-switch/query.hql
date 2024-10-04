CREATE TABLE IF NOT EXISTS test.venona_concurrent_agg(
device_id string,
mso string,
network string,
init_network string,
xgroup bigint,
min_time bigint,
next_min bigint);

SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;

WITH DATA AS
  (
  SELECT
    visit__device__uuid AS device_id,
    received__timestamp AS received_timestamp,
    CASE
      WHEN visit__account__details__mso = 'NONECAPTURED' THEN 'L-TWC'
      WHEN visit__account__details__mso = 'CHARTER' THEN 'L-CHTR'
      WHEN visit__account__details__mso = 'BH' THEN 'L-BHN'
      WHEN visit__account__details__mso = 'TWC' THEN 'L-TWC'
      ELSE visit__account__details__mso
    END AS mso,
    visit__connection__network_status AS network
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
INSERT INTO TABLE test.venona_concurrent_agg
SELECT
  device_id,
  mso,
  network,
  init_network,
  xgroup,
  MIN(received_timestamp) AS min_time,
  LEAD(MIN(received_timestamp)) OVER (PARTITION BY device_id ORDER BY MIN(received_timestamp)) AS next_min
FROM
  (
  SELECT
    device_id,
    mso,
    network,
    received_timestamp,
    init_network,
    SUM(IF(network<>prev_network OR ISNULL(prev_network),1,0)) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS xgroup
  FROM
    (
    SELECT
      device_id,
      mso,
      network,
      received_timestamp,
      LAG(network) OVER (PARTITION BY device_id ORDER BY received_timestamp) AS prev_network,
      FIRST_VALUE(network) OVER (PARTITION BY device_id ORDER BY received_timestamp) AS init_network
    FROM DATA
    ) a
  ) b
GROUP BY
  device_id,
  mso,
  network,
  init_network,
  xgroup;

SELECT
  mso,
  init_network,
  SIZE(COLLECT_SET(device_id)) as unique_devices,
  AVG(switch_to_ih) as avg_switch_to_ih,
  AVG(switch_to_ooh) AS avg_switch_to_ooh,
  AVG(ended_ooh) AS avg_ended_ooh,
  avg(time_ooh) as time_ooh
FROM
  (
  SELECT
    device_id,
    mso,
    init_network,
    SUM(IF(network = 'offNet' AND ISNOTNULL(next_min),1,0)) as switch_to_ih,
    SUM(IF(network = 'onNet' AND ISNOTNULL(next_min),1,0)) as switch_to_ooh,
    SUM(IF(network = 'offNet' AND ISNULL(next_min),1,0)) as ended_ooh,
    sum(if(network = 'offNet' AND ISNOTNULL(next_min), next_min - min_time, 0)) as time_ooh
  FROM test.venona_concurrent_agg
  GROUP BY
    device_id,
    mso,
    init_network
  HAVING
    (init_network = 'onNet' AND COUNT(*) > 2) OR (init_network = 'offNet' AND COUNT(*) > 1)
  ) a
GROUP BY
  mso,
  init_network;
