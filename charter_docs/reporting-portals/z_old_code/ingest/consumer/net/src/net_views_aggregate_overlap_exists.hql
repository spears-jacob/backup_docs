use ${env:ENVIRONMENT};

SET hive.execution.engine=mr;
SET hive.cbo.enable=true;
SET hive.compute.query.using.stats=true;
SET hive.exec.parallel=true;
SET hive.enforce.bucketing=true;
SET hive.stats.fetch.column.stats=true;
SET hive.stats.fetch.partition.stats=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.auto.convert.join=false;

SET NUMBER_OF_DAYS=2;

DROP TABLE IF EXISTS ${env:TMP_db}.net_views_existing;
CREATE TABLE ${env:TMP_db}.net_views_existing (view_id STRING);
INSERT INTO TABLE ${env:TMP_db}.net_views_existing
  SELECT DISTINCT net_event_ids.view_id
  FROM
    (
      SELECT
        net_events.state__content__stream__view_id AS view_id,
        message__timestamp AS message_timestamp,
        net_events.partition_date AS partition_date
      FROM
        net_events
      WHERE
      CAST(net_events.partition_date AS DATE) BETWEEN DATE_ADD(CAST('${hiveconf:LAST_DATE}' AS DATE), 1) AND DATE_ADD(CAST('${hiveconf:LAST_DATE}' AS DATE), ${hiveconf:NUMBER_OF_DAYS})
    ) net_event_ids
  JOIN
    net_views_agg ON (net_event_ids.view_id = net_views_agg.view_id)
  WHERE
    CAST(net_event_ids.partition_date AS DATE) BETWEEN DATE_ADD(CAST('${hiveconf:LAST_DATE}' AS DATE), 1) AND DATE_ADD(CAST('${hiveconf:LAST_DATE}' AS DATE), ${hiveconf:NUMBER_OF_DAYS}) AND
  ( (
        net_event_ids.message_timestamp IS NOT NULL
        AND
        UNIX_TIMESTAMP(REGEXP_REPLACE(net_event_ids.message_timestamp, 'T', ' ')) > UNIX_TIMESTAMP(net_views_agg.start_date_time)
      )
      OR
      net_event_ids.message_timestamp IS NULL
    );
