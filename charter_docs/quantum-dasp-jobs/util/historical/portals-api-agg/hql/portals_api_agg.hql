USE ${env:DASP_db};

set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager;
set hive.support.concurrency=false;
--set hive.tez.container.size=16000;
set hive.auto.convert.join=false;
set hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set mapreduce.input.fileinputformat.split.minsize=524288000;
set mapreduce.input.fileinputformat.split.maxsize=524288000;
set hive.optimize.sort.dynamic.partition = false;
set hive.exec.dynamic.partition.mode=nonstrict;
set orc.force.positional.evolution=true;


ADD JAR ${env:JARS_S3_LOCATION}/epoch-1.0-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION epoch_converter AS 'Epoch_To_Denver';

ADD JAR ${env:JARS_S3_LOCATION}/epochtotimestamp-1.0-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION epoch_timestamp AS 'Epoch_Timestamp';


INSERT OVERWRITE TABLE ${env:DASP_db}.asp_api_agg partition(denver_date)
SELECT
  grouping_id,
  CASE WHEN (grouping_id & 1024) = 0 THEN hour_denver ELSE 'All Hours' END AS hour_denver,
  CASE WHEN (grouping_id & 512) = 0 THEN minute_group ELSE 'All Minutes' END AS minute_group,
  CASE WHEN (grouping_id & 256) = 0 THEN application_name ELSE 'All Applications' END AS application_name,
  CASE WHEN (grouping_id & 128) = 0 THEN mso ELSE 'All MSOs' END AS mso,
  CASE WHEN (grouping_id & 8) = 0 THEN cust_type ELSE 'All Customer Types' END AS cust_type,
  CASE WHEN (grouping_id & 64) = 0 THEN api_category ELSE 'All Categories' END AS api_category,
  CASE WHEN (grouping_id & 32) = 0 THEN api_name ELSE 'All API' END AS api_name,
  CASE WHEN (grouping_id & 4) = 0 THEN stream_subtype ELSE 'All Stream Subtypes' END AS stream_subtype,
  CASE WHEN (grouping_id & 2) = 0 THEN current_page_name ELSE 'All Page Names' END AS current_page_name,
  metric_name,
  metric_value,
  CASE WHEN date_format(denver_date, 'E') = 'Wed' THEN denver_date
    WHEN date_format(denver_date, 'E') = 'Thu' THEN date_add(denver_date, 6)
    WHEN date_format(denver_date, 'E') = 'Fri' THEN date_add(denver_date, 5)
    WHEN date_format(denver_date, 'E') = 'Sat' THEN date_add(denver_date, 4)
    WHEN date_format(denver_date, 'E') = 'Sun' THEN date_add(denver_date, 3)
    WHEN date_format(denver_date, 'E') = 'Mon' THEN date_add(denver_date, 2)
    WHEN date_format(denver_date, 'E') = 'Tue' THEN date_add(denver_date, 1)
  ELSE denver_date END AS week_end,
  CONCAT(SUBSTR(denver_date, 1, 7), '-01') as month_start,
  CASE WHEN (grouping_id & 1) = 0 THEN technology_type ELSE 'All Tech Types' END AS technology_type,
  denver_date
FROM
  (
  SELECT
    grouping_id,
    hour_denver,
    minute_group,
    application_name,
    mso,
    api_category,
    api_name,
    MAP(
      'device_count', device_count,
      'success_count', success_count,
      'client_error_count', client_error_count,
      'server_error_count', server_error_count,
      'other_response_count', other_response_count,
      'event_count', event_count,
      'success_rate', success_count / event_count,
      'average_response_time_ms', average_response_time_ms,
      'median_response_time_ms', perc_response_time_ms[0],
      '95th_response_time_ms', perc_response_time_ms[1],
      '99th_response_time_ms', perc_response_time_ms[2]
    ) AS tmp_map,
    denver_date,
    cust_type,
    stream_subtype,
    current_page_name,
    technology_type
  FROM
    (
    SELECT
      CAST(grouping__id AS int) AS grouping_id,
      hour_denver,
      minute_group,
      application_name,
      mso,
      api_category,
      api_name,
      SIZE(COLLECT_SET(device_id)) AS device_count,
      SUM(IF(api_code BETWEEN 200 AND 299 OR service_result = 'success', 1,0)) AS success_count,
      SUM(IF(api_code BETWEEN 400 AND 499 AND service_result = 'failure', 1,0)) AS client_error_count,
      SUM(IF(api_code BETWEEN 500 AND 599 AND service_result = 'failure', 1,0)) AS server_error_count,
      SUM(IF((api_code NOT BETWEEN 200 AND 599
        OR api_code BETWEEN 300 AND 399
        OR ISNULL(api_code)) AND service_result = 'failure',1,0)) AS other_response_count,
       COUNT(*) AS event_count,
      AVG(response_ms) AS average_response_time_ms,
      PERCENTILE(response_ms, array(0.5, 0.95, 0.99)) AS perc_response_time_ms,
      denver_date,
      cust_type,
      stream_subtype,
      current_page_name,
      technology_type
    FROM
      (
      SELECT
        HOUR(epoch_timestamp(received__timestamp,'America/Denver')) as hour_denver,
        ((MINUTE(epoch_timestamp(received__timestamp,'America/Denver')) DIV 5) * 5) as minute_group,
        visit__application_details__application_name AS application_name,
        CASE
          WHEN  visit__account__details__mso IN ('CHARTER','CHTR','"CHTR"')       THEN 'L-CHTR'
          WHEN  visit__account__details__mso IN ('TWC','"TWC"', 'NONECAPTURED')   THEN 'L-TWC'
          WHEN  visit__account__details__mso IN ('BH','BHN','"BHN"')              THEN 'L-BHN'
          WHEN (visit__account__details__mso IN ('','unknown','"NONE"', 'NONE')
                OR visit__account__details__mso IS NULL )                         THEN 'MSO-MISSING'
          ELSE visit__account__details__mso
        END AS mso,
        case when visit__account__bi_account_info__stream_2 = 'SPP Choice' then 'Stream Choice'
              when visit__account__bi_account_info__stream_2 = 'TRUE' then 'Stream 2.0'
             when visit__account__bi_account_info__stream_2 is null then 'Video Sub'
             when visit__account__bi_account_info__stream_2 = 'bulkmdu' then 'bulkmdu'
          else "Spectrum University"
        end  AS cust_type,
        nvl(application__api__api_category, message__category) AS api_category,
        IF(SUBSTR(application__api__api_name,1,4) = 'http', 'BAD:httpDVR', application__api__api_name) AS api_name,
        application__api__response_code AS api_code,
        COALESCE(application__api__response_time_ms,0) AS response_ms,
        visit__account__details__customer_stream_subtype as stream_subtype,
        epoch_converter(received__timestamp,'America/Denver') as denver_date,
        visit__device__enc_uuid AS device_id,
        application__api__service_result AS service_result,
        state__view__current_page__page_name AS current_page_name,
        visit__application_details__technology_Type AS technology_type
      FROM ${env:ENVIRONMENT}.core_quantum_events_sspp
      WHERE partition_date_hour_utc >= '${hiveconf:START_DATE_TZ}'
        AND partition_date_hour_utc <  '${hiveconf:END_DATE_TZ}'
        AND visit__application_details__application_name IN ('SpecNet','SMB','MySpectrum','IDManagement','PrivacyMicrosite')
        AND message__name = 'apiCall'
        AND COALESCE(application__api__response_time_ms,0) > 0
        AND COALESCE(application__api__response_time_ms,0) < 60000
      ) base_data
    GROUP BY
      hour_denver,
      minute_group,
      application_name,
      mso,
      api_category,
      api_name,
      denver_date,
      cust_type,
      stream_subtype,
      current_page_name,
      technology_type
    GROUPING SETS (
--      (denver_date, api_category, api_name),
--      (denver_date, api_category),
--      (denver_date, mso, application_name, api_category, api_name),
--      (denver_date, mso, application_name, api_category),
--      (denver_date, mso, api_category, api_name),
--      (denver_date, mso, api_category),
--      (denver_date, application_name, api_category, api_name),
--      (denver_date, application_name, api_category),
--      (denver_date, hour_denver, api_category, api_name),
--      (denver_date, hour_denver, api_category),
--      (denver_date, hour_denver, minute_group, api_category, api_name),
--      (denver_date, hour_denver, minute_group, api_category),
--      (denver_date, cust_type, api_category, api_name),
--      (denver_date, cust_type, api_category),
--      (denver_date, cust_type, mso, application_name, api_category, api_name),
--      (denver_date, cust_type, mso, application_name, api_category),
--      (denver_date, cust_type, mso, api_category, api_name),
--      (denver_date, cust_type, mso, api_category),
--      (denver_date, cust_type, application_name, api_category, api_name),
--      (denver_date, cust_type, application_name, api_category),
--      (denver_date, cust_type, hour_denver, api_category, api_name),
--      (denver_date, cust_type, hour_denver, api_category),
--      (denver_date, cust_type, hour_denver, minute_group, api_category, api_name),
--      (denver_date, cust_type, hour_denver, minute_group, api_category),
--      (denver_date,api_category, api_name, application_name,stream_subtype) ,
--      (denver_date, api_category, api_name, current_page_name),
    (denver_date, api_category, api_name, application_name, current_page_name)
--      (denver_date, application_name, technology_type, api_category, api_name),
--      (denver_date, application_name, technology_type, api_category),
--      (denver_date, mso, application_name, technology_type, api_category, api_name),
--      (denver_date, mso, application_name, technology_type, api_category)
    )
    ) grouping_sets
  ) mapping_pivot
LATERAL VIEW EXPLODE(tmp_map) explode_table AS metric_name, metric_value;
