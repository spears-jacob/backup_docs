SET hive.tez.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

-------------------------------------------------------------------------------

USE ${env:ENVIRONMENT};

INSERT INTO TABLE net_message_names_monthly 
SELECT 
  concat(year(partition_date),'-',lpad(month(partition_date),2,'0')) as year_month,
  message__name,
  message__category,
  state__view__current_page__name,
  state__view__current_page__section,
  operation__user_entry__type,
  state__content__stream__type,
  state__content__stream__content_format,
  visit__connection__network_status,
  visit__device__operating_system,
  visit__connection__type,
  visit__device__browser__name,
  count(distinct visit__visit_id) AS visit,
  count(*) AS `count`
FROM 
  net_events
WHERE 
  partition_date BETWEEN '${hiveconf:START_DATE}' AND '${hiveconf:END_DATE}'
GROUP BY
  concat(year(partition_date),'-',lpad(month(partition_date),2,'0')), 
  message__name,
  message__category,
  state__view__current_page__name,
  state__view__current_page__section,
  operation__user_entry__type,
  state__content__stream__type,
  state__content__stream__content_format,
  visit__connection__network_status,
  visit__device__operating_system,
  visit__connection__type,
  visit__device__browser__name;
  
