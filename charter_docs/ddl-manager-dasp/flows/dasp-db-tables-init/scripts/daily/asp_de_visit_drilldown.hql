CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_de_visit_drilldown(
  visit__account__details__mso string,
  visit__application_details__application_name string,
  state__view__current_page__user_journey array<string>,
  state__view__current_page__user_sub_journey array<string>,
  state__view__current_page__page_name string,
  visit__visit_id string,
  partition_date_utc string
)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY");
