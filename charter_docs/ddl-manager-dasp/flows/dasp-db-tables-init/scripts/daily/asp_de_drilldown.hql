CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_de_drilldown(
  state__view__current_page__page_name string,
  state__view__current_page__app_section string,
  visit__account__details__mso string,
  visit__application_details__application_name string,
  state__view__current_page__user_journey array<string>,
  state__view__current_page__user_sub_journey array<string>,
  visits bigint,
  visits_with_call bigint
) PARTITIONED BY (
partition_date_utc string
)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY");
