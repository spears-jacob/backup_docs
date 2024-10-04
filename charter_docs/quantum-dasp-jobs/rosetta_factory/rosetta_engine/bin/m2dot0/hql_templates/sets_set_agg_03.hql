
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

) AS call_map
FROM
  (
  SELECT
  partition_date_utc,
  unit_identifier,
  visit__application_details__application_name, -- 32
  visit__application_details__application_type, -- 16
  visit__application_details__app_version, -- 8
  agg_custom_visit__account__details__service_subscriptions, -- 4
  agg_custom_customer_group, -- 2
  agg_visit__account__configuration_factors, -- 1
  unit_type,
    CAST(grouping__id AS INT) AS grouping_id,
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
