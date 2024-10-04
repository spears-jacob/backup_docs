
    --------------------------------------------------------------------------------
    --------------------------------------------------------------------------------
    --------------------------------------------------------------------------------

    ) AS tmp_map
  FROM
    (
    SELECT
      partition_date_utc, -- 64
      visit__application_details__application_name, -- 32
      visit__application_details__application_type, -- 16
      visit__application_details__app_version, -- 8
      agg_custom_visit__account__details__service_subscriptions, -- 4
      agg_custom_customer_group, -- 2
      agg_visit__account__configuration_factors, -- 1
      'Instances' as unit_identifier,
      --------------------------------------------------------------------------------
      --------------------------------------------------------------------------------
      --------------------------------------------------------------------------------
