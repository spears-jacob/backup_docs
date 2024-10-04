
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------


      FROM asp_m2dot0_metric_agg
      WHERE (partition_date_utc >= ("${hiveconf:START_DATE}") AND partition_date_utc < ("${hiveconf:END_DATE}"))
      GROUP BY
      partition_date_utc, -- 64
      visit__application_details__application_name, -- 32
      visit__application_details__application_type, -- 16
      visit__application_details__app_version, -- 8
      agg_custom_visit__account__details__service_subscriptions, -- 4
      agg_custom_customer_group, -- 2
      agg_visit__account__configuration_factors -- 1
      ) sumfirst
    GROUP BY
      'teehee', -- 256
      unit_identifier, -- 128
      partition_date_utc, -- 64
      visit__application_details__application_name, -- 32
      visit__application_details__application_type, -- 16
      visit__application_details__app_version, -- 8
      agg_custom_visit__account__details__service_subscriptions, -- 4
      agg_custom_customer_group, -- 2
      agg_visit__account__configuration_factors -- 1
      GROUPING SETS (
        (partition_date_utc, unit_identifier),
        (partition_date_utc, unit_identifier, visit__application_details__application_name, agg_custom_customer_group)
        --(partition_date_utc, unit_identifier, visit__application_details__application_name, CUSTOM_visit__application_details__app_version, agg_custom_customer_group),
        -- (partition_date_utc, unit_identifier, visit__application_details__application_name, CUSTOM_visit__application_details__app_version, agg_CUSTOM_visit__account__details__service_subscriptions_core, agg_CUSTOM_visit__account__details__service_subscriptions_mobile),
        -- (partition_date_utc, unit_identifier, visit__application_details__application_name, CUSTOM_visit__application_details__app_version, agg_custom_visit__account__configuration_factors, agg_custom_visit__account__details__service_subscriptions_mobile)
      )) sets
LATERAL VIEW EXPLODE(tmp_map) explode_table AS metric_name, metric_value;

--------------------------------------------------------------------------------
--------------------------------***** END *****---------------------------------
---------------------------------------------------------------------------------
