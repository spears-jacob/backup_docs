      FROM asp_privacysite_metric_agg
      WHERE (partition_date_utc >= ("${hiveconf:START_DATE}") AND partition_date_utc < ("${hiveconf:END_DATE}"))
      GROUP BY
        app_section,
        user_role,
        message_context
      ) sumfirst
    GROUP BY
      partition_date_utc,
      app_section,
      user_role,
      message_context
    ) sets
LATERAL VIEW EXPLODE(tmp_map) explode_table AS metric_name, metric_value;
