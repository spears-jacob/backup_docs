      FROM asp_privacysite_metric_agg
      WHERE (partition_date_utc >= ("${hiveconf:START_DATE}") AND partition_date_utc < ("${hiveconf:END_DATE}"))
      GROUP BY
        app_section,
        user_role,
        message_context,
        {unit_identifier}
      ) sumfirst
    GROUP BY
    app_section,
    user_role,
    message_context,
    unit_type,
    unit_identifier
  ) sets
  GROUP BY
    '${hiveconf:partition_date_utc}',
    app_section,
    user_role,
    message_context,
    unit_type
  ) mapit
LATERAL VIEW EXPLODE(tmp_map) explode_table AS metric_name, metric_value;
