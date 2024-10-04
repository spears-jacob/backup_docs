    ) AS tmp_map
  FROM
    (
    SELECT
      '${hiveconf:partition_date_utc}' AS partition_date_utc,
      app_section,
      user_role,
      message_context,
