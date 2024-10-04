    FROM
      (
      SELECT
        app_section,
        user_role,
        message_context,
        '{unit_type}' AS unit_type,
        {unit_identifier} AS unit_identifier,
