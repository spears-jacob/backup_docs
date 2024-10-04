
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

    FROM
      (
      SELECT
        page_name,
        app_section,
        user_role,
        device_id,
        visit_id,
        application_type,
        device_type,
        app_version,
        logged_in,
        os_name,
        operating_system,
        browser_name,
        browser_version,
        browser_size_breakpoint,
        form_factor,
        referrer_link,
        application_name,
        '{unit_type}' AS unit_type,
        {unit_identifier} AS unit_identifier,

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
