
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

    FROM
      ( --start of innermost query
      SELECT
        mso,
        application_type,
        device_type,
        connection_type,
        network_status,
        playback_type,
        cust_type,
        application_group_type,
        app_version,
        logged_in AS logged_in_status,
        application_name,
        'All OS Names' AS os_name,
        operating_system AS os_version,
        browser_name,
        browser_version,
        form_factor,
        '{unit_type}' AS unit_type,
        {unit_identifier} AS unit_identifier,

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
