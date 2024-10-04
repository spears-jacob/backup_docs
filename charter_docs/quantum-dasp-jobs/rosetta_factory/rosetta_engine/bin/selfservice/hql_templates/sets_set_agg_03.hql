
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

) AS call_map
  FROM
    (
    SELECT
      unit_type,
      unit_identifier,
      mso,
      application_type,
      device_type,
      connection_type,
      network_status,
      playback_type,
      cust_type,
      application_group_type,
      app_version,
      logged_in_status,
      application_name,
      os_name,
      os_version,
      browser_name,
      browser_version,
      form_factor,
      CAST(grouping__id AS INT) AS grouping_id,

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
