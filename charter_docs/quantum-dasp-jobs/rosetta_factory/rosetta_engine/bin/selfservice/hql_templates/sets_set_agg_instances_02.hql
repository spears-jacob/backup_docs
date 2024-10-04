
    --------------------------------------------------------------------------------
    --------------------------------------------------------------------------------
    --------------------------------------------------------------------------------

    ) AS tmp_map
  FROM
    (
    SELECT
      '${hiveconf:label_date_denver}' AS label_date_denver,
      mso,
      application_type,
      application_group_type,
      app_version,
      device_type,
      connection_type,
      network_status,
      playback_type,
      cust_type,
      logged_in AS logged_in_status,
      application_name,
      'All OS Names' AS os_name,
      operating_system AS os_version,
      browser_name,
      browser_version,
      form_factor,

      --------------------------------------------------------------------------------
      --------------------------------------------------------------------------------
      --------------------------------------------------------------------------------
