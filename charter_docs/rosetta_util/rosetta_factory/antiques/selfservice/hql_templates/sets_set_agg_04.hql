
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------


      FROM quantum_metric_agg_portals
      WHERE (denver_date >= ("${hiveconf:START_DATE}") AND denver_date < ("${hiveconf:END_DATE}"))
      GROUP BY
        mso,
        application_type,
        device_type,
        connection_type,
        network_status,
        playback_type,
        cust_type,
        application_group_type,
        app_version,
        logged_in,
        application_name,
        'All OS Names',
        operating_system,
        browser_name,
        browser_version,
        form_factor,
        {unit_identifier},
        '{unit_type}'
      ) sumfirst
    GROUP BY
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
      unit_type
    GROUPING SETS (
      (unit_identifier),
      (unit_identifier, network_status),
      (unit_identifier, network_status, browser_name),
      (unit_identifier, network_status, browser_name, form_factor),
      (unit_identifier, network_status, form_factor),
      (unit_identifier, os_version),
      (unit_identifier, browser_name),
      (unit_identifier, browser_name, form_factor),
      (unit_identifier, browser_name, browser_version),
      (unit_identifier, form_factor),
      (unit_identifier, mso, application_name),
      (unit_identifier, mso, application_name, network_status),
      (unit_identifier, mso, application_name, network_status, app_version),
      (unit_identifier, mso, application_name, network_status, app_version, browser_name),
      (unit_identifier, mso, application_name, network_status, app_version, form_factor),
      (unit_identifier, mso, application_name, network_status, browser_name),
      (unit_identifier, mso, application_name, network_status, browser_name, form_factor),
      (unit_identifier, mso, application_name, network_status, form_factor),
      (unit_identifier, mso, application_name, app_version),
      (unit_identifier, mso, application_name, app_version, browser_name),
      (unit_identifier, mso, application_name, app_version, browser_name, form_factor),
      (unit_identifier, mso, application_name, app_version, form_factor),
      (unit_identifier, mso, application_name, browser_name),
      (unit_identifier, mso, application_name, browser_name, form_factor),
      (unit_identifier, mso, application_name, form_factor),
      (unit_identifier, application_name),
      (unit_identifier, application_name, network_status),
      (unit_identifier, application_name, network_status, app_version),
      (unit_identifier, application_name, network_status, app_version, browser_name),
      (unit_identifier, application_name, network_status, app_version, form_factor),
      (unit_identifier, application_name, network_status, browser_name),
      (unit_identifier, application_name, network_status, browser_name, form_factor),
      (unit_identifier, application_name, network_status, form_factor),
      (unit_identifier, application_name, app_version),
      (unit_identifier, application_name, app_version, browser_name),
      (unit_identifier, application_name, app_version, browser_name, form_factor),
      (unit_identifier, application_name, app_version, browser_name, browser_version),
      (unit_identifier, application_name, app_version, form_factor),
      (unit_identifier, application_name, os_version),
      (unit_identifier, application_name, browser_name),
      (unit_identifier, application_name, browser_name, form_factor),
      (unit_identifier, application_name, browser_name, browser_version),
      (unit_identifier, application_name, form_factor),
      (unit_identifier, application_name, device_type, mso, app_version),
      (unit_identifier, application_name, device_type, mso),
      (unit_identifier, application_name, device_type, app_version),
      (unit_identifier, application_name, device_type))
      ) sets
  GROUP BY
    '${hiveconf:label_date_denver}',
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
    grouping_id,
    unit_type
  ) mapit
LATERAL VIEW EXPLODE(tmp_map) explode_table AS metric_name, metric_value;

--------------------------------------------------------------------------------
--------------------------------***** END *****---------------------------------
--------------------------------------------------------------------------------
