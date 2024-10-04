
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------


      FROM ${hiveconf:inputmetricaggtable}
      WHERE (denver_date >= ("${hiveconf:START_DATE}") AND denver_date < ("${hiveconf:END_DATE}"))
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
        logged_in,
        application_name,
        'All OS Names',
        operating_system,
        browser_name,
        browser_version,
        form_factor
      ) sumfirst
    GROUP BY
      label_date_denver,
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
      form_factor
    GROUPING SETS (
      (label_date_denver),
      (label_date_denver, network_status),
      (label_date_denver, network_status, browser_name),
      (label_date_denver, network_status, browser_name, form_factor),
      (label_date_denver, network_status, form_factor),
      (label_date_denver, os_version),
      (label_date_denver, browser_name),
      (label_date_denver, browser_name, form_factor),
      (label_date_denver, browser_name, browser_version),
      (label_date_denver, form_factor),
      (label_date_denver, mso, application_name),
      (label_date_denver, mso, application_name, network_status),
      (label_date_denver, mso, application_name, network_status, app_version),
      (label_date_denver, mso, application_name, network_status, app_version, browser_name),
      (label_date_denver, mso, application_name, network_status, app_version, form_factor),
      (label_date_denver, mso, application_name, network_status, browser_name),
      (label_date_denver, mso, application_name, network_status, browser_name, form_factor),
      (label_date_denver, mso, application_name, network_status, form_factor),
      (label_date_denver, mso, application_name, app_version),
      (label_date_denver, mso, application_name, app_version, browser_name),
      (label_date_denver, mso, application_name, app_version, browser_name, form_factor),
      (label_date_denver, mso, application_name, app_version, form_factor),
      (label_date_denver, mso, application_name, browser_name),
      (label_date_denver, mso, application_name, browser_name, form_factor),
      (label_date_denver, mso, application_name, form_factor),
      (label_date_denver, application_name),
      (label_date_denver, application_name, network_status),
      (label_date_denver, application_name, network_status, app_version),
      (label_date_denver, application_name, network_status, app_version, browser_name),
      (label_date_denver, application_name, network_status, app_version, form_factor),
      (label_date_denver, application_name, network_status, browser_name),
      (label_date_denver, application_name, network_status, browser_name, form_factor),
      (label_date_denver, application_name, network_status, form_factor),
      (label_date_denver, application_name, app_version),
      (label_date_denver, application_name, app_version, browser_name),
      (label_date_denver, application_name, app_version, browser_name, form_factor),
      (label_date_denver, application_name, app_version, browser_name, browser_version),
      (label_date_denver, application_name, app_version, form_factor),
      (label_date_denver, application_name, os_version),
      (label_date_denver, application_name, browser_name),
      (label_date_denver, application_name, browser_name, form_factor),
      (label_date_denver, application_name, browser_name, browser_version),
      (label_date_denver, application_name, form_factor),
      (label_date_denver, application_name, device_type, mso, app_version),
      (label_date_denver, application_name, device_type, mso),
      (label_date_denver, application_name, device_type, app_version),
      (label_date_denver, application_name, device_type))
      ) sets
LATERAL VIEW EXPLODE(tmp_map) explode_table AS metric_name, metric_value;

--------------------------------------------------------------------------------
--------------------------------***** END *****---------------------------------
-----------------------------------------------------------------------------
