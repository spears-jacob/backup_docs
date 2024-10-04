
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------


      FROM asp_idm_metric_agg
      WHERE (denver_date >= ("${env:START_DATE}") AND denver_date < ("${env:END_DATE}"))
      GROUP BY
        page_name,
        application_type,
        app_section,
        app_version,
        logged_in,
        application_name,
        os_name,
        operating_system,
        browser_name,
        browser_version,
        browser_size_breakpoint,
        user_role,
        device_id,
        visit_id,
        device_type,
        form_factor,
        referrer_link,
        {unit_identifier},
        '{unit_type}'
      ) sumfirst
    GROUP BY
      unit_identifier,
      page_name,
      application_type,
      app_section,
      app_version,
      logged_in,
      application_name,
      os_name,
      operating_system,
      browser_name,
      browser_version,
      browser_size_breakpoint,
      user_role,
      device_id,
      visit_id,
      device_type,
      form_factor,
      referrer_link,
      unit_type
GROUPING SETS (
    (unit_identifier),
    (unit_identifier, referrer_link, page_name),
    (unit_identifier, referrer_link, browser_name, page_name),
    (unit_identifier, referrer_link, browser_name, app_section, page_name),
    (unit_identifier, referrer_link, device_type, page_name),
    (unit_identifier, referrer_link, browser_name, browser_size_breakpoint, page_name),
    (unit_identifier, referrer_link, app_section, browser_size_breakpoint, page_name),
    (unit_identifier, referrer_link, browser_name, page_name),
    (unit_identifier, page_name),
    (unit_identifier, browser_name, page_name),
    (unit_identifier, browser_name, app_section, page_name),
    (unit_identifier, device_type, page_name),
    (unit_identifier, browser_name, browser_size_breakpoint, page_name),
    (unit_identifier, app_section, browser_size_breakpoint, page_name),
    (unit_identifier, browser_name, page_name))
  ) sets
  GROUP BY
    '${env:label_date_denver}',
    page_name,
    application_type,
    app_section,
    app_version,
    logged_in,
    application_name,
    os_name,
    operating_system,
    browser_name,
    browser_version,
    browser_size_breakpoint,
    user_role,
    device_id,
    visit_id,
    device_type,
    form_factor,
    referrer_link,
    grouping_id,
    unit_type
  ) mapit
LATERAL VIEW EXPLODE(tmp_map) explode_table AS metric_name, metric_value;

--------------------------------------------------------------------------------
--------------------------------***** END *****---------------------------------
--------------------------------------------------------------------------------
