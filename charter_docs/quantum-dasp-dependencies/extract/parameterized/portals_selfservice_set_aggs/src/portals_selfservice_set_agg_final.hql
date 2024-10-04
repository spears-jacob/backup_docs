USE ${env:ENVIRONMENT};

SELECT'
--------------------------------------------------------------------------------
------------------ *** Insert all units into final table *** -------------------
--------------------------------------------------------------------------------
'
;


INSERT OVERWRITE TABLE venona_set_agg_portals PARTITION (label_date_denver, grain)

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
  grouping_id,
  metric_name,
  metric_value,
  logged_in_status,
  application_name,
  os_name,
  os_version,
  browser_name,
  browser_version,
  form_factor,
  process_date_time_denver,
  process_identity,
  unit_type,
  label_date_denver,
  grain
  FROM ${env:TMP_db}.venona_set_agg_portals_stage_accounts_${env:execid}

  UNION ALL

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
    grouping_id,
    metric_name,
    metric_value,
    logged_in_status,
    application_name,
    os_name,
    os_version,
    browser_name,
    browser_version,
    form_factor,
    process_date_time_denver,
    process_identity,
    unit_type,
    label_date_denver,
    grain
    FROM ${env:TMP_db}.venona_set_agg_portals_stage_devices_${env:execid}

      UNION ALL

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
        grouping_id,
        metric_name,
        metric_value,
        logged_in_status,
        application_name,
        os_name,
        os_version,
        browser_name,
        browser_version,
        form_factor,
        process_date_time_denver,
        process_identity,
        unit_type,
        label_date_denver,
        grain
        FROM ${env:TMP_db}.venona_set_agg_portals_stage_instances_${env:execid}

          UNION ALL

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
            grouping_id,
            metric_name,
            metric_value,
            logged_in_status,
            application_name,
            os_name,
            os_version,
            browser_name,
            browser_version,
            form_factor,
            process_date_time_denver,
            process_identity,
            unit_type,
            label_date_denver,
            grain
            FROM ${env:TMP_db}.venona_set_agg_portals_stage_visits_${env:execid}
;


DROP TABLE IF EXISTS ${env:TMP_db}.venona_set_agg_portals_stage_accounts_${env:execid} PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.venona_set_agg_portals_stage_devices_${env:execid} PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.venona_set_agg_portals_stage_instances_${env:execid} PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.venona_set_agg_portals_stage_visits_${env:execid} PURGE;
