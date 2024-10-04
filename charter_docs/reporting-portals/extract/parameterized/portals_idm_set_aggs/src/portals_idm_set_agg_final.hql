USE ${env:ENVIRONMENT};

SELECT'
--------------------------------------------------------------------------------
------------------ *** Insert all units into final table *** -------------------
--------------------------------------------------------------------------------
'
;


INSERT OVERWRITE TABLE asp_idm_set_agg PARTITION (label_date_denver, grain)

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
  application_name,
  os_name,
  operating_system,
  browser_name,
  browser_version,
  browser_size_breakpoint,
  form_factor,
  referrer_link,
  grouping_id,
  metric_name,
  metric_value,
  process_date_time_denver,
  process_identity,
  unit_type,
  grain,
  label_date_denver
  FROM ${env:TMP_db}.venona_set_agg_idm_stage_accounts_${env:execid}

  UNION ALL

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
      application_name,
      os_name,
      operating_system,
      browser_name,
      browser_version,
      browser_size_breakpoint,
      form_factor,
      referrer_link,
      grouping_id,
      metric_name,
      metric_value,
      process_date_time_denver,
      process_identity,
      unit_type,
      grain,
      label_date_denver
    FROM ${env:TMP_db}.venona_set_agg_idm_stage_devices_${env:execid}

      UNION ALL

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
          application_name,
          os_name,
          operating_system,
          browser_name,
          browser_version,
          browser_size_breakpoint,
          form_factor,
          referrer_link,
          grouping_id,
          metric_name,
          metric_value,
          process_date_time_denver,
          process_identity,
          unit_type,
          grain,
          label_date_denver
        FROM ${env:TMP_db}.venona_set_agg_idm_stage_instances_${env:execid}

          UNION ALL

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
              application_name,
              os_name,
              operating_system,
              browser_name,
              browser_version,
              browser_size_breakpoint,
              form_factor,
              referrer_link,
              grouping_id,
              metric_name,
              metric_value,
              process_date_time_denver,
              process_identity,
              unit_type,
              grain,
              label_date_denver 
            FROM ${env:TMP_db}.venona_set_agg_idm_stage_visits_${env:execid}
;


DROP TABLE IF EXISTS ${env:TMP_db}.venona_set_agg_idm_stage_accounts_${env:execid} PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.venona_set_agg_idm_stage_devices_${env:execid} PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.venona_set_agg_idm_stage_instances_${env:execid} PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.venona_set_agg_idm_stage_visits_${env:execid} PURGE;
