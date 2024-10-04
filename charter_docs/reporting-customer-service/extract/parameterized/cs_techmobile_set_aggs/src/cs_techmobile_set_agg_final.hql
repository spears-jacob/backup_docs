USE ${env:ENVIRONMENT};

SELECT'
--------------------------------------------------------------------------------
------------------ *** Insert all units into final table *** -------------------
--------------------------------------------------------------------------------
'
;


INSERT OVERWRITE TABLE cs_techmobile_set_agg PARTITION (label_date_denver, grain)

SELECT
raw_order_number,
visit_applicationdetails_appversion,
visit_location_region,
visit_location_regionname,
visit_technician_techid,
visit_technician_quadid,
visit_device_devicetype,
visit_device_model,
jobName,
message_timestamp,
receivedDate,
unit_type,
grouping_id,
metric_name,
metric_value,
grain,
partition_date_utc
FROM ${env:TMP_db}.cs_techmobile_set_agg_stage_accounts_${env:execid}

  UNION ALL

    SELECT
    raw_order_number,
    visit_applicationdetails_appversion,
    visit_location_region,
    visit_location_regionname,
    visit_technician_techid,
    visit_technician_quadid,
    visit_device_devicetype,
    visit_device_model,
    jobName,
    message_timestamp,
    receivedDate,
    unit_type,
    grouping_id,
    metric_name,
    metric_value,
    grain,
    partition_date_utc
    FROM ${env:TMP_db}.cs_techmobile_set_agg_stage_devices_${env:execid}

      UNION ALL

        SELECT
        raw_order_number,
        visit_applicationdetails_appversion,
        visit_location_region,
        visit_location_regionname,
        visit_technician_techid,
        visit_technician_quadid,
        visit_device_devicetype,
        visit_device_model,
        jobName,
        message_timestamp,
        receivedDate,
        unit_type,
        grouping_id,
        metric_name,
        metric_value,
        grain,
        partition_date_utc
        FROM ${env:TMP_db}.cs_techmobile_set_agg_stage_instances_${env:execid}

          UNION ALL

            SELECT
            raw_order_number,
            visit_applicationdetails_appversion,
            visit_location_region,
            visit_location_regionname,
            visit_technician_techid,
            visit_technician_quadid,
            visit_device_devicetype,
            visit_device_model,
            jobName,
            message_timestamp,
            receivedDate,
            unit_type,
            grouping_id,
            metric_name,
            metric_value,
            grain,
            partition_date_utc
            FROM ${env:TMP_db}.cs_techmobile_set_agg_stage_visits_${env:execid}
;


DROP TABLE IF EXISTS ${env:TMP_db}.cs_techmobile_set_agg_stage_accounts_${env:execid} PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.cs_techmobile_set_agg_stage_devices_${env:execid} PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.cs_techmobile_set_agg_stage_instances_${env:execid} PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.cs_techmobile_set_agg_stage_visits_${env:execid} PURGE;
