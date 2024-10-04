      FROM cs_techmobile_metric_agg
      WHERE (partition_date_utc >= ("${hiveconf:START_DATE}") AND partition_date_utc < ("${hiveconf:END_DATE}"))
      GROUP BY
        message_feature_transactionid,
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
        {unit_identifier}
      ) sumfirst
    GROUP BY
    unit_identifier,
    message_feature_transactionid,
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
    unit_type
  GROUPING SETS (
    (unit_identifier),
    (unit_identifier, visit_applicationdetails_appversion),
    (unit_identifier, visit_applicationdetails_appversion, visit_technician_techid),
    (unit_identifier, visit_applicationdetails_appversion, visit_technician_techid, jobName),
    (unit_identifier, visit_applicationdetails_appversion, jobName))
  ) sets
  GROUP BY
    '${hiveconf:partition_date_utc}',
    aw_order_number,
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
    grouping_id,
    unit_type
  ) mapit
LATERAL VIEW EXPLODE(tmp_map) explode_table AS metric_name, metric_value;
