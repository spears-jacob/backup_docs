    ) AS tmp_map
  FROM
    (
    SELECT
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
      unit_type,
      CAST(grouping__id AS INT) AS grouping_id,
