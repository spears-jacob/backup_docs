-- QUERY DEFINITIONS FOR [INSERT METRIC NAME HERE]
-- LAST UPDATED [INSERT DATE HERE] BY [INSERT NAME HERE]

USE {env:ENVIRONMENT};

-- NOTE - All queries set to calculate for most recent day (Denver Time)

-- ***** RESIDENTIAL PORTAL QUERIES *****
-- L-CHTR Query
SELECT
  'RES' as portal_platform,
  'L-CHTR' as legacy_footprint,
  [insert_metric_calculation] as [insert_metric_name]
FROM [insert_table_name]
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;

-- L-TWC Query


-- L-BHN Query



-- ***** SMB PORTAL QUERIES *****
-- L-CHTR Query
SELECT
  'SMB' as portal_platform,
  'L-CHTR' as legacy_footprint,
  [insert_metric_calculation] as [insert_metric_name]
FROM [insert_table_name]
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;

-- L-TWC Query


-- L-BHN Query



-- ***** APP QUERIES *****
-- L-CHTR Query
SELECT
  'APP' as portal_platform,
  'L-CHTR' as legacy_footprint,
  [insert_metric_calculation] as [insert_metric_name]
FROM [insert_table_name]
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;

-- L-TWC Query


-- L-BHN Query
