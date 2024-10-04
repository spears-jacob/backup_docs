USE ${env:ENVIRONMENT};

SELECT '
--------------------------------------------------------------------------------
----------------- *** Daily Operational Report View *** ------------------------
--------------------------------------------------------------------------------
';

DROP VIEW IF EXISTS rosetta_v_asp_daily_ops;
CREATE VIEW IF NOT EXISTS rosetta_v_asp_daily_ops
AS
SELECT
sa.mso,
sa.application_type,
sa.device_type,
sa.connection_type,
sa.network_status,
sa.playback_type,
sa.cust_type,
sa.application_group_type,
sa.app_version,
sa.grouping_id,
sa.metric_name,
sa.metric_value,
sa.logged_in_status,
sa.application_name,
sa.os_name,
sa.os_version,
sa.browser_name,
sa.browser_version,
sa.form_factor,
sa.process_date_time_denver,
sa.process_identity,
sa.unit_type,
sa.label_date_denver,
sa.grain,
dl.display_name,
dl.report,
dl.lookup_1 AS form_factor2,
dl.lookup_2,
dl.lookup_3,
dl.lookup_4,
dl.lookup_5
from (
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
  FROM prod.venona_set_agg_portals
  where
  label_date_denver>=date_sub(current_date, 8) and label_date_Denver<=date_sub(current_date, 1)
  AND form_factor = 'All Form Factors'
  AND grouping_id = '2049'
  AND grain = 'daily'
) sa
LEFT JOIN asp_display_layer dl
  ON dl.hive_metric = sa.metric_name
    and sa.application_name = dl.application
    AND sa.unit_type = dl.unit
    AND dl.report = 'asp_daily_ops'
;

SELECT '
--------------------------------------------------------------------------------
--------------- *** END Daily Operational Report View *** ----------------------
--------------------------------------------------------------------------------
';

SELECT '
--------------------------------------------------------------------------------
----------------- *** CCPA Microsite Report View *** ------------------------
--------------------------------------------------------------------------------
';

DROP VIEW IF EXISTS rosetta_v_ccpa_microsite;
CREATE VIEW IF NOT EXISTS rosetta_v_ccpa_microsite
AS
SELECT
sa.mso,
sa.application_type,
sa.device_type,
sa.connection_type,
sa.network_status,
sa.playback_type,
sa.cust_type,
sa.application_group_type,
sa.app_version,
sa.grouping_id,
sa.metric_name,
sa.metric_value,
sa.logged_in_status,
sa.application_name,
sa.os_name,
sa.os_version,
sa.browser_name,
sa.browser_version,
sa.form_factor,
sa.process_date_time_denver,
sa.process_identity,
sa.unit_type,
sa.label_date_denver,
sa.grain,
dl.display_name,
dl.report,
dl.lookup_1,
dl.lookup_2,
dl.lookup_3,
dl.lookup_4,
dl.lookup_5
from(
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
  FROM prod.venona_set_agg_portals
  WHERE
  label_date_denver>=date_sub(current_date, 90) and label_date_Denver<=date_sub(current_date, 1)
  AND form_factor = 'All Form Factors'
--  AND grouping_id = '2049' --** Unknown which groupings you'll be using
-- add -- AND aplication

  AND grain IN ('daily','weekly','monthly','fiscal_monthly') --* --** Unknown which grain(s) you'll be using
) sa
LEFT JOIN asp_display_layer dl
  ON dl.hive_metric = sa.metric_name
    and sa.application_name = dl.application
    AND sa.unit_type = dl.unit
    AND dl.report = 'ccpa_microsite'
;


SELECT '
--------------------------------------------------------------------------------
--------------- *** END CCPA Microsite Report View *** ----------------------
--------------------------------------------------------------------------------
';
