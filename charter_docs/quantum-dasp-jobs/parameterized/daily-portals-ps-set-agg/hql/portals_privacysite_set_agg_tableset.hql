USE ${env:TMP_db};

CREATE TABLE IF NOT EXISTS portals_privacysite_set_agg_stage_accounts_${hiveconf:CLUSTER}
(
  app_section        STRING,
  user_role          STRING,
  message_context    STRING,
  unit_type          STRING,
  metric_name        STRING,
  metric_value       DOUBLE,
  partition_date_utc STRING,
  grain              STRING)
;

CREATE TABLE IF NOT EXISTS portals_privacysite_set_agg_stage_devices_${hiveconf:CLUSTER}
                                LIKe portals_privacysite_set_agg_stage_accounts_${hiveconf:CLUSTER}
;


CREATE TABLE IF NOT EXISTS portals_privacysite_set_agg_stage_instances_${hiveconf:CLUSTER}
                                LIKe portals_privacysite_set_agg_stage_accounts_${hiveconf:CLUSTER}
;

CREATE TABLE IF NOT EXISTS portals_privacysite_set_agg_stage_visits_${hiveconf:CLUSTER}
                                LIKe portals_privacysite_set_agg_stage_accounts_${hiveconf:CLUSTER}
;
