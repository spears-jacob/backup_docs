USE ${env:TMP_db};

CREATE TABLE IF NOT EXISTS m2dot0_set_agg_portals_stage_accounts_${hiveconf:execid}_${hiveconf:stepid}
(
  visit__application_details__application_name                        STRING,
  visit__application_details__application_type                        STRING,
  visit__application_details__app_version                             STRING,
  agg_custom_visit__account__details__service_subscriptions           STRING,
  agg_custom_customer_group                                           STRING,
  agg_visit__account__configuration_factors                           STRING,
  grouping_id                                                         INT,
  metric_name                                                         STRING,
  metric_value                                                        DOUBLE,
  process_date_time_denver                                            STRING,
  process_identity                                                    STRING,
  unit_type                                                           STRING,
  call_count_24h                                                      INT,
  partition_date_utc                                                  STRING,
  grain                                                               STRING
)

;

CREATE TABLE IF NOT EXISTS m2dot0_set_agg_portals_stage_devices_${hiveconf:execid}_${hiveconf:stepid}
                                LIKe m2dot0_set_agg_portals_stage_accounts_${hiveconf:execid}_${hiveconf:stepid}
;


CREATE TABLE IF NOT EXISTS m2dot0_set_agg_instances_${hiveconf:execid}_${hiveconf:stepid}
                                LIKe m2dot0_set_agg_portals_stage_accounts_${hiveconf:execid}_${hiveconf:stepid}
;

CREATE TABLE IF NOT EXISTS m2dot0_set_agg_portals_stage_visits_${hiveconf:execid}_${hiveconf:stepid}
                                LIKe m2dot0_set_agg_portals_stage_accounts_${hiveconf:execid}_${hiveconf:stepid}
;
