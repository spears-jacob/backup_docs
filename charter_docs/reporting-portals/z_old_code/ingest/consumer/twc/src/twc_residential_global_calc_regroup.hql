USE ${env:TMP_db};

INSERT INTO TABLE twc_residential_global_calc_combined_events
SELECT
  unique_id,
  MAX(visit_id),
  MAX(previous_visit_id),
  MAX(page_sub_section),
  MAX(sequence_number),
  MAX(visit_failed_attempts),
  MAX(visit_login_duration),
  MAX(visit_start_timestamp),
  MAX(message_name),
  COLLECT_LIST(message_feature_name)
FROM twc_residential_global_calc
GROUP BY unique_id
;
