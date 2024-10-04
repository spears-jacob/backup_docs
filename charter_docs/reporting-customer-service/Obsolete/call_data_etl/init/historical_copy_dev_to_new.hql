

INSERT OVERWRITE TABLE ${env:TMP_db}.steve_care_events PARTITION (partition_date_utc)
SELECT * FROM dev_tmp.steve_care_events;


INSERT OVERWRITE TABLE ${env:ENVIRONMENT}.steve_calls_with_prior_visit
SELECT * FROM dev.steve_calls_with_prior_visit;
