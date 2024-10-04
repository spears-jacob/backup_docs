

INSERT OVERWRITE TABLE ${env:TMP_db}.cs_care_events PARTITION (partition_date_utc)
SELECT * FROM dev_tmp.cs_care_events;


INSERT OVERWRITE TABLE ${env:ENVIRONMENT}.cs_calls_with_prior_visit
SELECT * FROM dev.cs_calls_with_prior_visit;
