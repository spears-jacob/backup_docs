USE ${env:TMP_db};

TRUNCATE TABLE bhn_residential_raw;
TRUNCATE TABLE bhn_residential_denorm;
TRUNCATE TABLE bhn_residential_calc;
TRUNCATE TABLE bhn_residential_events_no_part;