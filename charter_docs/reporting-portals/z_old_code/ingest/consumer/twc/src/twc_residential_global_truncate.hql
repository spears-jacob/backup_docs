USE ${env:TMP_db};

TRUNCATE TABLE twc_residential_global_raw;
TRUNCATE TABLE twc_residential_global_denorm;
TRUNCATE TABLE twc_residential_global_denorm_all_files;
TRUNCATE TABLE twc_residential_global_calc;
TRUNCATE TABLE twc_residential_global_calc_combined_events;
TRUNCATE TABLE twc_residential_global_events_no_part;