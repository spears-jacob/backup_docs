use ${env:ENVIRONMENT};

TRUNCATE TABLE ${env:TMP_db}.net_raw;
TRUNCATE TABLE ${env:TMP_db}.net_denorm;
TRUNCATE TABLE ${env:TMP_db}.net_calc;
TRUNCATE TABLE ${env:TMP_db}.net_events_no_part;
