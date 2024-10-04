USE ${env:TMP_db};

SELECt "\n\nNow Truncating Look Temporary Tables to ensure they are clean.\n\n";

TRUNCATE TABLE ${env:dbprefix_raw};
TRUNCATE TABLE ${env:dbprefix_denorm};
TRUNCATE TABLE ${env:dbprefix_calc};
TRUNCATE TABLE ${env:dbprefix_events_no_part};
