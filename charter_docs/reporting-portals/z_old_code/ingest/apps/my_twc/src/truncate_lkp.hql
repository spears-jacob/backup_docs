USE ${env:LKP_db};

SELECt "\n\nNow Truncating Look Up Tables to ensure they are clean.\n\n";

TRUNCATE TABLE ${env:dbprefix_event};
TRUNCATE TABLE ${env:dbprefix_browser};
TRUNCATE TABLE ${env:dbprefix_country};
TRUNCATE TABLE ${env:dbprefix_connection_type};
TRUNCATE TABLE ${env:dbprefix_javascript};
TRUNCATE TABLE ${env:dbprefix_language};
TRUNCATE TABLE ${env:dbprefix_os};
TRUNCATE TABLE ${env:dbprefix_resolution};
TRUNCATE TABLE ${env:dbprefix_search_engine};
