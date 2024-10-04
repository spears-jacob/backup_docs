use ${env:ENVIRONMENT};

TRUNCATE TABLE ${env:LKP_db}.net_event;
TRUNCATE TABLE ${env:LKP_db}.net_browser;
TRUNCATE TABLE ${env:LKP_db}.net_country;
TRUNCATE TABLE ${env:LKP_db}.net_connection_type;
TRUNCATE TABLE ${env:LKP_db}.net_javascript;
TRUNCATE TABLE ${env:LKP_db}.net_language;
TRUNCATE TABLE ${env:LKP_db}.net_os;
TRUNCATE TABLE ${env:LKP_db}.net_resolution;
TRUNCATE TABLE ${env:LKP_db}.net_search_engine;
