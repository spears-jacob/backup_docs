USE ${env:ENVIRONMENT};

TRUNCATE TABLE ${env:TMP_db}.sbnet_exec_monthly_subscriber_counts_manual;
TRUNCATE TABLE ${env:TMP_db}.sbnet_exec_monthly_bhn_sso_metrics_manual;
TRUNCATE TABLE ${env:TMP_db}.sbnet_exec_monthly_bhn_accounts_manual;
