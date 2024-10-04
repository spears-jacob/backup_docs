-- Truncates tables that hold manually loaded data

USE ${env:ENVIRONMENT};

TRUNCATE TABLE ${env:TMP_db}.net_monthly_subscriber_counts_manual;
TRUNCATE TABLE ${env:TMP_db}.net_monthly_bhn_sso_metrics_manual;
TRUNCATE TABLE ${env:TMP_db}.net_monthly_bhn_accounts_manual;
TRUNCATE TABLE ${env:TMP_db}.net_products_monthly_tableau_metric_lkp;
