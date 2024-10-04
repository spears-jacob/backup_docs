USE ${env:ENVIRONMENT};

set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager;
set hive.support.concurrency=false;
set hive.tez.container.size=16000;

set hive.auto.convert.join=false;
set hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;

SELECT "\n\nFor 3: unique_households\n\n";

DROP TABLE IF EXISTS ${env:TMP_db}.asp_monthly_unique_households PURGE;

CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.asp_monthly_unique_households AS

SELECT
        '${env:label_date_denver}' AS label_date_denver,
        application_name,
        SUM(portals_site_unique) as portals_site_unique_hh,
        SUM(portals_site_unique_auth) AS portals_site_unique_auth_hh
  FROM
        (
        SELECT
                application_name,
                portals_unique_acct_key,
                IF(SUM(portals_site_unique) > 0, 1, 0) AS portals_site_unique,
                IF(SUM(portals_site_unique_auth) > 0, 1, 0) AS portals_site_unique_auth
          FROM
                (SELECT
                       case WHEN (application_name = 'myspectrum' or application_name = 'specnet') THEN 'Consumer'
                            WHEN (application_name = 'smb') THEN 'SMB'
                            else 'Other'
                       END as application_name,
                       portals_unique_acct_key,
                       SUM(portals_site_unique) AS portals_site_unique,
                       SUM(portals_site_unique_auth) AS portals_site_unique_auth
                  FROM prod.venona_metric_agg_portals
                 WHERE (denver_date >= ("${env:START_DATE}") AND denver_date < ("${env:END_DATE}"))
                 GROUP BY
                       case WHEN (application_name = 'myspectrum' or application_name = 'specnet') THEN 'Consumer'
                            WHEN (application_name = 'smb') THEN 'SMB'
                            else 'Other'
                       END,
                       portals_unique_acct_key
                ) sumfirst
        GROUP BY
              application_name,
              portals_unique_acct_key
        ) sets
GROUP BY
      '${env:label_date_denver}',
      application_name;

INSERT INTO TABLE asp_product_monthly_metrics partition(label_date_denver)
SELECT application_name,
       'unique_households' as metric_name,
       portals_site_unique_hh as metric_value,
        current_date as run_date,
       label_date_denver
  FROM ${env:TMP_db}.asp_monthly_unique_households;
