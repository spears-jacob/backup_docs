USE ${env:ENVIRONMENT};

SELECT '\n\n*****-- Now Inserting Manual File entries into asp_${env:CADENCE}_agg_raw  --*****\n\n';

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)

SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (SELECT  subscriber_counts as value,
              'subscribers' as unit,
              'sb' as domain,
              company,
              year_month as ${env:pf},
              'total_hhs' as metric
      FROM ${env:TMP_db}.sbnet_exec_monthly_subscriber_counts_manual
      UNION
      SELECT CAST(REGEXP_REPLACE(REGEXP_REPLACE(new_accounts,'"',''),',','') as bigint)
             AS value,
                'accounts' as unit,
                'sb' as domain,
                company,
                year_month as ${env:pf},
                'total_new_accounts_created' as metric
      FROM ${env:TMP_db}.sbnet_exec_monthly_bhn_accounts_manual
      WHERE INT(site_id)=5 and role='Administrator'
      UNION
      SELECT  value,
             'page views' as unit,
             'sb' as domain,
             company,
             year_month as ${env:pf},
             'support_page_views' as metric
      FROM ${env:TMP_db}.sbnet_exec_monthly_bhn_sso_metrics_manual
      WHERE metric='support_page_views'
----       UNION
----       SELECT  value,
----               'households' as unit,
----               'sb' as domain,
----               company,
----               year_month as ${env:pf},
----               'hhs_logged_in' as metric
----       FROM ${env:TMP_db}.sbnet_exec_monthly_bhn_sso_metrics_manual
----       WHERE metric='unique_hhs_logged_in'
----       UNION
----       SELECT  value,
----               'attempts' as unit,
----               'sb' as domain,
----               company,
----               year_month as ${env:pf},
----               'total_login_attempts' as metric
----       FROM ${env:TMP_db}.sbnet_exec_monthly_bhn_sso_metrics_manual
----       WHERE metric='login_attempts'
----       UNION
----       SELECT  value,
----               'successes' as unit,
----               'sb' as domain,
----               company,
----               year_month as ${env:pf},
----               'total_login_successes' as metric
----       FROM ${env:TMP_db}.sbnet_exec_monthly_bhn_sso_metrics_manual
----       WHERE metric='login_successes'
    ) q
WHERE "${env:CADENCE}" <> 'daily'
;


SELECT '\n\n*****-- Finished Inserting Manual File entries into asp_${env:CADENCE}_agg_raw  --*****\n\n';
