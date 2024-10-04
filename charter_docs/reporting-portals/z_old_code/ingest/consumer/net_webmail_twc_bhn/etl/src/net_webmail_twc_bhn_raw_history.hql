USE ${env:ENVIRONMENT};

INSERT INTO TABLE net_webmail_twc_bhn_raw_history PARTITION(partition_year_month)
SELECT 
  bhn_mso,
  twcc_mso,
  webmail_segment_twcc_global_rsid,
  aem_webmail_login_page,
  page_views,
  visits,
  unique_visitors,
  year_month as partition_year_month
FROM ${env:TMP_db}.net_webmail_twc_bhn_raw; 