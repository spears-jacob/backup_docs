USE ${env:ENVIRONMENT};

INSERT INTO TABLE net_webmail_twc_bhn_metrics_monthly PARTITION(partition_year_month)
SELECT 
  sum(if(twcc_mso=1 AND webmail_segment_twcc_global_rsid=1,page_views,0)) as twc_webmail_page_views,
  sum(if(twcc_mso=1 AND aem_webmail_login_page=1,page_views,0)) as twc_webmail_login_page_views,
  sum(if(bhn_mso=1 AND webmail_segment_twcc_global_rsid=1,page_views,0)) as bhn_webmail_page_views,
  sum(if(bhn_mso=1 AND aem_webmail_login_page=1,page_views,0)) as bhn_webmail_login_page_views,
  year_month AS partition_year_month
FROM ${env:TMP_db}.net_webmail_twc_bhn_raw
GROUP BY
  year_month; 