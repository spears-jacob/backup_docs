USE ${env:ENVIRONMENT};

DROP view if exists  asp_v_adobe_smb_ss_counts_hourly;

create view if not exists  asp_v_adobe_smb_ss_counts_hourly as
select * from asp_metrics_detail_hourly
where (date_denver >= (DATE_SUB(CURRENT_DATE, 180))
  and domain = 'smb'
  AND metric IN ('ss_referring_domain', 'ss_referring_site_section', 'ss_support_articles_auth','ss_support_articles_unauth')
  AND source_table = 'asp_v_sbnet_events');
