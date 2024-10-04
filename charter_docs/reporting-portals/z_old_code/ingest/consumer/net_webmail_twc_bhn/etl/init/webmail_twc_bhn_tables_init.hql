USE ${env:ENVIRONMENT};

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.net_webmail_twc_bhn_raw_strings
(
  month_name_year String,
  bhn_mso String,
  twcc_mso String,
  webmail_segment_twcc_global_rsid String,
  aem_webmail_login_page String,
  page_views Int,
  visits Int,
  unique_visitors Int
)
row format delimited fields terminated by ','
lines terminated by '\n'
stored as textfile TBLPROPERTIES('serialization.null.format'='',"skip.header.line.count"="1"); --skips first line of .csv file containing headers

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.net_webmail_twc_bhn_raw
(
  year_month String,
  bhn_mso Int,
  twcc_mso Int,
  webmail_segment_twcc_global_rsid Int,
  aem_webmail_login_page Int,
  page_views Int,
  visits Int,
  unique_visitors Int
)
row format delimited fields terminated by '\t'
lines terminated by '\n'
stored as textfile TBLPROPERTIES('serialization.null.format'='');

CREATE TABLE IF NOT EXISTS net_webmail_twc_bhn_metrics_monthly
(
  twc_webmail_page_views Int,
  twc_webmail_login_page_views Int,
  bhn_webmail_page_views Int,
  bhn_webmail_login_page_views Int
)
PARTITIONED BY (partition_year_month STRING)
row format delimited fields terminated by '\t'
lines terminated by '\n'
stored as orc TBLPROPERTIES('serialization.null.format'='');

CREATE TABLE IF NOT EXISTS net_webmail_twc_bhn_raw_history
(
  bhn_mso Int,
  twcc_mso Int,
  webmail_segment_twcc_global_rsid Int,
  aem_webmail_login_page Int,
  page_views Int,
  visits Int,
  unique_visitors Int
)
PARTITIONED BY (partition_year_month STRING)
row format delimited fields terminated by '\t'
lines terminated by '\n'
stored as orc TBLPROPERTIES('serialization.null.format'='');