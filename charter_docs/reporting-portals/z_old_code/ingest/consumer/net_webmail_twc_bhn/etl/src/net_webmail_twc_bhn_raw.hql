USE ${env:ENVIRONMENT};

INSERT INTO TABLE ${env:TMP_db}.net_webmail_twc_bhn_raw
SELECT 
  CASE 
    WHEN month_name_year LIKE '%January%' THEN REGEXP_REPLACE(CONCAT(SPLIT(month_name_year,' ')[1],'-01'),'"','')
    WHEN month_name_year LIKE '%February%' THEN REGEXP_REPLACE(CONCAT(SPLIT(month_name_year,' ')[1],'-02'),'"','')
    WHEN month_name_year LIKE '%March%' THEN REGEXP_REPLACE(CONCAT(SPLIT(month_name_year,' ')[1],'-03'),'"','')
    WHEN month_name_year LIKE '%April%' THEN REGEXP_REPLACE(CONCAT(SPLIT(month_name_year,' ')[1],'-04'),'"','')
    WHEN month_name_year LIKE '%May%' THEN REGEXP_REPLACE(CONCAT(SPLIT(month_name_year,' ')[1],'-05'),'"','')
    WHEN month_name_year LIKE '%June%' THEN REGEXP_REPLACE(CONCAT(SPLIT(month_name_year,' ')[1],'-06'),'"','')
    WHEN month_name_year LIKE '%July%' THEN REGEXP_REPLACE(CONCAT(SPLIT(month_name_year,' ')[1],'-07'),'"','')
    WHEN month_name_year LIKE '%August%' THEN REGEXP_REPLACE(CONCAT(SPLIT(month_name_year,' ')[1],'-08'),'"','')
    WHEN month_name_year LIKE '%September%' THEN REGEXP_REPLACE(CONCAT(SPLIT(month_name_year,' ')[1],'-09'),'"','')
    WHEN month_name_year LIKE '%October%' THEN REGEXP_REPLACE(CONCAT(SPLIT(month_name_year,' ')[1],'-10'),'"','')
    WHEN month_name_year LIKE '%November%' THEN REGEXP_REPLACE(CONCAT(SPLIT(month_name_year,' ')[1],'-11'),'"','')
    WHEN month_name_year LIKE '%December%' THEN REGEXP_REPLACE(CONCAT(SPLIT(month_name_year,' ')[1],'-12'),'"','')
    ELSE month_name_year
  END AS year_month,
  CAST(REGEXP_REPLACE(bhn_mso,'"','') AS INT) as bhn_mso,
  CAST(REGEXP_REPLACE(twcc_mso,'"','') AS INT) as twcc_mso,
  CAST(REGEXP_REPLACE(webmail_segment_twcc_global_rsid,'"','') AS INT) as webmail_segment_twcc_global_rsid,
  CAST(REGEXP_REPLACE(aem_webmail_login_page,'"','') AS INT) as aem_webmail_login_page,
  page_views as page_views,
  visits as visits,
  unique_visitors as unique_visitors
FROM ${env:TMP_db}.net_webmail_twc_bhn_raw_strings;