USE ${env:ENVIRONMENT};

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.fhr_monthly_bpss_metrics_agg
(
  support_overview_page_visitors INT,
  support_videos_viewed INT,
  equipment_refresh_tv INT,
  equipment_refresh_internet INT,
  equipment_refresh_voice INT,
  equipment_refresh_total INT,
  service_appointments_cancelled INT,
  service_appointments_rescheduled INT,
  contact_info_updates INT,
  pref_comm_set INT,
  legacy_company STRING,
  platform STRING,
  year_month STRING
) 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t' 
LINES TERMINATED BY '\n'
STORED AS ORC TBLPROPERTIES('serialization.null.format'='');


CREATE TABLE IF NOT EXISTS ${env:TMP_db}.fhr_chtr_bill_pay_iva_metrics_manual
(
  legacy_company STRING,
  platform STRING,
  year_month STRING,
  auto_pay_processed_cc_dc INT,
  auto_pay_processed_eft INT,
  online_one_time_payments INT,
  payments_other INT,
  total_questions_asked_conversations INT,
  conversations_iva INT,
  conversations_live_chat INT,
  conversations_live_chat_deflected Decimal(15,4)
  ) 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t' 
LINES TERMINATED BY '\n'
STORED AS TEXTFILE TBLPROPERTIES('serialization.null.format'='',"skip.header.line.count"="1");

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.fhr_chtr_brightcove_support_videos_manual
(
  legacy_company STRING,
  platform STRING,
  year_month STRING,
  support_videos_viewed INT
  ) 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t' 
LINES TERMINATED BY '\n'
STORED AS TEXTFILE TBLPROPERTIES('serialization.null.format'='',"skip.header.line.count"="1");


CREATE TABLE IF NOT EXISTS ${env:TMP_db}.fhr_twc_iva_metrics_manual
(
  legacy_company STRING,
  platform STRING,
  year_month STRING,
  total_questions_asked_conversations INT,
  conversations_iva INT,
  conversations_live_chat INT,
  conversations_live_chat_deflected Decimal(15,4)
  ) 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t' 
LINES TERMINATED BY '\n'
STORED AS TEXTFILE TBLPROPERTIES('serialization.null.format'='',"skip.header.line.count"="1");


CREATE TABLE IF NOT EXISTS ${env:TMP_db}.fhr_bhn_bill_pay_type_manual
(
  legacy_company STRING,
  platform STRING,
  year_month STRING,
  auto_pay_processed_cc_dc INT,
  auto_pay_processed_eft INT,
  online_one_time_payments INT,
  payments_other INT
  ) 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t' 
LINES TERMINATED BY '\n'
STORED AS TEXTFILE TBLPROPERTIES('serialization.null.format'='',"skip.header.line.count"="1");


CREATE TABLE IF NOT EXISTS ${env:TMP_db}.fhr_monthly_bpss_bill_pay
(
  visitors_viewing_bill INT,
  one_time_payments_confirmed INT,
  one_time_payments_attempted INT,
  auto_pay_setup_confirmed INT,
  auto_pay_setup_attempted INT,
  auto_pay_processed_cc_dc INT,
  auto_pay_processed_eft INT,
  online_one_time_payments INT,
  payments_other INT,
  legacy_company STRING,
  platform STRING,
  year_month STRING
  ) 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t' 
LINES TERMINATED BY '\n'
STORED AS ORC TBLPROPERTIES('serialization.null.format'='');


CREATE TABLE IF NOT EXISTS ${env:TMP_db}.fhr_monthly_bpss_iva
(
  total_questions_asked_conversations INT,
  conversations_iva INT,
  user_requests_percent_visits Decimal(15,4),
  conversations_live_chat INT,
  conversations_live_chat_deflected Decimal(15,4),
  legacy_company STRING,
  platform STRING,
  year_month STRING
) 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t' 
LINES TERMINATED BY '\n'
STORED AS ORC TBLPROPERTIES('serialization.null.format'='');

CREATE TABLE IF NOT EXISTS fhr_monthly_bpss_metrics_agg
(
  visitors_viewing_bill INT,
  one_time_payments_confirmed INT,
  one_time_payments_percent_confirmed Decimal(15,4),
  auto_pay_setup_confirmed INT,
  auto_pay_setup_percent_confirmed Decimal(15,4),
  auto_pay_processed_cc_dc INT,
  auto_pay_processed_eft INT,
  online_one_time_payments INT,
  payments_other INT,
  support_overview_page_visitors INT,
  support_videos_viewed INT,
  equipment_refresh_tv INT,
  equipment_refresh_internet INT,
  equipment_refresh_voice INT,
  equipment_refresh_total INT,
  service_appointments_cancelled INT,
  service_appointments_rescheduled INT,
  total_questions_asked_conversations INT,
  conversations_iva INT,
  user_requests_percent_visits Decimal(15,4),
  conversations_live_chat INT,
  conversations_live_chat_deflected Decimal(15,4),
  contact_info_updates INT,
  pref_comm_set INT,
  legacy_company STRING,
  platform STRING
) 
PARTITIONED BY (partition_year_month STRING)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t' 
LINES TERMINATED BY '\n'
STORED AS ORC TBLPROPERTIES('serialization.null.format'='');

CREATE TABLE IF NOT EXISTS fhr_chtr_bill_pay_iva_metrics_manual_history
(
  legacy_company STRING,
  platform STRING,
  auto_pay_processed_cc_dc INT,
  auto_pay_processed_eft INT,
  online_one_time_payments INT,
  payments_other INT,
  total_questions_asked_conversations INT,
  conversations_iva INT,
  conversations_live_chat INT,
  conversations_live_chat_deflected Decimal(15,4)
) 
PARTITIONED BY (partition_year_month STRING)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t' 
LINES TERMINATED BY '\n'
STORED AS ORC TBLPROPERTIES('serialization.null.format'='');

CREATE TABLE IF NOT EXISTS fhr_chtr_brightcove_support_videos_manual_history 
(
  legacy_company STRING,
  platform STRING,
  support_videos_viewed INT
) 
PARTITIONED BY (partition_year_month STRING)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t' 
LINES TERMINATED BY '\n'
STORED AS ORC TBLPROPERTIES('serialization.null.format'='');

CREATE TABLE IF NOT EXISTS fhr_twc_iva_metrics_manual_history 
(
  legacy_company STRING,
  platform STRING,
  total_questions_asked_conversations INT,
  conversations_iva INT,
  conversations_live_chat INT,
  conversations_live_chat_deflected Decimal(15,4)
) 
PARTITIONED BY (partition_year_month STRING)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t' 
LINES TERMINATED BY '\n'
STORED AS ORC TBLPROPERTIES('serialization.null.format'='');

CREATE TABLE IF NOT EXISTS fhr_bhn_bill_pay_type_manual 
(
  legacy_company STRING,
  platform STRING,
  auto_pay_processed_cc_dc INT,
  auto_pay_processed_eft INT,
  online_one_time_payments INT,
  payments_other INT
) 
PARTITIONED BY (partition_year_month STRING)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t' 
LINES TERMINATED BY '\n'
STORED AS ORC TBLPROPERTIES('serialization.null.format'='');
