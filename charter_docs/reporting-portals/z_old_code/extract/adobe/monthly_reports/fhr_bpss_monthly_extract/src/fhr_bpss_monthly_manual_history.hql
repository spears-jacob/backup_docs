USE ${env:ENVIRONMENT};

INSERT OVERWRITE TABLE fhr_chtr_bill_pay_iva_metrics_manual_history PARTITION (partition_year_month)
SELECT
  legacy_company AS legacy_company,
  platform AS platform,
  auto_pay_processed_cc_dc AS auto_pay_processed_cc_dc,
  auto_pay_processed_eft AS auto_pay_processed_eft,
  online_one_time_payments AS online_one_time_payments,
  payments_other AS payments_other,
  total_questions_asked_conversations AS total_questions_asked_conversations,
  conversations_iva AS conversations_iva,
  conversations_live_chat AS conversations_live_chat,
  conversations_live_chat_deflected AS conversations_live_chat_deflected,
  year_month AS partition_year_month
FROM ${env:TMP_db}.fhr_chtr_bill_pay_iva_metrics_manual
;

INSERT OVERWRITE TABLE fhr_chtr_brightcove_support_videos_manual_history PARTITION (partition_year_month)
SELECT
  legacy_company AS legacy_company,
  platform AS platform,
  support_videos_viewed AS support_videos_viewed,
  year_month AS partition_year_month
FROM ${env:TMP_db}.fhr_chtr_brightcove_support_videos_manual
;

INSERT OVERWRITE TABLE fhr_twc_iva_metrics_manual_history PARTITION (partition_year_month)
SELECT
  legacy_company AS legacy_company,
  platform AS platform,
  total_questions_asked_conversations AS total_questions_asked_conversations,
  conversations_iva AS conversations_iva,
  conversations_live_chat AS conversations_live_chat,
  conversations_live_chat_deflected AS conversations_live_chat_deflected,
  year_month AS partition_year_month
FROM ${env:TMP_db}.fhr_twc_iva_metrics_manual
;

INSERT OVERWRITE TABLE fhr_bhn_bill_pay_type_manual PARTITION (partition_year_month)
SELECT
  legacy_company AS legacy_company,
  platform AS platform,
  auto_pay_processed_cc_dc AS auto_pay_processed_cc_dc,
  auto_pay_processed_eft AS auto_pay_processed_eft,
  online_one_time_payments AS online_one_time_payments,
  payments_other AS payments_other,
  year_month AS partition_year_month
FROM ${env:TMP_db}.fhr_bhn_bill_pay_type_manual
;