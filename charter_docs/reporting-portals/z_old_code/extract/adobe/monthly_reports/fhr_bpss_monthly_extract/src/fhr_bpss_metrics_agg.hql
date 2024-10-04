USE ${env:ENVIRONMENT};

INSERT OVERWRITE TABLE fhr_monthly_bpss_metrics_agg PARTITION (partition_year_month)
SELECT
  bp.visitors_viewing_bill AS visitors_viewing_bill,
  bp.one_time_payments_confirmed AS one_time_payments_confirmed,
  bp.one_time_payments_confirmed / bp.one_time_payments_attempted AS one_time_payments_percent_confirmed,
  bp.auto_pay_setup_confirmed AS auto_pay_setup_confirmed,
  bp.auto_pay_setup_confirmed / bp.auto_pay_setup_attempted AS auto_pay_setup_percent_confirmed,
  bp.auto_pay_processed_cc_dc AS auto_pay_processed_cc_dc,
  bp.auto_pay_processed_eft AS auto_pay_processed_eft,
  bp.online_one_time_payments AS online_one_time_payments,
  bp.payments_other AS payments_other,
  met.support_overview_page_visitors AS support_overview_page_visitors,
  met.support_videos_viewed AS support_videos_viewed,
  met.equipment_refresh_tv AS equipment_refresh_tv,
  met.equipment_refresh_internet AS equipment_refresh_internet,
  met.equipment_refresh_voice AS equipment_refresh_voice,
  met.equipment_refresh_total AS equipment_refresh_total,
  met.service_appointments_cancelled AS service_appointments_cancelled,
  met.service_appointments_rescheduled AS service_appointments_rescheduled,
  iva.total_questions_asked_conversations AS total_questions_asked_conversations,
  iva.conversations_iva AS conversations_iva,
  iva.user_requests_percent_visits AS user_requests_percent_visits,
  iva.conversations_live_chat AS conversations_live_chat,
  iva.conversations_live_chat_deflected AS conversations_live_chat_deflected,
  met.contact_info_updates AS contact_info_updates,
  met.pref_comm_set AS pref_comm_set,
  met.legacy_company AS legacy_company,
  met.platform AS platform ,
  met.year_month AS partition_year_month
FROM ${env:TMP_db}.fhr_monthly_bpss_metrics_agg met
LEFT JOIN ${env:TMP_db}.fhr_monthly_bpss_bill_pay bp
ON met.legacy_company = bp.legacy_company AND met.platform = bp.platform AND met.year_month = bp.year_month
LEFT JOIN ${env:TMP_db}.fhr_monthly_bpss_iva iva
ON met.legacy_company = iva.legacy_company AND met.platform = iva.platform AND met.year_month = bp.year_month
;